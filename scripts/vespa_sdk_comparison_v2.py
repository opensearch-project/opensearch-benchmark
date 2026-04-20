#!/usr/bin/env python3
"""
Vespa SDK comparison v2 — isolated runs with cooldown between each test.

Run ONE approach at a time with --mode flag to avoid server saturation:
    python3.11 vespa_sdk_comparison_v2.py --mode aiohttp
    python3.11 vespa_sdk_comparison_v2.py --mode pyvespa-sync
    python3.11 vespa_sdk_comparison_v2.py --mode pyvespa-async
    python3.11 vespa_sdk_comparison_v2.py --mode all  (runs all with 30s cooldown)

Query count scales with concurrency to target ~20s of steady-state measurement
per test (Option B). At 1 client a short run is fine; at 32 clients we need
many more queries to keep the runtime long enough for percentiles to stabilize.

NOTE: Vectors are pre-converted to Python lists before benchmarking to avoid
measuring numpy.tolist() overhead (which is GIL-held and would dominate at
high concurrency). This matches OSB's behavior where vectors are pre-loaded
from HDF5 as Python lists.
"""

import argparse
import asyncio
import concurrent.futures
import time
import logging
import numpy as np

logging.getLogger("httpr").setLevel(logging.WARNING)

VESPA_HOST = "http://10.0.139.8:8080"
DIM = 768
K = 100
CONCURRENCY_LEVELS = [1, 8, 16, 32]
COOLDOWN = 30  # seconds between test modes

# Option B: scale query count with concurrency so each test gets ~20s of
# steady-state measurement regardless of client count.
#   1 client  → 5,000 queries  (~20s at 250 QPS)
#   8 clients → 40,000 queries (~20s at 2000 QPS)
#  32 clients → 160,000 queries (~20s at 8000 QPS)
QUERIES_PER_CLIENT = 5000
MIN_QUERIES = 5000


def queries_for(num_clients: int) -> int:
    return max(MIN_QUERIES, num_clients * QUERIES_PER_CLIENT)


def warmup_for(num_queries: int) -> int:
    return max(200, num_queries // 20)  # 5%, minimum 200


# Vector pool. We don't need unique vectors per query — cycling through a
# pool of 5000 unique vectors gives enough entropy that server-side query
# caches don't become the dominant effect, and keeps memory bounded.
# 5000 × 768 × 24 bytes (Python float) = ~92MB for the pool.
POOL_SIZE = 5000
np.random.seed(42)
_pool_np = np.random.randn(POOL_SIZE, DIM).astype(np.float32)
vector_pool = [_pool_np[i].tolist() for i in range(POOL_SIZE)]
del _pool_np  # free the numpy backing store


def make_query_body(qvec_list):
    """Build Vespa query body. qvec_list must already be a Python list."""
    return {
        "yql": f"select documentid from target_index where {{targetHits:{K}}}nearestNeighbor(embedding, query_vector)",
        "ranking": "vector-similarity",
        "hits": K,
        "timeout": "10s",
        "input.query(query_vector)": qvec_list,
    }


def build_bodies(num_queries: int):
    """Build `num_queries` query body dicts by cycling through the vector pool.

    Dicts hold references to pool vectors (not copies), so memory is O(num_queries)
    in dict overhead only — the vectors themselves are shared.
    """
    return [make_query_body(vector_pool[i % POOL_SIZE]) for i in range(num_queries)]


def percentile(latencies, p):
    latencies.sort()
    idx = int(len(latencies) * p / 100)
    return latencies[min(idx, len(latencies) - 1)]


def report(latencies, wall_time, num_queries, num_clients):
    qps = len(latencies) / wall_time
    print(f"    {num_clients:>2} clients ({num_queries:>6} queries, {wall_time:>5.1f}s):"
          f"  QPS: {qps:>7.0f}   p50: {percentile(latencies, 50):>5.1f}ms"
          f"   p90: {percentile(latencies, 90):>5.1f}ms"
          f"   p99: {percentile(latencies, 99):>5.1f}ms")


# =============================================================================
# 1. Raw aiohttp — async, HTTP/1.1, no Vespa SDK
# =============================================================================

async def test_aiohttp():
    print(f"\n  Raw aiohttp (HTTP/1.1, async, connection pooling)")
    print(f"  {'─' * 70}")

    import aiohttp

    for num_clients in CONCURRENCY_LEVELS:
        num_queries = queries_for(num_clients)
        num_warmup = warmup_for(num_queries)
        test_bodies = build_bodies(num_queries)
        warmup_bodies = build_bodies(num_warmup)

        # Connector sized to match the desired concurrency level, not the default (100).
        connector = aiohttp.TCPConnector(limit=num_clients, limit_per_host=num_clients)
        async with aiohttp.ClientSession(connector=connector) as session:
            # Worker-queue: N workers pulling from a shared queue. Bounds memory
            # to N in-flight tasks instead of creating all num_queries upfront.
            async def worker(queue, latencies):
                while True:
                    try:
                        body = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    t0 = time.perf_counter()
                    async with session.post(f"{VESPA_HOST}/search/", json=body) as resp:
                        await resp.json()
                    latencies.append((time.perf_counter() - t0) * 1000)

            # Warmup
            wq: asyncio.Queue = asyncio.Queue()
            for b in warmup_bodies:
                wq.put_nowait(b)
            _warmup_latencies: list = []
            await asyncio.gather(*[worker(wq, _warmup_latencies) for _ in range(num_clients)])

            # Measured
            q: asyncio.Queue = asyncio.Queue()
            for b in test_bodies:
                q.put_nowait(b)
            latencies: list = []
            t0 = time.perf_counter()
            await asyncio.gather(*[worker(q, latencies) for _ in range(num_clients)])
            wall_time = time.perf_counter() - t0

        report(latencies, wall_time, num_queries, num_clients)


# =============================================================================
# 2. pyvespa syncio + ThreadPoolExecutor — current OSB pattern
# =============================================================================

async def test_pyvespa_sync():
    """Uses app.syncio(compress=False) — same as OSB's search path.

    Creates a VespaSync context with httpr.Client (Rust HTTP engine).
    compress=False avoids redundant json.dumps+gzip in Python, letting httpr
    serialize via serde with the GIL released.
    """
    print(f"\n  pyvespa syncio + ThreadPoolExecutor (httpr Rust, compress=False)")
    print(f"  {'─' * 70}")

    from vespa.application import Vespa as PyvespaApp
    vespa_app = PyvespaApp(url=VESPA_HOST)

    for num_clients in CONCURRENCY_LEVELS:
        num_queries = queries_for(num_clients)
        num_warmup = warmup_for(num_queries)
        test_bodies = build_bodies(num_queries)
        warmup_bodies = build_bodies(num_warmup)

        with vespa_app.syncio(compress=False) as vespa:
            def search(body):
                t0 = time.perf_counter()
                vespa.query(body=body)
                return (time.perf_counter() - t0) * 1000

            # Thread pool sized to exactly num_clients — we want the concurrency
            # level to be controlled by the pool, not by queue backpressure.
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_clients) as executor:
                # Warmup
                list(executor.map(search, warmup_bodies))

                # Measured
                t0 = time.perf_counter()
                latencies = list(executor.map(search, test_bodies))
                wall_time = time.perf_counter() - t0

        report(latencies, wall_time, num_queries, num_clients)


# =============================================================================
# 3. pyvespa asyncio — native async via httpr.AsyncClient
# =============================================================================

async def test_pyvespa_async():
    """Uses app.asyncio() — pyvespa's native async path.

    Uses httpr.AsyncClient internally, which runs on a single-threaded tokio
    runtime. The single tokio thread becomes the bottleneck at high concurrency
    since all HTTP I/O is serialized through it — this is the hypothesis we're
    testing.
    """
    print(f"\n  pyvespa async (VespaAsync, httpr.AsyncClient, HTTP/2)")
    print(f"  {'─' * 70}")

    from vespa.application import Vespa as PyvespaApp
    vespa_app = PyvespaApp(url=VESPA_HOST)

    for num_clients in CONCURRENCY_LEVELS:
        num_queries = queries_for(num_clients)
        num_warmup = warmup_for(num_queries)
        test_bodies = build_bodies(num_queries)
        warmup_bodies = build_bodies(num_warmup)

        # `connections=num_clients` tells pyvespa how many HTTP/2 connections
        # to open. HTTP/2 multiplexes many requests per connection, but more
        # connections can help when the server has per-connection bottlenecks
        # or the client's tokio runtime is the bottleneck.
        async with vespa_app.asyncio(connections=num_clients, timeout=30) as vespa_async:
            # Worker-queue: N workers each pull from a shared queue. This is
            # equivalent to using a Semaphore but cleaner — we explicitly have
            # N parallel workers, matching the semantics of "N clients".
            async def worker(queue, latencies):
                while True:
                    try:
                        body = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    t0 = time.perf_counter()
                    await vespa_async.query(body=body)
                    latencies.append((time.perf_counter() - t0) * 1000)

            # Warmup
            wq: asyncio.Queue = asyncio.Queue()
            for b in warmup_bodies:
                wq.put_nowait(b)
            _warmup_latencies: list = []
            await asyncio.gather(*[worker(wq, _warmup_latencies) for _ in range(num_clients)])

            # Measured
            q: asyncio.Queue = asyncio.Queue()
            for b in test_bodies:
                q.put_nowait(b)
            latencies: list = []
            t0 = time.perf_counter()
            await asyncio.gather(*[worker(q, latencies) for _ in range(num_clients)])
            wall_time = time.perf_counter() - t0

        report(latencies, wall_time, num_queries, num_clients)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["aiohttp", "pyvespa-sync", "pyvespa-async", "all"], default="all")
    args = parser.parse_args()

    print(f"Vespa SDK Comparison v2")
    print(f"Host: {VESPA_HOST}")
    print(f"Scaling: {QUERIES_PER_CLIENT} queries per client (min {MIN_QUERIES})")
    print(f"{'=' * 70}")

    if args.mode in ("aiohttp", "all"):
        await test_aiohttp()
    if args.mode == "all":
        print(f"\n  ... cooling down {COOLDOWN}s ...")
        await asyncio.sleep(COOLDOWN)

    if args.mode in ("pyvespa-sync", "all"):
        await test_pyvespa_sync()
    if args.mode == "all":
        print(f"\n  ... cooling down {COOLDOWN}s ...")
        await asyncio.sleep(COOLDOWN)

    if args.mode in ("pyvespa-async", "all"):
        await test_pyvespa_async()

    print(f"\n{'=' * 70}")
    print("Done!")

asyncio.run(main())
