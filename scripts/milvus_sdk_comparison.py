#!/usr/bin/env python3
"""
Milvus SDK comparison — sync pymilvus (ThreadPoolExecutor) vs AsyncMilvusClient
(native grpc.aio) vs REST API paths.

Run ONE approach at a time with --mode flag:
    python3.11 milvus_sdk_comparison.py --mode pymilvus
    python3.11 milvus_sdk_comparison.py --mode pymilvus-async
    python3.11 milvus_sdk_comparison.py --mode rest-aiohttp
    python3.11 milvus_sdk_comparison.py --mode rest-httpr
    python3.11 milvus_sdk_comparison.py --mode all  (runs all with 30s cooldown)

Query count scales with concurrency to target ~20s of steady-state measurement
per test (Option B). Matches vespa_sdk_comparison_v2.py.

Vectors pre-converted to Python lists (no numpy.tolist() in hot path).

Requires: pymilvus>=2.5.0 (for AsyncMilvusClient), aiohttp, httpr
"""

import argparse
import asyncio
import concurrent.futures
import time
import logging
import numpy as np

logging.getLogger("httpr").setLevel(logging.WARNING)

MILVUS_HOST = "10.0.138.142"
MILVUS_GRPC_PORT = 19530
MILVUS_REST_PORT = 19530  # REST API is on the same port in Milvus 2.4+
DIM = 768
K = 100
EF_SEARCH = 100
COLLECTION = "target_index"
CONCURRENCY_LEVELS = [1, 8, 16, 32]
COOLDOWN = 30

# Option B: scale query count with concurrency so each test gets ~20s of
# steady-state measurement regardless of client count.
QUERIES_PER_CLIENT = 5000
MIN_QUERIES = 5000


def queries_for(num_clients: int) -> int:
    return max(MIN_QUERIES, num_clients * QUERIES_PER_CLIENT)


def warmup_for(num_queries: int) -> int:
    return max(200, num_queries // 20)  # 5%, minimum 200


# Vector pool (same pattern as vespa_sdk_comparison_v2.py — cycle through 5000
# unique vectors rather than allocating one per query).
POOL_SIZE = 5000
np.random.seed(42)
_pool_np = np.random.randn(POOL_SIZE, DIM).astype(np.float32)
vector_pool = [_pool_np[i].tolist() for i in range(POOL_SIZE)]
del _pool_np


def get_vector(i: int):
    return vector_pool[i % POOL_SIZE]


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
# 1. pymilvus (sync) + ThreadPoolExecutor — current OSB pattern
# =============================================================================

async def test_pymilvus():
    """Same pattern as OSB: sync pymilvus.MilvusClient + ThreadPoolExecutor."""
    print(f"\n  pymilvus (sync gRPC) + ThreadPoolExecutor")
    print(f"  {'─' * 70}")

    from pymilvus import MilvusClient

    client = MilvusClient(uri=f"http://{MILVUS_HOST}:{MILVUS_GRPC_PORT}")
    client.load_collection(COLLECTION)

    try:
        for num_clients in CONCURRENCY_LEVELS:
            num_queries = queries_for(num_clients)
            num_warmup = warmup_for(num_queries)

            def search(i):
                t0 = time.perf_counter()
                client.search(
                    collection_name=COLLECTION,
                    data=[get_vector(i)],
                    anns_field="embedding",
                    limit=K,
                    output_fields=["doc_id"],
                    search_params={"params": {"ef": EF_SEARCH}},
                )
                return (time.perf_counter() - t0) * 1000

            # Thread pool sized to exactly num_clients — the concurrency level
            # IS the pool size. OSB's pattern is slightly different (pool is
            # max_workers=64) but for this isolated test we want a tight match.
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_clients) as executor:
                # Warmup
                list(executor.map(search, range(num_warmup)))

                # Measured
                t0 = time.perf_counter()
                latencies = list(executor.map(search, range(num_queries)))
                wall_time = time.perf_counter() - t0

            report(latencies, wall_time, num_queries, num_clients)
    finally:
        client.close()


# =============================================================================
# 2. pymilvus AsyncMilvusClient — native grpc.aio
# =============================================================================

async def test_pymilvus_async():
    """pymilvus AsyncMilvusClient — native async gRPC via grpc.aio.

    AsyncMilvusClient was added in pymilvus 2.5.0. It uses grpc.aio under the
    hood, matching the pattern of OSB's OpenSearch gRPC client. If throughput
    is comparable to sync+ThreadPoolExecutor, we can drop the threadpool layer
    in Milvus's OSB runner.
    """
    print(f"\n  pymilvus AsyncMilvusClient (native grpc.aio)")
    print(f"  {'─' * 70}")

    try:
        from pymilvus import AsyncMilvusClient
    except ImportError:
        print("    AsyncMilvusClient not available — need pymilvus >= 2.5.0")
        return

    client = AsyncMilvusClient(uri=f"http://{MILVUS_HOST}:{MILVUS_GRPC_PORT}")
    try:
        await client.load_collection(COLLECTION)
    except Exception as e:
        print(f"    load_collection failed: {e} — skipping")
        try:
            await client.close()
        except Exception:
            pass
        return

    try:
        for num_clients in CONCURRENCY_LEVELS:
            num_queries = queries_for(num_clients)
            num_warmup = warmup_for(num_queries)

            # Worker-queue: N workers each pull from a shared queue. Bounds
            # memory to N in-flight coroutines and maps cleanly onto "N clients".
            async def worker(queue, latencies):
                while True:
                    try:
                        i = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    t0 = time.perf_counter()
                    await client.search(
                        collection_name=COLLECTION,
                        data=[get_vector(i)],
                        anns_field="embedding",
                        limit=K,
                        output_fields=["doc_id"],
                        search_params={"params": {"ef": EF_SEARCH}},
                    )
                    latencies.append((time.perf_counter() - t0) * 1000)

            # Warmup
            wq: asyncio.Queue = asyncio.Queue()
            for i in range(num_warmup):
                wq.put_nowait(i)
            _warmup: list = []
            await asyncio.gather(*[worker(wq, _warmup) for _ in range(num_clients)])

            # Measured
            q: asyncio.Queue = asyncio.Queue()
            for i in range(num_queries):
                q.put_nowait(i)
            latencies: list = []
            t0 = time.perf_counter()
            await asyncio.gather(*[worker(q, latencies) for _ in range(num_clients)])
            wall_time = time.perf_counter() - t0

            report(latencies, wall_time, num_queries, num_clients)
    finally:
        try:
            await client.close()
        except Exception:
            pass


# =============================================================================
# 3. Milvus REST API + aiohttp (async, bypasses protobuf)
# =============================================================================

async def test_rest_aiohttp():
    """Milvus RESTful API via aiohttp — JSON over HTTP, no protobuf."""
    print(f"\n  Milvus REST API + aiohttp (async, JSON, no protobuf)")
    print(f"  {'─' * 70}")

    import aiohttp

    url = f"http://{MILVUS_HOST}:{MILVUS_REST_PORT}/v2/vectordb/entities/search"

    def make_body(i):
        return {
            "collectionName": COLLECTION,
            "data": [get_vector(i)],
            "annsField": "embedding",
            "limit": K,
            "outputFields": ["doc_id"],
            "searchParams": {"params": {"ef": EF_SEARCH}},
        }

    for num_clients in CONCURRENCY_LEVELS:
        num_queries = queries_for(num_clients)
        num_warmup = warmup_for(num_queries)

        connector = aiohttp.TCPConnector(limit=num_clients, limit_per_host=num_clients)
        async with aiohttp.ClientSession(connector=connector) as session:
            async def worker(queue, latencies):
                while True:
                    try:
                        i = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    t0 = time.perf_counter()
                    async with session.post(url, json=make_body(i)) as resp:
                        await resp.json()
                    latencies.append((time.perf_counter() - t0) * 1000)

            # Warmup
            wq: asyncio.Queue = asyncio.Queue()
            for i in range(num_warmup):
                wq.put_nowait(i)
            _warmup: list = []
            await asyncio.gather(*[worker(wq, _warmup) for _ in range(num_clients)])

            # Measured
            q: asyncio.Queue = asyncio.Queue()
            for i in range(num_queries):
                q.put_nowait(i)
            latencies: list = []
            t0 = time.perf_counter()
            await asyncio.gather(*[worker(q, latencies) for _ in range(num_clients)])
            wall_time = time.perf_counter() - t0

        report(latencies, wall_time, num_queries, num_clients)


# =============================================================================
# 4. Milvus REST API + httpr (sync Rust, GIL released, ThreadPoolExecutor)
# =============================================================================

async def test_rest_httpr():
    """Milvus RESTful API via httpr — JSON serialized in Rust, GIL released."""
    print(f"\n  Milvus REST API + httpr (sync Rust, GIL released, ThreadPoolExecutor)")
    print(f"  {'─' * 70}")

    import httpr

    url = f"http://{MILVUS_HOST}:{MILVUS_REST_PORT}/v2/vectordb/entities/search"
    client = httpr.Client(timeout=30)

    try:
        for num_clients in CONCURRENCY_LEVELS:
            num_queries = queries_for(num_clients)
            num_warmup = warmup_for(num_queries)

            def search(i):
                body = {
                    "collectionName": COLLECTION,
                    "data": [get_vector(i)],
                    "annsField": "embedding",
                    "limit": K,
                    "outputFields": ["doc_id"],
                    "searchParams": {"params": {"ef": EF_SEARCH}},
                }
                t0 = time.perf_counter()
                client.post(url, json=body)
                return (time.perf_counter() - t0) * 1000

            with concurrent.futures.ThreadPoolExecutor(max_workers=num_clients) as executor:
                # Warmup
                list(executor.map(search, range(num_warmup)))

                # Measured
                t0 = time.perf_counter()
                latencies = list(executor.map(search, range(num_queries)))
                wall_time = time.perf_counter() - t0

            report(latencies, wall_time, num_queries, num_clients)
    finally:
        try:
            client.close()
        except Exception:
            pass


async def main():
    global MILVUS_HOST  # pylint: disable=global-statement

    parser = argparse.ArgumentParser(description="Milvus SDK comparison benchmark")
    parser.add_argument("--mode",
                        choices=["pymilvus", "pymilvus-async", "rest-aiohttp", "rest-httpr", "all"],
                        default="all")
    parser.add_argument("--host", default=MILVUS_HOST, help="Milvus host IP")
    args = parser.parse_args()

    MILVUS_HOST = args.host

    print(f"Milvus SDK Comparison")
    print(f"Host: {MILVUS_HOST}:{MILVUS_GRPC_PORT} (gRPC) / {MILVUS_REST_PORT} (REST)")
    print(f"Scaling: {QUERIES_PER_CLIENT} queries per client (min {MIN_QUERIES})")
    print(f"{'=' * 70}")

    if args.mode in ("pymilvus", "all"):
        await test_pymilvus()
    if args.mode == "all":
        print(f"\n  ... cooling down {COOLDOWN}s ...")
        await asyncio.sleep(COOLDOWN)

    if args.mode in ("pymilvus-async", "all"):
        await test_pymilvus_async()
    if args.mode == "all":
        print(f"\n  ... cooling down {COOLDOWN}s ...")
        await asyncio.sleep(COOLDOWN)

    if args.mode in ("rest-aiohttp", "all"):
        await test_rest_aiohttp()
    if args.mode == "all":
        print(f"\n  ... cooling down {COOLDOWN}s ...")
        await asyncio.sleep(COOLDOWN)

    if args.mode in ("rest-httpr", "all"):
        await test_rest_httpr()

    print(f"\n{'=' * 70}")
    print("Done!")

asyncio.run(main())
