#!/usr/bin/env python3
"""
Vespa SDK comparison — compares search performance across:
1. Raw aiohttp (current OSB implementation)
2. pyvespa sync (Vespa.query())
3. pyvespa async (VespaAsync.query())

Tests sequential (1 client) and concurrent (8, 32 clients) to see
how each approach scales.

Run from the test runner node:
    python3.11 vespa_sdk_comparison.py

Requires: pip install aiohttp pyvespa numpy
"""

import asyncio
import concurrent.futures
import time
import logging
import numpy as np

# Suppress pyvespa's per-request logging
logging.getLogger("httpr").setLevel(logging.WARNING)

VESPA_HOST = "http://10.0.142.54:8080"
DIM = 768
K = 100
NUM_QUERIES = 500
CONCURRENCY_LEVELS = [1, 8, 32]

np.random.seed(42)
query_vectors = [np.random.randn(DIM).astype(np.float32) for _ in range(NUM_QUERIES)]

def make_query_body(qvec):
    return {
        "yql": f"select documentid from target_index where {{targetHits:{K}}}nearestNeighbor(embedding, query_vector)",
        "ranking": "vector-similarity",
        "hits": K,
        "timeout": "10s",
        "input.query(query_vector)": qvec.tolist(),
    }

def percentile(latencies, p):
    latencies.sort()
    idx = int(len(latencies) * p / 100)
    return latencies[min(idx, len(latencies) - 1)]

def report(name, latencies, wall_time):
    n = len(latencies)
    qps = n / wall_time
    print(f"    QPS: {qps:>7.0f}   p50: {percentile(latencies, 50):>5.1f}ms"
          f"   p90: {percentile(latencies, 90):>5.1f}ms"
          f"   p99: {percentile(latencies, 99):>5.1f}ms")


async def main():
    print(f"Vespa SDK Comparison ({NUM_QUERIES} queries, {DIM}-dim, k={K})")
    print(f"{'='*70}")

    # ── 1. Raw aiohttp (current OSB path) ───────────────────────────
    print(f"\n  1. Raw aiohttp (current OSB implementation)")
    print(f"  {'─'*50}")

    import aiohttp

    for num_clients in CONCURRENCY_LEVELS:
        semaphore = asyncio.Semaphore(num_clients)

        async def aiohttp_search(session, qvec):
            async with semaphore:
                body = make_query_body(qvec)
                t0 = time.perf_counter()
                async with session.post(f"{VESPA_HOST}/search/", json=body) as resp:
                    await resp.json()
                return (time.perf_counter() - t0) * 1000

        async with aiohttp.ClientSession() as session:
            # Warmup
            for qvec in query_vectors[:10]:
                await aiohttp_search(session, qvec)

            t0 = time.perf_counter()
            tasks = [aiohttp_search(session, qvec) for qvec in query_vectors]
            latencies = await asyncio.gather(*tasks)
            wall_time = time.perf_counter() - t0

        print(f"    {num_clients:>2} clients:", end="")
        report("aiohttp", list(latencies), wall_time)

    # ── 2. pyvespa sync (Vespa.query()) ─────────────────────────────
    print(f"\n  2. pyvespa sync (Vespa.query())")
    print(f"  {'─'*50}")

    from vespa.application import Vespa as PyvespaApp

    vespa_sync = PyvespaApp(url=VESPA_HOST)

    for num_clients in CONCURRENCY_LEVELS:
        if num_clients == 1:
            # Sequential
            latencies = []
            # Warmup
            for qvec in query_vectors[:10]:
                vespa_sync.query(body=make_query_body(qvec))

            t0 = time.perf_counter()
            for qvec in query_vectors:
                qt0 = time.perf_counter()
                vespa_sync.query(body=make_query_body(qvec))
                latencies.append((time.perf_counter() - qt0) * 1000)
            wall_time = time.perf_counter() - t0

            print(f"     1 clients:", end="")
            report("pyvespa sync", latencies, wall_time)
        else:
            # Threaded (same pattern as Milvus — sync SDK in thread pool)
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=num_clients)

            def sync_search(qvec):
                t0 = time.perf_counter()
                vespa_sync.query(body=make_query_body(qvec))
                return (time.perf_counter() - t0) * 1000

            # Warmup
            list(executor.map(sync_search, query_vectors[:10]))

            t0 = time.perf_counter()
            futures = [executor.submit(sync_search, qvec) for qvec in query_vectors]
            latencies = [f.result() for f in futures]
            wall_time = time.perf_counter() - t0
            executor.shutdown(wait=True)

            print(f"    {num_clients:>2} clients:", end="")
            report("pyvespa sync+threads", latencies, wall_time)

    # ── 3. pyvespa async (VespaAsync.query()) ───────────────────────
    print(f"\n  3. pyvespa async (VespaAsync.query() — HTTP/2)")
    print(f"  {'─'*50}")

    vespa_app = PyvespaApp(url=VESPA_HOST)

    for num_clients in CONCURRENCY_LEVELS:
        semaphore = asyncio.Semaphore(num_clients)

        async with vespa_app.asyncio(connections=num_clients, timeout=30) as vespa_async:
            async def pyvespa_async_search(qvec):
                async with semaphore:
                    t0 = time.perf_counter()
                    await vespa_async.query(body=make_query_body(qvec))
                    return (time.perf_counter() - t0) * 1000

            # Warmup
            for qvec in query_vectors[:10]:
                await pyvespa_async_search(qvec)

            t0 = time.perf_counter()
            tasks = [pyvespa_async_search(qvec) for qvec in query_vectors]
            latencies = await asyncio.gather(*tasks)
            wall_time = time.perf_counter() - t0

        print(f"    {num_clients:>2} clients:", end="")
        report("pyvespa async", list(latencies), wall_time)

    print(f"\n{'='*70}")
    print("Done!")

asyncio.run(main())
