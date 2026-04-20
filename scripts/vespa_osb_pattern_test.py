#!/usr/bin/env python3
"""
Test pyvespa sync client with asyncio.to_thread() — the same pattern
OSB would use if we switch from aiohttp to pyvespa.

Compares:
1. Current OSB pattern (aiohttp async)
2. Proposed pattern (pyvespa sync + asyncio.to_thread, like Milvus)

Run: python3.11 vespa_osb_pattern_test.py
"""

import asyncio
import concurrent.futures
import functools
import time
import logging
import numpy as np

logging.getLogger("httpr").setLevel(logging.WARNING)

VESPA_HOST = "http://10.0.142.54:8080"
DIM = 768
K = 100
NUM_QUERIES = 2000
WARMUP = 200
CONCURRENCY_LEVELS = [1, 8, 32]

np.random.seed(42)
query_vectors = [np.random.randn(DIM).astype(np.float32) for _ in range(NUM_QUERIES + WARMUP)]
warmup_vecs = query_vectors[:WARMUP]
test_vecs = query_vectors[WARMUP:]

def make_body(qvec):
    return {
        "yql": f"select documentid from target_index where {{targetHits:{K}}}nearestNeighbor(embedding, query_vector)",
        "ranking": "vector-similarity",
        "hits": K,
        "timeout": "10s",
        "input.query(query_vector)": qvec.tolist(),
    }

def percentile(latencies, p):
    latencies.sort()
    return latencies[min(int(len(latencies) * p / 100), len(latencies) - 1)]

def report(latencies, wall_time):
    qps = len(latencies) / wall_time
    print(f"    QPS: {qps:>7.0f}   p50: {percentile(latencies, 50):>5.1f}ms"
          f"   p90: {percentile(latencies, 90):>5.1f}ms"
          f"   p99: {percentile(latencies, 99):>5.1f}ms")


async def main():
    print(f"Vespa OSB Pattern Test ({NUM_QUERIES} queries, {WARMUP} warmup)")
    print(f"{'='*70}")

    # ── 1. Current: aiohttp async ───────────────────────────────────
    print(f"\n  1. Current OSB pattern (aiohttp async)")
    print(f"  {'─'*50}")

    import aiohttp

    for num_clients in CONCURRENCY_LEVELS:
        semaphore = asyncio.Semaphore(num_clients)

        async def aiohttp_search(session, qvec):
            async with semaphore:
                t0 = time.perf_counter()
                async with session.post(f"{VESPA_HOST}/search/", json=make_body(qvec)) as resp:
                    await resp.json()
                return (time.perf_counter() - t0) * 1000

        async with aiohttp.ClientSession() as session:
            warmup_tasks = [aiohttp_search(session, qvec) for qvec in warmup_vecs]
            await asyncio.gather(*warmup_tasks)

            t0 = time.perf_counter()
            tasks = [aiohttp_search(session, qvec) for qvec in test_vecs]
            latencies = await asyncio.gather(*tasks)
            wall_time = time.perf_counter() - t0

        print(f"    {num_clients:>2} clients:", end="")
        report(list(latencies), wall_time)

    await asyncio.sleep(15)
    print(f"\n  ... cooldown 15s ...")

    # ── 2. Proposed: pyvespa sync + asyncio.to_thread ───────────────
    print(f"\n  2. Proposed OSB pattern (pyvespa sync + to_thread, dedicated executor)")
    print(f"  {'─'*50}")

    from vespa.application import Vespa as PyvespaApp

    vespa = PyvespaApp(url=VESPA_HOST)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=64, thread_name_prefix="vespa")

    async def _run(fn, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(executor, functools.partial(fn, *args, **kwargs))

    for num_clients in CONCURRENCY_LEVELS:
        semaphore = asyncio.Semaphore(num_clients)

        async def pyvespa_search(qvec):
            async with semaphore:
                t0 = time.perf_counter()
                await _run(vespa.query, body=make_body(qvec))
                return (time.perf_counter() - t0) * 1000

        # Warmup
        warmup_tasks = [pyvespa_search(qvec) for qvec in warmup_vecs]
        await asyncio.gather(*warmup_tasks)

        t0 = time.perf_counter()
        tasks = [pyvespa_search(qvec) for qvec in test_vecs]
        latencies = await asyncio.gather(*tasks)
        wall_time = time.perf_counter() - t0

        print(f"    {num_clients:>2} clients:", end="")
        report(list(latencies), wall_time)

    executor.shutdown(wait=True)

    print(f"\n{'='*70}")
    print("Done!")

asyncio.run(main())
