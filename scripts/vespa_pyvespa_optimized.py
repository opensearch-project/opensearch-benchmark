#!/usr/bin/env python3
"""
Optimized pyvespa sync approaches vs baseline aiohttp.

Tests:
1. Baseline aiohttp (for comparison)
2. pyvespa sync default (current proposed pattern)
3. pyvespa sync per-thread instances (avoid sharing)
4. httpr sync direct (bypass pyvespa wrapper, use httpr.Client directly)

Run: python3.11 vespa_pyvespa_optimized.py
"""

import asyncio
import concurrent.futures
import functools
import threading
import time
import logging
import json
import numpy as np

logging.getLogger("httpr").setLevel(logging.WARNING)

VESPA_HOST = "http://10.0.142.54:8080"
DIM = 768
K = 100
NUM_QUERIES = 2000
WARMUP = 200

np.random.seed(42)
query_vectors = [np.random.randn(DIM).astype(np.float32) for _ in range(NUM_QUERIES + WARMUP)]
warmup_vecs = query_vectors[:WARMUP]
test_vecs = query_vectors[WARMUP:]

NUM_CLIENTS = 32

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

def report(name, latencies, wall_time):
    qps = len(latencies) / wall_time
    print(f"  {name}")
    print(f"    QPS: {qps:>7.0f}   p50: {percentile(latencies, 50):>5.1f}ms"
          f"   p90: {percentile(latencies, 90):>5.1f}ms"
          f"   p99: {percentile(latencies, 99):>5.1f}ms")


async def main():
    print(f"Pyvespa Optimization Test ({NUM_QUERIES} queries, {NUM_CLIENTS} clients)")
    print(f"{'='*70}")

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=64, thread_name_prefix="vespa")

    async def _run(fn, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(executor, functools.partial(fn, *args, **kwargs))

    # ── 1. Baseline: aiohttp ────────────────────────────────────────
    import aiohttp
    semaphore = asyncio.Semaphore(NUM_CLIENTS)

    async def aiohttp_search(session, qvec):
        async with semaphore:
            t0 = time.perf_counter()
            async with session.post(f"{VESPA_HOST}/search/", json=make_body(qvec)) as resp:
                await resp.json()
            return (time.perf_counter() - t0) * 1000

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*[aiohttp_search(session, qvec) for qvec in warmup_vecs])
        t0 = time.perf_counter()
        latencies = await asyncio.gather(*[aiohttp_search(session, qvec) for qvec in test_vecs])
        wall_time = time.perf_counter() - t0
    report("1. Baseline aiohttp", list(latencies), wall_time)

    await asyncio.sleep(10)

    # ── 2. pyvespa sync shared instance ─────────────────────────────
    from vespa.application import Vespa as PyvespaApp
    vespa = PyvespaApp(url=VESPA_HOST)
    semaphore = asyncio.Semaphore(NUM_CLIENTS)

    async def pyvespa_shared(qvec):
        async with semaphore:
            t0 = time.perf_counter()
            await _run(vespa.query, body=make_body(qvec))
            return (time.perf_counter() - t0) * 1000

    await asyncio.gather(*[pyvespa_shared(qvec) for qvec in warmup_vecs])
    t0 = time.perf_counter()
    latencies = await asyncio.gather(*[pyvespa_shared(qvec) for qvec in test_vecs])
    wall_time = time.perf_counter() - t0
    report("2. pyvespa sync (shared instance)", list(latencies), wall_time)

    await asyncio.sleep(10)

    # ── 3. pyvespa sync thread-local instances ──────────────────────
    local = threading.local()

    def pyvespa_threadlocal_search(qvec):
        if not hasattr(local, 'vespa'):
            local.vespa = PyvespaApp(url=VESPA_HOST)
        t0 = time.perf_counter()
        local.vespa.query(body=make_body(qvec))
        return (time.perf_counter() - t0) * 1000

    semaphore = asyncio.Semaphore(NUM_CLIENTS)

    async def pyvespa_threadlocal(qvec):
        async with semaphore:
            return await _run(pyvespa_threadlocal_search, qvec)

    await asyncio.gather(*[pyvespa_threadlocal(qvec) for qvec in warmup_vecs])
    t0 = time.perf_counter()
    latencies = await asyncio.gather(*[pyvespa_threadlocal(qvec) for qvec in test_vecs])
    wall_time = time.perf_counter() - t0
    report("3. pyvespa sync (thread-local instances)", list(latencies), wall_time)

    await asyncio.sleep(10)

    # ── 4. httpr.Client direct (bypass pyvespa wrapper) ─────────────
    try:
        import httpr

        http_client = httpr.Client()

        def httpr_direct_search(qvec):
            body = make_body(qvec)
            t0 = time.perf_counter()
            resp = http_client.post(
                f"{VESPA_HOST}/search/",
                json=body,
                headers={"Content-Type": "application/json"},
            )
            resp.json()
            return (time.perf_counter() - t0) * 1000

        semaphore = asyncio.Semaphore(NUM_CLIENTS)

        async def httpr_search(qvec):
            async with semaphore:
                return await _run(httpr_direct_search, qvec)

        await asyncio.gather(*[httpr_search(qvec) for qvec in warmup_vecs])
        t0 = time.perf_counter()
        latencies = await asyncio.gather(*[httpr_search(qvec) for qvec in test_vecs])
        wall_time = time.perf_counter() - t0
        report("4. httpr.Client direct (no pyvespa wrapper)", list(latencies), wall_time)
    except ImportError:
        print("  4. httpr not available, skipping")

    executor.shutdown(wait=True)
    print(f"\n{'='*70}")
    print("Done!")

asyncio.run(main())
