#!/usr/bin/env python3
"""
Test pyvespa async with different connection pool sizes.
The default is 1 (HTTP/2 multiplexed). Let's see if more connections help.

Run: python3.11 vespa_async_connections_test.py
"""

import asyncio
import time
import logging
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


async def main():
    from vespa.application import Vespa as PyvespaApp

    print(f"pyvespa async — varying connection pool size")
    print(f"32 concurrent clients, {NUM_QUERIES} queries, {WARMUP} warmup")
    print(f"{'='*70}")

    vespa_app = PyvespaApp(url=VESPA_HOST)
    num_clients = 32
    semaphore = asyncio.Semaphore(num_clients)

    for num_connections in [1, 2, 4, 8, 16, 32]:
        async with vespa_app.asyncio(connections=num_connections, timeout=30) as vespa_async:
            async def search(qvec):
                async with semaphore:
                    t0 = time.perf_counter()
                    await vespa_async.query(body=make_body(qvec))
                    return (time.perf_counter() - t0) * 1000

            # Warmup
            warmup_tasks = [search(qvec) for qvec in warmup_vecs]
            await asyncio.gather(*warmup_tasks)

            # Measured
            t0 = time.perf_counter()
            tasks = [search(qvec) for qvec in test_vecs]
            latencies = await asyncio.gather(*tasks)
            wall_time = time.perf_counter() - t0

        latencies = list(latencies)
        qps = len(latencies) / wall_time
        print(f"  {num_connections:>2} connections: QPS={qps:>7.0f}  p50={percentile(latencies, 50):>5.1f}ms"
              f"  p90={percentile(latencies, 90):>5.1f}ms  p99={percentile(latencies, 99):>5.1f}ms")

        await asyncio.sleep(5)  # Brief cooldown

    print(f"\n{'='*70}")
    print("Done!")

asyncio.run(main())
