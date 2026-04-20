#!/usr/bin/env python3
"""
Milvus client pool benchmark — measures the impact of GIL contention
and compares single client vs client pool at various concurrency levels.

Also measures raw pymilvus overhead vs what OSB would add.

Run from the test runner node:
    python3.11 milvus_client_pool_test.py
"""

import concurrent.futures
import time
import threading
import numpy as np
from pymilvus import MilvusClient

MILVUS_URI = "http://10.0.143.186:19530"
COLLECTION = "target_index"  # Already loaded with 1M docs
DIM = 768
K = 100
EF_SEARCH = 100
NUM_QUERIES = 1000
CONCURRENCY_LEVELS = [1, 8, 16, 32, 64]
POOL_SIZES = [1, 4, 8, 16]

# ── Generate query vectors ──────────────────────────────────────────
print("Generating query vectors...")
np.random.seed(42)
query_vectors = [np.random.randn(DIM).astype(np.float32).tolist() for _ in range(NUM_QUERIES)]
search_params = {"params": {"ef": EF_SEARCH}}

# ── Test 1: Single client baseline (matches current OSB implementation) ──
print(f"\n{'='*70}")
print(f"TEST 1: Single MilvusClient (current OSB pattern)")
print(f"{'='*70}")

single_client = MilvusClient(uri=MILVUS_URI, dedicated=True)

def search_single(qvec):
    t0 = time.perf_counter()
    results = single_client.search(
        collection_name=COLLECTION, data=[qvec], anns_field="embedding",
        limit=K, output_fields=["doc_id"], search_params=search_params,
    )
    return (time.perf_counter() - t0) * 1000

for num_clients in CONCURRENCY_LEVELS:
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=num_clients)
    t0 = time.perf_counter()
    futures = [executor.submit(search_single, qvec) for qvec in query_vectors]
    latencies = sorted([f.result() for f in futures])
    wall_time = time.perf_counter() - t0
    qps = NUM_QUERIES / wall_time

    print(f"\n  {num_clients:>2} threads, 1 client:")
    print(f"    QPS: {qps:>8.0f}   p50: {latencies[len(latencies)//2]:>6.1f}ms"
          f"   p99: {latencies[int(len(latencies)*0.99)]:>6.1f}ms")
    executor.shutdown(wait=True)

single_client.close()

# ── Test 2: Client pool (round-robin across N clients) ──────────────
print(f"\n{'='*70}")
print(f"TEST 2: Client pool (round-robin)")
print(f"{'='*70}")

for pool_size in POOL_SIZES:
    clients = [MilvusClient(uri=MILVUS_URI, dedicated=True) for _ in range(pool_size)]
    counter = [0]
    lock = threading.Lock()

    def search_pooled(qvec):
        with lock:
            idx = counter[0] % len(clients)
            counter[0] += 1
        client = clients[idx]
        t0 = time.perf_counter()
        results = client.search(
            collection_name=COLLECTION, data=[qvec], anns_field="embedding",
            limit=K, output_fields=["doc_id"], search_params=search_params,
        )
        return (time.perf_counter() - t0) * 1000

    for num_threads in CONCURRENCY_LEVELS:
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=num_threads)
        t0 = time.perf_counter()
        futures = [executor.submit(search_pooled, qvec) for qvec in query_vectors]
        latencies = sorted([f.result() for f in futures])
        wall_time = time.perf_counter() - t0
        qps = NUM_QUERIES / wall_time

        print(f"\n  {num_threads:>2} threads, {pool_size} clients:")
        print(f"    QPS: {qps:>8.0f}   p50: {latencies[len(latencies)//2]:>6.1f}ms"
              f"   p99: {latencies[int(len(latencies)*0.99)]:>6.1f}ms")
        executor.shutdown(wait=True)

    for c in clients:
        c.close()

# ── Test 3: Thread-local clients (each thread gets its own) ────────
print(f"\n{'='*70}")
print(f"TEST 3: Thread-local clients (one per thread)")
print(f"{'='*70}")

local = threading.local()

def search_thread_local(qvec):
    if not hasattr(local, 'client'):
        local.client = MilvusClient(uri=MILVUS_URI, dedicated=True)
    t0 = time.perf_counter()
    results = local.client.search(
        collection_name=COLLECTION, data=[qvec], anns_field="embedding",
        limit=K, output_fields=["doc_id"], search_params=search_params,
    )
    return (time.perf_counter() - t0) * 1000

for num_threads in CONCURRENCY_LEVELS:
    local = threading.local()  # Reset for each concurrency level
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=num_threads)

    # Warmup — let all threads create their clients
    warmup_futures = [executor.submit(search_thread_local, query_vectors[0]) for _ in range(num_threads)]
    for f in warmup_futures:
        f.result()

    t0 = time.perf_counter()
    futures = [executor.submit(search_thread_local, qvec) for qvec in query_vectors]
    latencies = sorted([f.result() for f in futures])
    wall_time = time.perf_counter() - t0
    qps = NUM_QUERIES / wall_time

    print(f"\n  {num_threads:>2} threads, {num_threads} clients (thread-local):")
    print(f"    QPS: {qps:>8.0f}   p50: {latencies[len(latencies)//2]:>6.1f}ms"
          f"   p99: {latencies[int(len(latencies)*0.99)]:>6.1f}ms")
    executor.shutdown(wait=True)

# ── Test 4: Measure OSB-like overhead ───────────────────────────────
print(f"\n{'='*70}")
print(f"TEST 4: OSB overhead simulation")
print(f"{'='*70}")

client = MilvusClient(uri=MILVUS_URI, dedicated=True)

# Raw pymilvus call
latencies_raw = []
for qvec in query_vectors[:200]:
    t0 = time.perf_counter()
    results = client.search(
        collection_name=COLLECTION, data=[qvec], anns_field="embedding",
        limit=K, output_fields=["doc_id"], search_params=search_params,
    )
    latencies_raw.append((time.perf_counter() - t0) * 1000)

# With OSB-like overhead: asyncio.to_thread dispatch + response conversion
import asyncio
import functools

async def search_with_osb_overhead(qvec):
    loop = asyncio.get_running_loop()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    t0 = time.perf_counter()
    # Simulate run_in_executor (same as OSB's _run())
    raw_result = await loop.run_in_executor(
        executor,
        functools.partial(
            client.search,
            collection_name=COLLECTION, data=[qvec], anns_field="embedding",
            limit=K, output_fields=["doc_id"], search_params=search_params,
        )
    )
    # Simulate convert_milvus_search_response
    hits_list = raw_result[0] if raw_result else []
    os_hits = [{"_id": str(h.get("doc_id", "")), "_score": h.get("distance", 0.0)} for h in hits_list]
    response = {"hits": {"total": {"value": len(os_hits)}, "hits": os_hits}}
    elapsed = (time.perf_counter() - t0) * 1000

    executor.shutdown(wait=False)
    return elapsed

async def run_osb_sim():
    latencies = []
    for qvec in query_vectors[:200]:
        lat = await search_with_osb_overhead(qvec)
        latencies.append(lat)
    return latencies

latencies_osb = asyncio.run(run_osb_sim())

latencies_raw.sort()
latencies_osb.sort()
n = len(latencies_raw)

print(f"\n  Raw pymilvus (200 sequential queries):")
print(f"    p50: {latencies_raw[n//2]:.2f}ms   p99: {latencies_raw[int(n*0.99)]:.2f}ms")
print(f"\n  With OSB overhead (run_in_executor + response conversion):")
print(f"    p50: {latencies_osb[n//2]:.2f}ms   p99: {latencies_osb[int(n*0.99)]:.2f}ms")
print(f"\n  Overhead: {latencies_osb[n//2] - latencies_raw[n//2]:.2f}ms at p50")

client.close()
print(f"\n{'='*70}")
print("Done!")
