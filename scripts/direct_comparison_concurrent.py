"""
Direct vectorsearch comparison: OpenSearch vs Vespa (concurrent)
Bypasses OSB entirely — raw aiohttp queries with recall calculation.

Usage: python3.11 scripts/direct_comparison_concurrent.py
"""

import asyncio
import aiohttp
import h5py
import json
import time
import statistics

DATA_PATH = "/home/ec2-user/.benchmark/benchmarks/data/cohere-10m/documents-10m.hdf5"
OS_HOST = "http://10.0.137.227:9200"
VESPA_HOST = "http://10.0.142.54:8080"
NUM_QUERIES = 1000
K = 100
EF_SEARCH = 256
CONCURRENCY = 32


def load_data():
    f = h5py.File(DATA_PATH, "r")
    queries = f["test"][:NUM_QUERIES].tolist()
    neighbors = [[str(n) for n in row[:K]] for row in f["neighbors"][:NUM_QUERIES]]
    f.close()
    return queries, neighbors


async def search_opensearch(session, query_vec):
    body = {
        "size": K,
        "query": {
            "knn": {
                "embedding": {
                    "vector": query_vec,
                    "k": K
                }
            }
        },
        "_source": False,
        "docvalue_fields": ["_id"]
    }
    async with session.post(
        f"{OS_HOST}/vdb_bench_index/_search",
        json=body
    ) as resp:
        data = await resp.json()
        hits = data.get("hits", {}).get("hits", [])
        return [h["_id"] for h in hits]


async def search_vespa(session, query_vec):
    explore_additional = EF_SEARCH - K
    yql = (
        f"select * from target_index where "
        f"{{targetHits:{K},approximate:true,hnsw.exploreAdditionalHits:{explore_additional}}}"
        f"nearestNeighbor(embedding, query_vector)"
    )
    params = {
        "yql": yql,
        "input.query(query_vector)": json.dumps(query_vec),
        "ranking": "vector-similarity",
        "hits": str(K),
    }
    async with session.get(
        f"{VESPA_HOST}/search/",
        params=params
    ) as resp:
        data = await resp.json()
        children = data.get("root", {}).get("children", [])
        return [c["id"].rsplit("::", 1)[-1] for c in children]


def calculate_recall_k(returned_ids, true_neighbors):
    truth_set = set(true_neighbors[:K])
    correct = sum(1 for rid in returned_ids if rid in truth_set)
    return correct / len(truth_set) if truth_set else 0.0


def calculate_recall_1(returned_ids, true_neighbors):
    if not returned_ids or not true_neighbors:
        return 0.0
    return 1.0 if returned_ids[0] in true_neighbors[:1] else 0.0


async def benchmark(name, search_fn, queries, neighbors):
    sem = asyncio.Semaphore(CONCURRENCY)
    recalls_k = []
    recalls_1 = []
    latencies = []

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=CONCURRENCY),
        timeout=aiohttp.ClientTimeout(total=60)
    ) as session:
        # Warmup
        warmup_tasks = [search_fn(session, q) for q in queries[:CONCURRENCY]]
        await asyncio.gather(*warmup_tasks)

        # Timed queries
        async def run_query(i):
            async with sem:
                start = time.perf_counter()
                returned_ids = await search_fn(session, queries[i])
                elapsed_ms = (time.perf_counter() - start) * 1000
                return i, returned_ids, elapsed_ms

        wall_start = time.perf_counter()
        results = await asyncio.gather(*[run_query(i) for i in range(NUM_QUERIES)])
        wall_elapsed = time.perf_counter() - wall_start

    for i, returned_ids, elapsed_ms in results:
        latencies.append(elapsed_ms)
        recalls_k.append(calculate_recall_k(returned_ids, neighbors[i]))
        recalls_1.append(calculate_recall_1(returned_ids, neighbors[i]))

    latencies_sorted = sorted(latencies)
    qps = NUM_QUERIES / wall_elapsed

    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"  {NUM_QUERIES} queries, K={K}, ef_search={EF_SEARCH}, concurrency={CONCURRENCY}")
    print(f"{'='*60}")
    print(f"  Wall-clock QPS:    {qps:.1f}")
    print(f"  p50 latency:       {latencies_sorted[len(latencies)//2]:.1f} ms")
    print(f"  p90 latency:       {latencies_sorted[int(len(latencies)*0.9)]:.1f} ms")
    print(f"  p99 latency:       {latencies_sorted[int(len(latencies)*0.99)]:.1f} ms")
    print(f"  Mean recall@{K}:    {statistics.mean(recalls_k):.4f}")
    print(f"  Mean recall@1:     {statistics.mean(recalls_1):.4f}")
    print(f"  Recall@k range:    [{min(recalls_k):.2f}, {max(recalls_k):.2f}]")
    print(f"  Total wall time:   {wall_elapsed:.1f}s")


async def main():
    print(f"Loading {NUM_QUERIES} queries from {DATA_PATH}...")
    queries, neighbors = load_data()
    print(f"Loaded. dim={len(queries[0])}, K={K}, ef_search={EF_SEARCH}, concurrency={CONCURRENCY}")

    await benchmark("OpenSearch 3.4.0", search_opensearch, queries, neighbors)
    await benchmark("Vespa 8.660", search_vespa, queries, neighbors)


if __name__ == "__main__":
    asyncio.run(main())
