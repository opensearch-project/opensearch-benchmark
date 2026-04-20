"""
Direct vectorsearch comparison: OpenSearch vs Vespa
Bypasses OSB entirely — raw HTTP queries with recall calculation.

Usage: python3.11 scripts/direct_comparison.py
"""

import h5py
import json
import time
import urllib.request
import urllib.parse
import statistics

DATA_PATH = "/home/ec2-user/.benchmark/benchmarks/data/cohere-10m/documents-10m.hdf5"
OS_HOST = "http://10.0.137.227:9200"
VESPA_HOST = "http://10.0.142.54:8080"
NUM_QUERIES = 100
K = 100
EF_SEARCH = 256


def load_data():
    f = h5py.File(DATA_PATH, "r")
    queries = f["test"][:NUM_QUERIES].tolist()
    neighbors = [[str(n) for n in row[:K]] for row in f["neighbors"][:NUM_QUERIES]]
    f.close()
    return queries, neighbors


def search_opensearch(query_vec):
    body = json.dumps({
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
    }).encode()

    req = urllib.request.Request(
        f"{OS_HOST}/vdb_bench_index/_search",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    resp = json.loads(urllib.request.urlopen(req).read())
    hits = resp.get("hits", {}).get("hits", [])
    return [h["_id"] for h in hits]


def search_vespa(query_vec):
    explore_additional = EF_SEARCH - K
    yql = (
        f"select * from target_index where "
        f"{{targetHits:{K},approximate:true,hnsw.exploreAdditionalHits:{explore_additional}}}"
        f"nearestNeighbor(embedding, query_vector)"
    )
    params = urllib.parse.urlencode({
        "yql": yql,
        "input.query(query_vector)": json.dumps(query_vec),
        "ranking": "vector-similarity",
        "hits": K,
    })
    url = f"{VESPA_HOST}/search/?{params}"
    resp = json.loads(urllib.request.urlopen(url).read())
    children = resp.get("root", {}).get("children", [])
    return [c["id"].rsplit("::", 1)[-1] for c in children]


def calculate_recall_k(returned_ids, true_neighbors):
    truth_set = set(true_neighbors[:K])
    correct = sum(1 for rid in returned_ids if rid in truth_set)
    return correct / len(truth_set) if truth_set else 0.0


def calculate_recall_1(returned_ids, true_neighbors):
    if not returned_ids or not true_neighbors:
        return 0.0
    return 1.0 if returned_ids[0] in true_neighbors[:1] else 0.0


def benchmark(name, search_fn, queries, neighbors):
    recalls_k = []
    recalls_1 = []
    latencies = []

    # Warmup (5 queries, not measured)
    for q in queries[:5]:
        search_fn(q)

    # Timed queries
    for query_vec, true_neighbors in zip(queries, neighbors):
        start = time.perf_counter()
        returned_ids = search_fn(query_vec)
        elapsed_ms = (time.perf_counter() - start) * 1000

        latencies.append(elapsed_ms)
        recalls_k.append(calculate_recall_k(returned_ids, true_neighbors))
        recalls_1.append(calculate_recall_1(returned_ids, true_neighbors))

    total_time_s = sum(latencies) / 1000
    qps = len(latencies) / total_time_s
    latencies_sorted = sorted(latencies)

    print(f"\n{'='*50}")
    print(f"  {name} ({NUM_QUERIES} queries, K={K}, ef_search={EF_SEARCH})")
    print(f"{'='*50}")
    print(f"  QPS (sequential):  {qps:.1f}")
    print(f"  p50 latency:       {latencies_sorted[len(latencies)//2]:.1f} ms")
    print(f"  p90 latency:       {latencies_sorted[int(len(latencies)*0.9)]:.1f} ms")
    print(f"  p99 latency:       {latencies_sorted[int(len(latencies)*0.99)]:.1f} ms")
    print(f"  Mean recall@{K}:    {statistics.mean(recalls_k):.4f}")
    print(f"  Mean recall@1:     {statistics.mean(recalls_1):.4f}")
    print(f"  Recall@k range:    [{min(recalls_k):.2f}, {max(recalls_k):.2f}]")


def main():
    print(f"Loading {NUM_QUERIES} queries from {DATA_PATH}...")
    queries, neighbors = load_data()
    print(f"Loaded. Vector dim: {len(queries[0])}, K: {K}, ef_search: {EF_SEARCH}")

    benchmark("OpenSearch 3.4.0", search_opensearch, queries, neighbors)
    benchmark("Vespa 8.660", search_vespa, queries, neighbors)


if __name__ == "__main__":
    main()
