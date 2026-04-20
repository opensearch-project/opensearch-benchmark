#!/usr/bin/env python3
"""
Unified OSB overhead verification — raw SDK calls using each engine's
native Python SDK, without OSB's framework overhead.

- OpenSearch: opensearch-py AsyncOpenSearch
- Vespa: pyvespa Vespa.query()
- Milvus: pymilvus MilvusClient.search()

Run from the test runner node:
    python3.11 verify_osb_overhead.py

Requires: pip install opensearch-py pyvespa pymilvus numpy
"""

import time
import numpy as np

NUM_QUERIES = 200
DIM = 768
K = 100

np.random.seed(42)
query_vectors = [np.random.randn(DIM).astype(np.float32) for _ in range(NUM_QUERIES)]

def percentile(latencies, p):
    latencies.sort()
    idx = int(len(latencies) * p / 100)
    return latencies[min(idx, len(latencies) - 1)]


print(f"Verifying OSB overhead — native Python SDKs ({NUM_QUERIES} queries each)")
print(f"{'='*70}")

# ── OpenSearch (opensearch-py) ──────────────────────────────────────
print(f"\n  OpenSearch (10.0.137.227:9200)")
print(f"  SDK: opensearch-py OpenSearch (sync)")
print(f"  {'─'*50}")

try:
    from opensearchpy import OpenSearch

    os_client = OpenSearch(
        hosts=[{"host": "10.0.137.227", "port": 9200}],
        use_ssl=False,
    )

    count = os_client.count(index="vector_1m").get("count", 0)
    print(f"  Index: vector_1m ({count:,} docs)")

    latencies = []
    for qvec in query_vectors:
        body = {
            "size": K,
            "query": {
                "knn": {
                    "embedding": {
                        "vector": qvec.tolist(),
                        "k": K
                    }
                }
            },
            "_source": False,
            "docvalue_fields": ["_id"]
        }
        t0 = time.perf_counter()
        os_client.search(index="vector_1m", body=body)
        latencies.append((time.perf_counter() - t0) * 1000)

    print(f"  Raw SDK:  p50={percentile(latencies, 50):.1f}ms  p99={percentile(latencies, 99):.1f}ms")
    os_client.close()
except Exception as e:
    print(f"  Skipped: {e}")

# ── Vespa (pyvespa) ────────────────────────────────────────────────
print(f"\n  Vespa (10.0.142.54:8080)")
print(f"  SDK: pyvespa Vespa.query()")
print(f"  {'─'*50}")

try:
    from vespa.application import Vespa as PyvespaApp
    import logging
    logging.getLogger("httpr").setLevel(logging.WARNING)

    vespa_app = PyvespaApp(url="http://10.0.142.54:8080")
    print(f"  Connected to Vespa")

    latencies = []
    for qvec in query_vectors:
        body = {
            "yql": f"select documentid from target_index where {{targetHits:{K}}}nearestNeighbor(embedding, query_vector)",
            "ranking": "vector-similarity",
            "hits": K,
            "timeout": "10s",
            "input.query(query_vector)": qvec.tolist(),
        }
        t0 = time.perf_counter()
        vespa_app.query(body=body)
        latencies.append((time.perf_counter() - t0) * 1000)

    print(f"  Raw SDK:  p50={percentile(latencies, 50):.1f}ms  p99={percentile(latencies, 99):.1f}ms")
except Exception as e:
    print(f"  Skipped: {e}")

# ── Milvus (pymilvus) ──────────────────────────────────────────────
print(f"\n  Milvus (10.0.143.186:19530)")
print(f"  SDK: pymilvus MilvusClient.search()")
print(f"  {'─'*50}")

try:
    from pymilvus import MilvusClient

    client = MilvusClient(uri="http://10.0.143.186:19530", dedicated=True)
    stats = client.get_collection_stats("target_index")
    print(f"  Collection: target_index ({stats.get('row_count', 0):,} docs)")

    search_params = {"params": {"ef": 100}}

    latencies = []
    for qvec in query_vectors:
        t0 = time.perf_counter()
        client.search(
            collection_name="target_index", data=[qvec.tolist()],
            anns_field="embedding", limit=K,
            output_fields=["doc_id"], search_params=search_params,
        )
        latencies.append((time.perf_counter() - t0) * 1000)

    print(f"  Raw SDK:  p50={percentile(latencies, 50):.1f}ms  p99={percentile(latencies, 99):.1f}ms")
    client.close()
except Exception as e:
    print(f"  Skipped: {e}")

print(f"\n{'='*70}")
print("Done!")
