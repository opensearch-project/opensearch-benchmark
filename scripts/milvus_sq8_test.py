#!/usr/bin/env python3
"""
Quick test: HNSW vs IVF_SQ8 on the same 1M dataset.
Creates a new collection with SQ8 quantization, copies data via query+insert,
then benchmarks search throughput.

Run from the test runner node:
    python3.11 milvus_sq8_test.py
"""

import concurrent.futures
import time
import numpy as np
from pymilvus import MilvusClient, DataType

MILVUS_URI = "http://10.0.143.186:19530"
SOURCE_COLLECTION = "target_index"  # Existing HNSW collection with 1M docs
SQ8_COLLECTION = "target_index_sq8"
DIM = 768
K = 100
NUM_QUERIES = 1000
CONCURRENCY_LEVELS = [1, 8, 32]

client = MilvusClient(uri=MILVUS_URI, timeout=300, dedicated=True)
print(f"Connected to Milvus {client.get_server_version()}")

# ── Check if SQ8 collection already exists ──────────────────────────
if client.has_collection(SQ8_COLLECTION):
    stats = client.get_collection_stats(SQ8_COLLECTION)
    print(f"SQ8 collection exists with {stats.get('row_count', 0)} rows")
else:
    print(f"\nCreating SQ8 collection...")

    schema = client.create_schema()
    schema.add_field(field_name="doc_id", datatype=DataType.INT64, is_primary=True, auto_id=False)
    schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=DIM)

    # IVF_SQ8: scalar quantization (32-bit -> 8-bit), IVF clustering
    index_params = client.prepare_index_params()
    index_params.add_index(
        field_name="embedding",
        index_type="IVF_SQ8",
        metric_type="IP",
        params={"nlist": 1024},  # Number of IVF clusters
    )

    client.create_collection(
        collection_name=SQ8_COLLECTION,
        schema=schema,
        index_params=index_params,
    )
    print(f"Created {SQ8_COLLECTION} with IVF_SQ8 index")

    # ── Copy data from HNSW collection ──────────────────────────────
    print(f"\nCopying data from {SOURCE_COLLECTION}...")
    batch_size = 2000
    total = 1_000_000
    t0 = time.perf_counter()

    for offset in range(0, total, batch_size):
        # Query source collection for a batch
        results = client.query(
            collection_name=SOURCE_COLLECTION,
            filter=f"doc_id >= {offset} and doc_id < {offset + batch_size}",
            output_fields=["doc_id", "embedding"],
            limit=batch_size,
        )
        if not results:
            break

        # Insert into SQ8 collection
        docs = [{"doc_id": r["doc_id"], "embedding": r["embedding"]} for r in results]
        client.insert(collection_name=SQ8_COLLECTION, data=docs)

        if (offset + batch_size) % 100_000 == 0:
            elapsed = time.perf_counter() - t0
            print(f"  {offset + batch_size:>10,} docs copied ({elapsed:.0f}s)")

    print(f"\nFlushing...")
    client.flush(collection_name=SQ8_COLLECTION)
    print(f"Loading...")
    client.load_collection(collection_name=SQ8_COLLECTION)
    stats = client.get_collection_stats(SQ8_COLLECTION)
    print(f"SQ8 collection ready: {stats.get('row_count', 0)} rows")

# ── Benchmark: HNSW vs IVF_SQ8 ─────────────────────────────────────
print(f"\n{'='*70}")
print(f"BENCHMARK: HNSW vs IVF_SQ8 (1M docs, {K} results, {NUM_QUERIES} queries)")
print(f"{'='*70}")

np.random.seed(42)
query_vectors = [np.random.randn(DIM).astype(np.float32).tolist() for _ in range(NUM_QUERIES)]

configs = [
    ("HNSW (ef=100)", SOURCE_COLLECTION, {"params": {"ef": 100}}),
    ("IVF_SQ8 (nprobe=32)", SQ8_COLLECTION, {"params": {"nprobe": 32}}),
    ("IVF_SQ8 (nprobe=64)", SQ8_COLLECTION, {"params": {"nprobe": 64}}),
    ("IVF_SQ8 (nprobe=128)", SQ8_COLLECTION, {"params": {"nprobe": 128}}),
]

for name, collection, search_params in configs:
    print(f"\n  {name}:")
    for num_threads in CONCURRENCY_LEVELS:
        def do_search(qvec, coll=collection, sp=search_params):
            t0 = time.perf_counter()
            client.search(
                collection_name=coll, data=[qvec], anns_field="embedding",
                limit=K, output_fields=["doc_id"], search_params=sp,
            )
            return (time.perf_counter() - t0) * 1000

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=num_threads)
        t0 = time.perf_counter()
        futures = [executor.submit(do_search, qvec) for qvec in query_vectors]
        latencies = sorted([f.result() for f in futures])
        wall_time = time.perf_counter() - t0
        qps = NUM_QUERIES / wall_time

        print(f"    {num_threads:>2}t: QPS={qps:>6.0f}  p50={latencies[len(latencies)//2]:>5.1f}ms"
              f"  p99={latencies[int(len(latencies)*0.99)]:>6.1f}ms")
        executor.shutdown(wait=True)

# ── Cleanup option ──────────────────────────────────────────────────
print(f"\n(Keeping {SQ8_COLLECTION} for further testing. Drop manually if needed.)")
client.close()
print("Done!")
