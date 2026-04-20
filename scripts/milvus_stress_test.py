#!/usr/bin/env python3
"""
Milvus stress test — validates assumptions before OSB implementation.

Tests:
1. 768-dim / 100K docs — realistic vectorsearch scale
2. Concurrent search via ThreadPoolExecutor (simulates asyncio.to_thread pattern)
3. Error behavior — exception types for runner catch blocks

Run from the test runner node:
    python3.11 milvus_stress_test.py

Requires: pip install pymilvus numpy
"""

import concurrent.futures
import time
import traceback
import numpy as np
from pymilvus import MilvusClient, DataType

MILVUS_URI = "http://10.0.143.186:19530"
COLLECTION = "stress_test"
DIM = 768
NUM_DOCS = 100_000
BATCH_SIZE = 2000
K = 100
EF_CONSTRUCTION = 200
EF_SEARCH = 100
M = 16
METRIC = "COSINE"
CONCURRENT_CLIENTS = [1, 8, 32]

client = MilvusClient(uri=MILVUS_URI, timeout=300)
print(f"Connected to Milvus {client.get_server_version()}")
print(f"=" * 70)

# ═══════════════════════════════════════════════════════════════════════
# TEST 1: 768-dim / 100K docs — realistic scale
# ═══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"TEST 1: {DIM}-dim, {NUM_DOCS:,} docs, HNSW M={M}")
print(f"{'='*70}")

# Drop if exists
if client.has_collection(COLLECTION):
    client.drop_collection(COLLECTION)
    for _ in range(20):
        if not client.has_collection(COLLECTION):
            break
        time.sleep(0.5)

# Create
schema = client.create_schema()
schema.add_field(field_name="doc_id", datatype=DataType.INT64, is_primary=True, auto_id=False)
schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=DIM)

index_params = client.prepare_index_params()
index_params.add_index(
    field_name="embedding",
    index_type="HNSW",
    metric_type=METRIC,
    params={"M": M, "efConstruction": EF_CONSTRUCTION},
)

t0 = time.perf_counter()
client.create_collection(collection_name=COLLECTION, schema=schema, index_params=index_params)
print(f"  Collection created in {time.perf_counter() - t0:.2f}s")

# Insert in batches
print(f"\n  Inserting {NUM_DOCS:,} docs in batches of {BATCH_SIZE}...")
np.random.seed(42)
total_inserted = 0
t0 = time.perf_counter()

for batch_start in range(0, NUM_DOCS, BATCH_SIZE):
    batch_end = min(batch_start + BATCH_SIZE, NUM_DOCS)
    vectors = np.random.randn(batch_end - batch_start, DIM).astype(np.float32)
    batch = [
        {"doc_id": i, "embedding": vectors[i - batch_start].tolist()}
        for i in range(batch_start, batch_end)
    ]
    result = client.insert(collection_name=COLLECTION, data=batch)
    total_inserted += result.get("insert_count", 0)

    if (batch_start + BATCH_SIZE) % 20_000 == 0:
        elapsed = time.perf_counter() - t0
        print(f"    {total_inserted:>7,} docs, {total_inserted / elapsed:.0f} docs/s")

insert_time = time.perf_counter() - t0
print(f"  Insert complete: {total_inserted:,} docs in {insert_time:.1f}s "
      f"({total_inserted / insert_time:.0f} docs/s)")

# Flush
print(f"\n  Flushing...")
t0 = time.perf_counter()
client.flush(collection_name=COLLECTION)
print(f"  Flush: {time.perf_counter() - t0:.1f}s")

# Compact
print(f"  Compacting...")
t0 = time.perf_counter()
job_id = client.compact(collection_name=COLLECTION)
for i in range(120):
    state = client.get_compaction_state(job_id)
    if state == "Completed":
        break
    time.sleep(1)
print(f"  Compact: {time.perf_counter() - t0:.1f}s (state: {state})")

# Load (should be no-op since create auto-loaded)
t0 = time.perf_counter()
client.load_collection(collection_name=COLLECTION)
print(f"  Load: {time.perf_counter() - t0:.1f}s")

# Stats
stats = client.get_collection_stats(COLLECTION)
print(f"  Stats: {stats}")

# Quick single-query search to verify
print(f"\n  Single search warmup (k={K}, ef={EF_SEARCH})...")
query_vec = np.random.randn(DIM).astype(np.float32).tolist()
t0 = time.perf_counter()
results = client.search(
    collection_name=COLLECTION, data=[query_vec], anns_field="embedding",
    limit=K, output_fields=["doc_id"], search_params={"params": {"ef": EF_SEARCH}},
)
print(f"  Warmup search: {(time.perf_counter() - t0)*1000:.1f}ms, {len(results[0])} hits")

# ═══════════════════════════════════════════════════════════════════════
# TEST 2: Concurrent search via ThreadPoolExecutor
# ═══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"TEST 2: Concurrent search (simulates asyncio.to_thread pattern)")
print(f"{'='*70}")

# Pre-generate query vectors
NUM_QUERIES = 200
query_vectors = [np.random.randn(DIM).astype(np.float32).tolist() for _ in range(NUM_QUERIES)]

def do_search(qvec):
    """Sync search — this is what runs inside each thread."""
    t0 = time.perf_counter()
    results = client.search(
        collection_name=COLLECTION, data=[qvec], anns_field="embedding",
        limit=K, output_fields=["doc_id"], search_params={"params": {"ef": EF_SEARCH}},
    )
    elapsed_ms = (time.perf_counter() - t0) * 1000
    return {"hits": len(results[0]), "latency_ms": elapsed_ms}

for num_clients in CONCURRENT_CLIENTS:
    print(f"\n  {num_clients} concurrent clients, {NUM_QUERIES} queries...")
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=num_clients)

    t0 = time.perf_counter()
    futures = [executor.submit(do_search, qvec) for qvec in query_vectors]
    results_list = [f.result() for f in futures]
    wall_time = time.perf_counter() - t0

    latencies = sorted([r["latency_ms"] for r in results_list])
    qps = NUM_QUERIES / wall_time

    print(f"    QPS: {qps:.0f}")
    print(f"    p50: {latencies[len(latencies)//2]:.1f}ms")
    print(f"    p90: {latencies[int(len(latencies)*0.9)]:.1f}ms")
    print(f"    p99: {latencies[int(len(latencies)*0.99)]:.1f}ms")
    print(f"    Wall time: {wall_time:.2f}s")

    executor.shutdown(wait=True)

# ═══════════════════════════════════════════════════════════════════════
# TEST 3: Error behavior — what exceptions does pymilvus raise?
# ═══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"TEST 3: Error behavior (exception types for runner catch blocks)")
print(f"{'='*70}")

# 3a. Search on non-existent collection
print(f"\n  3a. Search on non-existent collection:")
try:
    client.search(
        collection_name="does_not_exist", data=[query_vectors[0]],
        anns_field="embedding", limit=10,
        search_params={"params": {"ef": 100}},
    )
    print("    No error (unexpected)")
except Exception as e:
    print(f"    Exception type: {type(e).__module__}.{type(e).__name__}")
    print(f"    Message: {e}")

# 3b. Insert wrong dimension vector
print(f"\n  3b. Insert wrong dimension vector (expect dim mismatch):")
try:
    client.insert(
        collection_name=COLLECTION,
        data=[{"doc_id": 999999, "embedding": [1.0, 2.0, 3.0]}],  # 3-dim, expect 768
    )
    print("    No error (unexpected)")
except Exception as e:
    print(f"    Exception type: {type(e).__module__}.{type(e).__name__}")
    print(f"    Message: {e}")

# 3c. Insert duplicate primary key
print(f"\n  3c. Insert duplicate primary key:")
try:
    client.insert(
        collection_name=COLLECTION,
        data=[{"doc_id": 0, "embedding": np.random.randn(DIM).astype(np.float32).tolist()}],
    )
    print("    No error (Milvus allows duplicate PKs on insert — they append, not upsert)")
except Exception as e:
    print(f"    Exception type: {type(e).__module__}.{type(e).__name__}")
    print(f"    Message: {e}")

# 3d. Search with wrong field name
print(f"\n  3d. Search with wrong anns_field:")
try:
    client.search(
        collection_name=COLLECTION, data=[query_vectors[0]],
        anns_field="nonexistent_field", limit=10,
        search_params={"params": {"ef": 100}},
    )
    print("    No error (unexpected)")
except Exception as e:
    print(f"    Exception type: {type(e).__module__}.{type(e).__name__}")
    print(f"    Message: {e}")

# 3e. Create collection that already exists
print(f"\n  3e. Create collection that already exists:")
try:
    schema2 = client.create_schema()
    schema2.add_field(field_name="doc_id", datatype=DataType.INT64, is_primary=True, auto_id=False)
    schema2.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=DIM)
    ip2 = client.prepare_index_params()
    ip2.add_index(field_name="embedding", index_type="HNSW", metric_type="COSINE",
                  params={"M": 16, "efConstruction": 200})
    client.create_collection(collection_name=COLLECTION, schema=schema2, index_params=ip2)
    print("    No error (unexpected)")
except Exception as e:
    print(f"    Exception type: {type(e).__module__}.{type(e).__name__}")
    print(f"    Message: {e}")

# 3f. Drop non-existent collection
print(f"\n  3f. Drop non-existent collection:")
try:
    client.drop_collection("does_not_exist")
    print("    No error (silent no-op)")
except Exception as e:
    print(f"    Exception type: {type(e).__module__}.{type(e).__name__}")
    print(f"    Message: {e}")

# 3g. Connection to dead host (quick timeout)
print(f"\n  3g. Connection to unreachable host (timeout=3s):")
try:
    dead_client = MilvusClient(uri="http://10.0.0.1:19530", timeout=3)
    dead_client.list_collections()
    print("    No error (unexpected)")
except Exception as e:
    print(f"    Exception type: {type(e).__module__}.{type(e).__name__}")
    print(f"    Message: {e}")

# ═══════════════════════════════════════════════════════════════════════
# Cleanup
# ═══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"Cleanup")
print(f"{'='*70}")
client.drop_collection(COLLECTION)
client.close()
print("Done!")
