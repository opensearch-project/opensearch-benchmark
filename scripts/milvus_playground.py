#!/usr/bin/env python3
"""
Milvus playground — exercises the full vectorsearch flow that the OSB
Milvus implementation will use. Run from the test runner node:

    python3.11 milvus_playground.py

Requires: pip install pymilvus numpy
"""

import time
import numpy as np
from pymilvus import MilvusClient, DataType

MILVUS_URI = "http://10.0.143.186:19530"
COLLECTION = "playground"
DIM = 128          # small for quick testing
NUM_DOCS = 10_000
BATCH_SIZE = 2000
K = 10
EF_CONSTRUCTION = 200
EF_SEARCH = 100
M = 16
METRIC = "COSINE"

client = MilvusClient(uri=MILVUS_URI, timeout=300)
print(f"Connected to Milvus {client.get_server_version()}")

# ── 1. Drop if exists (handle async drop race) ──────────────────────────
if client.has_collection(COLLECTION):
    print(f"Dropping existing collection '{COLLECTION}'...")
    client.drop_collection(COLLECTION)
    for _ in range(20):
        if not client.has_collection(COLLECTION):
            break
        time.sleep(0.5)

# ── 2. Create schema ────────────────────────────────────────────────────
print(f"\nCreating collection '{COLLECTION}' ({DIM}-dim, {METRIC}, HNSW M={M})...")
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
client.create_collection(
    collection_name=COLLECTION,
    schema=schema,
    index_params=index_params,
)
print(f"  Created in {time.perf_counter() - t0:.2f}s")
print(f"  Collection exists: {client.has_collection(COLLECTION)}")

# ── 3. Insert vectors in batches ────────────────────────────────────────
print(f"\nInserting {NUM_DOCS:,} docs in batches of {BATCH_SIZE}...")
np.random.seed(42)
all_vectors = np.random.randn(NUM_DOCS, DIM).astype(np.float32)

t0 = time.perf_counter()
total_inserted = 0
for batch_start in range(0, NUM_DOCS, BATCH_SIZE):
    batch_end = min(batch_start + BATCH_SIZE, NUM_DOCS)
    batch = [
        {"doc_id": i, "embedding": all_vectors[i].tolist()}
        for i in range(batch_start, batch_end)
    ]
    result = client.insert(collection_name=COLLECTION, data=batch)
    total_inserted += result.get("insert_count", 0)

insert_time = time.perf_counter() - t0
print(f"  Inserted {total_inserted:,} docs in {insert_time:.2f}s "
      f"({total_inserted / insert_time:.0f} docs/s)")

# ── 4. Flush ────────────────────────────────────────────────────────────
print("\nFlushing...")
t0 = time.perf_counter()
client.flush(collection_name=COLLECTION)
print(f"  Flush completed in {time.perf_counter() - t0:.2f}s")

# ── 5. Compact ──────────────────────────────────────────────────────────
print("\nCompacting...")
t0 = time.perf_counter()
job_id = client.compact(collection_name=COLLECTION)
# Poll for completion — get_compaction_state returns a string
for i in range(120):
    state = client.get_compaction_state(job_id)
    if state == "Completed":
        break
    time.sleep(1)
print(f"  Compaction completed in {time.perf_counter() - t0:.2f}s (state: {state})")

# ── 6. Load collection ─────────────────────────────────────────────────
# Note: create_collection with index_params auto-loads, so this may be a no-op
print("\nLoading collection...")
t0 = time.perf_counter()
client.load_collection(collection_name=COLLECTION)
print(f"  Load completed in {time.perf_counter() - t0:.2f}s")

# ── 7. Collection stats ────────────────────────────────────────────────
stats = client.get_collection_stats(COLLECTION)
print(f"\nCollection stats: {stats}")

# ── 8. Search ───────────────────────────────────────────────────────────
print(f"\nSearching (k={K}, ef={EF_SEARCH})...")

# Use first 5 vectors as queries (we know their IDs for recall check)
num_queries = 5
query_vectors = all_vectors[:num_queries].tolist()

search_params = {"params": {"ef": EF_SEARCH}}

latencies = []
for i, qvec in enumerate(query_vectors):
    t0 = time.perf_counter()
    results = client.search(
        collection_name=COLLECTION,
        data=[qvec],
        anns_field="embedding",
        limit=K,
        output_fields=["doc_id"],
        search_params=search_params,
    )
    elapsed_ms = (time.perf_counter() - t0) * 1000
    latencies.append(elapsed_ms)

    hits = results[0]
    hit_ids = [h["doc_id"] for h in hits]
    top_score = hits[0]["distance"] if hits else "N/A"

    # For COSINE with the query vector itself in the corpus,
    # the top hit should be the query's own doc_id with distance ~0
    self_found = i in hit_ids
    print(f"  Query {i}: {len(hits)} hits, top_score={top_score:.4f}, "
          f"self_in_top_{K}={'yes' if self_found else 'NO'}, "
          f"latency={elapsed_ms:.2f}ms")

print(f"\n  Avg latency: {np.mean(latencies):.2f}ms")
print(f"  p50 latency: {np.median(latencies):.2f}ms")

# ── 9. Recall test ──────────────────────────────────────────────────────
# Brute-force compute true k-NN for the first query using numpy
print(f"\nRecall test (brute-force ground truth vs HNSW, query 0, k={K})...")

query_vec = all_vectors[0]
# Cosine similarity = dot(a,b) / (||a|| * ||b||)
norms = np.linalg.norm(all_vectors, axis=1)
query_norm = np.linalg.norm(query_vec)
cosine_sims = np.dot(all_vectors, query_vec) / (norms * query_norm)
true_neighbors = np.argsort(-cosine_sims)[:K]  # top-K by similarity

# Get HNSW results
hnsw_results = client.search(
    collection_name=COLLECTION,
    data=[query_vec.tolist()],
    anns_field="embedding",
    limit=K,
    output_fields=["doc_id"],
    search_params=search_params,
)
hnsw_ids = set(str(h["doc_id"]) for h in hnsw_results[0])
true_ids = set(str(n) for n in true_neighbors)

recall = len(hnsw_ids & true_ids) / len(true_ids)
print(f"  True top-{K}: {sorted(int(x) for x in true_ids)}")
print(f"  HNSW top-{K}: {sorted(int(x) for x in hnsw_ids)}")
print(f"  Recall@{K}: {recall:.2f}")

# ── 10. Cleanup ─────────────────────────────────────────────────────────
print(f"\nDropping collection '{COLLECTION}'...")
client.drop_collection(COLLECTION)
client.close()
print("Done!")
