#!/usr/bin/env python3
"""
Vespa recall debugging tool.

Loads the cohere-1m HDF5 ground truth and runs a small number of queries
against Vespa using different rank expressions and configurations, then
computes recall@k and recall@1 against the ground truth.

Purpose: isolate WHY Vespa's OSB benchmark recall is 0.69 vs Milvus 0.97.
Candidate causes:
  1. Rank expression (closeness() vs dotproduct() vs dotproduct_dense())
  2. Doc ID mismatch (Vespa-assigned ID != HDF5 row index)
  3. ef_search too low (not enough exploration)
  4. Wrong distance metric in schema vs query intent
  5. Ground truth file uses a different metric than we're querying with

Usage:
    python3.11 scripts/vespa_recall_debug.py --host 10.0.142.54:8080 \\
        --hdf5 /home/ec2-user/.benchmark/data/vectorsearch/cohere-1m/cohere-1m.hdf5 \\
        --num-queries 100

The HDF5 file is auto-downloaded by OSB on the first workload run, usually to
~/.benchmark/data/vectorsearch/<corpus>/<corpus>.hdf5
"""

import argparse
import asyncio
import json
import logging

import aiohttp
import h5py
import numpy as np

logging.getLogger("httpr").setLevel(logging.WARNING)


# Candidate rank expressions to test. Each is a dict with:
#   name:        short label
#   yql:         YQL template (uses {k}, {explore}, {field})
#   ranking:     ranking profile name (must exist in schema; leave None to
#                skip setting it and use the schema default)
#   description: short explanation
RANK_EXPRESSIONS = [
    {
        "name": "ef_search=256 (current default)",
        "yql": "select documentid from target_index where "
               "{{targetHits:{k},approximate:true,hnsw.exploreAdditionalHits:156}}"
               "nearestNeighbor({field}, query_vector)",
        "ranking": "vector-similarity",
        "description": "OSB's current setup — targetHits=100 + exploreAdditionalHits=156",
    },
    {
        "name": "ef_search=512",
        "yql": "select documentid from target_index where "
               "{{targetHits:{k},approximate:true,hnsw.exploreAdditionalHits:412}}"
               "nearestNeighbor({field}, query_vector)",
        "ranking": "vector-similarity",
        "description": "targetHits=100 + exploreAdditionalHits=412 → effective ef_search=512",
    },
    {
        "name": "ef_search=1024",
        "yql": "select documentid from target_index where "
               "{{targetHits:{k},approximate:true,hnsw.exploreAdditionalHits:924}}"
               "nearestNeighbor({field}, query_vector)",
        "ranking": "vector-similarity",
        "description": "targetHits=100 + exploreAdditionalHits=924 → effective ef_search=1024",
    },
    {
        "name": "ef_search=2048",
        "yql": "select documentid from target_index where "
               "{{targetHits:{k},approximate:true,hnsw.exploreAdditionalHits:1948}}"
               "nearestNeighbor({field}, query_vector)",
        "ranking": "vector-similarity",
        "description": "targetHits=100 + exploreAdditionalHits=1948 → effective ef_search=2048",
    },
    {
        "name": "ef_search=4096",
        "yql": "select documentid from target_index where "
               "{{targetHits:{k},approximate:true,hnsw.exploreAdditionalHits:3996}}"
               "nearestNeighbor({field}, query_vector)",
        "ranking": "vector-similarity",
        "description": "targetHits=100 + exploreAdditionalHits=3996 → effective ef_search=4096",
    },
    {
        "name": "exact (no ANN)",
        "yql": "select documentid from target_index where "
               "{{targetHits:{k},approximate:false}}"
               "nearestNeighbor({field}, query_vector)",
        "ranking": "vector-similarity",
        "description": "Bypass HNSW entirely — brute force exact search. "
                       "If this recall is still ~0.69 then it's NOT an HNSW/ef issue",
    },
]


def extract_doc_id(vespa_id):
    """Vespa IDs look like 'id:namespace:doctype::123' or just '123'."""
    if isinstance(vespa_id, str) and "::" in vespa_id:
        return vespa_id.rsplit("::", 1)[-1]
    return str(vespa_id)


def recall_at(candidates, truth, k):
    """recall@k: fraction of top-k ground truth neighbors that are in the top-k candidates."""
    if k == 0:
        return 0.0
    truth_set = set(str(t) for t in truth[:k] if str(t) != "-1")
    if not truth_set:
        return 1.0
    cand_set = set(str(c) for c in candidates[:k])
    return len(truth_set & cand_set) / len(truth_set)


async def query_vespa(session, host, yql, ranking, query_vector, k):
    body = {
        "yql": yql,
        "hits": k,
        "timeout": "10s",
        "input.query(query_vector)": query_vector,
    }
    if ranking:
        body["ranking"] = ranking
    async with session.post(f"http://{host}/search/", json=body) as resp:
        data = await resp.json()

    if "root" not in data or "children" not in data.get("root", {}):
        return [], data
    hits = data["root"]["children"]
    # Vespa hits have 'id' like "id:namespace:target_index::42"
    ids = [extract_doc_id(h.get("id") or h.get("fields", {}).get("documentid", "")) for h in hits]
    return ids, data


async def run_expression(session, host, expr, query_vectors, true_neighbors, field, k):
    label = expr["name"]
    print(f"\n--- {label} ---")
    print(f"    {expr['description']}")

    yql = expr["yql"].format(k=k, explore=max(k, 256) - k, field=field)
    print(f"    yql: {yql}")
    print(f"    ranking: {expr['ranking']}")

    per_query_recall_k = []
    per_query_recall_1 = []
    num_hits_per_query = []
    first_sample_printed = False

    for i, qvec in enumerate(query_vectors):
        candidates, raw = await query_vespa(session, host, yql, expr["ranking"], qvec.tolist(), k)
        num_hits_per_query.append(len(candidates))
        per_query_recall_k.append(recall_at(candidates, true_neighbors[i], k))
        per_query_recall_1.append(recall_at(candidates, true_neighbors[i], 1))
        if not first_sample_printed and len(candidates) > 0:
            first_sample_printed = True
            print(f"    sample query 0: {len(candidates)} hits, "
                  f"top-5 candidates: {candidates[:5]}, "
                  f"top-5 ground truth: {[str(x) for x in true_neighbors[0][:5]]}")
        elif not first_sample_printed and i == 0:
            print(f"    sample query 0: 0 hits — RAW RESPONSE: {json.dumps(raw)[:500]}")

    mean_hits = np.mean(num_hits_per_query)
    mean_recall_k = np.mean(per_query_recall_k)
    mean_recall_1 = np.mean(per_query_recall_1)
    print(f"    mean hits returned: {mean_hits:.1f} / {k}")
    print(f"    recall@k: {mean_recall_k:.3f}")
    print(f"    recall@1: {mean_recall_1:.3f}")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="10.0.142.54:8080")
    parser.add_argument("--hdf5", required=True, help="Path to cohere-1m.hdf5 with 'test' and 'neighbors' datasets")
    parser.add_argument("--field", default="embedding", help="Vespa vector field name")
    parser.add_argument("--num-queries", type=int, default=100)
    parser.add_argument("--k", type=int, default=100)
    args = parser.parse_args()

    print(f"Vespa recall debug")
    print(f"Host: {args.host}")
    print(f"HDF5: {args.hdf5}")
    print(f"field: {args.field}   k: {args.k}   num_queries: {args.num_queries}")
    print("=" * 70)

    with h5py.File(args.hdf5, "r") as f:
        print(f"HDF5 datasets: {list(f.keys())}")
        # Vectorsearch workload convention: 'test' holds query vectors,
        # 'neighbors' holds ground truth neighbor indices (shape: [num_queries, k])
        query_vectors = np.asarray(f["test"][: args.num_queries], dtype=np.float32)
        true_neighbors = np.asarray(f["neighbors"][: args.num_queries])
        print(f"query shape: {query_vectors.shape}")
        print(f"neighbors shape: {true_neighbors.shape}")
        if "train" in f:
            print(f"train shape: {f['train'].shape}  (base vectors — should match what was ingested)")
        print(f"first 5 ground truth neighbors of query 0: {list(true_neighbors[0][:5])}")

    connector = aiohttp.TCPConnector(limit=8)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Sanity check: can we reach Vespa at all?
        try:
            async with session.get(f"http://{args.host}/ApplicationStatus") as resp:
                status = await resp.json()
                print(f"Vespa reachable. Application version: "
                      f"{status.get('application', {}).get('vespa', {}).get('version', 'unknown')}")
        except Exception as e:
            print(f"WARNING: couldn't fetch ApplicationStatus: {e}")

        for expr in RANK_EXPRESSIONS:
            try:
                await run_expression(
                    session, args.host, expr, query_vectors, true_neighbors, args.field, args.k,
                )
            except Exception as e:
                print(f"    FAILED: {e}")

    print("\n" + "=" * 70)
    print("Done. Interpretation guide:")
    print("  - If ALL expressions give ~0.69 recall, the problem isn't rank expression.")
    print("    Next suspects: doc ID mismatch (compare candidate IDs to ground truth "
          "indices — do they look like valid row indices 0-999999?) or the HDF5 "
          "ground truth was computed with a different metric than the schema.")
    print("  - If 'exact (no ANN)' gives ~1.0 but the HNSW ones are lower, ef_search "
          "is insufficient for this index density — bump further.")
    print("  - If 'exact (no ANN)' still gives ~0.69, ingest or metric mismatch.")
    print("  - If 'higher ef_search (1024)' beats 256, we're just under-exploring.")


asyncio.run(main())
