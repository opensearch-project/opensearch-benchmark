#!/usr/bin/env python3
"""
Raw HTTP comparison — sustained load test across all 3 engines.

Single aiohttp client per engine, N concurrent workers, each sending
requests back-to-back for a fixed duration. Every round-trip is recorded.
QPS and latency percentiles computed from the full sample set after the run.

Run:
    python3.11 raw_http_comparison.py --mode all
    python3.11 raw_http_comparison.py --mode opensearch
    python3.11 raw_http_comparison.py --mode vespa
    python3.11 raw_http_comparison.py --mode milvus
    python3.11 raw_http_comparison.py --duration 60    # 1 minute instead of 5
    python3.11 raw_http_comparison.py --clients 1 8 32
"""

import argparse
import asyncio
import time
import numpy as np
import aiohttp

OPENSEARCH_URL = "http://10.0.137.227:9200"
VESPA_URL = "http://10.0.142.54:8080"
MILVUS_URL = "http://10.0.143.186:19530"

DIM = 768
K = 100
EF_SEARCH = 100
DEFAULT_DURATION = 300  # 5 minutes
DEFAULT_CLIENTS = [1, 8, 32]
WARMUP_SECONDS = 10
COOLDOWN = 15

# Pre-generate a pool of query vectors (reused in round-robin)
VECTOR_POOL_SIZE = 500
np.random.seed(42)
VECTOR_POOL = [np.random.randn(DIM).astype(np.float32).tolist()
               for _ in range(VECTOR_POOL_SIZE)]


def percentile(latencies, p):
    idx = int(len(latencies) * p / 100)
    return latencies[min(idx, len(latencies) - 1)]


def report(latencies, wall_time):
    latencies.sort()
    n = len(latencies)
    qps = n / wall_time
    print(f"      Total requests:  {n:,}")
    print(f"      Wall time:       {wall_time:.1f}s")
    print(f"      QPS:             {qps:,.0f}")
    print(f"      p50:             {percentile(latencies, 50):.2f}ms")
    print(f"      p90:             {percentile(latencies, 90):.2f}ms")
    print(f"      p99:             {percentile(latencies, 99):.2f}ms")
    print(f"      p99.9:           {percentile(latencies, 99.9):.2f}ms")
    print(f"      min:             {latencies[0]:.2f}ms")
    print(f"      max:             {latencies[-1]:.2f}ms")
    print(f"      mean:            {sum(latencies)/n:.2f}ms")
    errors = sum(1 for _ in [])  # placeholder
    return qps


def opensearch_body(qvec):
    return {
        "size": K,
        "query": {
            "knn": {
                "embedding": {
                    "vector": qvec,
                    "k": K
                }
            }
        },
        "_source": False,
        "docvalue_fields": ["_id"],
    }


def vespa_body(qvec):
    return {
        "yql": f"select documentid from target_index where {{targetHits:{K}}}nearestNeighbor(embedding, query_vector)",
        "ranking": "vector-similarity",
        "hits": K,
        "timeout": "10s",
        "input.query(query_vector)": qvec,
    }


def milvus_body(qvec):
    return {
        "collectionName": "target_index",
        "data": [qvec],
        "annsField": "embedding",
        "limit": K,
        "outputFields": ["doc_id"],
        "searchParams": {
            "params": {"ef": EF_SEARCH}
        }
    }


async def sustained_bench(name, url, make_body, num_clients, duration, warmup=WARMUP_SECONDS):
    """Run num_clients concurrent workers, each sending back-to-back requests."""
    total_duration = warmup + duration
    latencies = []
    errors = 0
    vec_idx = 0  # shared round-robin counter (approximate, no lock needed)

    async def worker(session, start_time):
        nonlocal vec_idx, errors
        local_latencies = []
        while True:
            elapsed = time.perf_counter() - start_time
            if elapsed >= total_duration:
                break

            # Round-robin through vector pool
            qvec = VECTOR_POOL[vec_idx % VECTOR_POOL_SIZE]
            vec_idx += 1
            body = make_body(qvec)

            t0 = time.perf_counter()
            try:
                async with session.post(url, json=body) as resp:
                    await resp.json()
                rtt = (time.perf_counter() - t0) * 1000

                # Only record after warmup
                if elapsed >= warmup:
                    local_latencies.append(rtt)
            except Exception:
                errors += 1

        return local_latencies

    connector = aiohttp.TCPConnector(limit=num_clients + 10, limit_per_host=num_clients + 10)
    async with aiohttp.ClientSession(connector=connector) as session:
        start = time.perf_counter()
        tasks = [worker(session, start) for _ in range(num_clients)]
        results = await asyncio.gather(*tasks)

    wall_time = time.perf_counter() - start - warmup
    for r in results:
        latencies.extend(r)

    print(f"    {num_clients:>2} client{'s' if num_clients > 1 else ' '}:")
    if errors:
        print(f"      Errors: {errors}")
    report(latencies, wall_time)
    print()
    return latencies, wall_time


async def run_engine(name, url, make_body, clients, duration):
    print(f"\n  {name}")
    print(f"  Duration: {duration}s per concurrency level, {WARMUP_SECONDS}s warmup")
    print(f"  {'─'*55}")

    for num_clients in clients:
        await sustained_bench(name, url, make_body, num_clients, duration)


async def main():
    parser = argparse.ArgumentParser(description="Sustained raw HTTP comparison across all 3 engines")
    parser.add_argument("--mode", choices=["opensearch", "vespa", "milvus", "all"], default="all")
    parser.add_argument("--duration", type=int, default=DEFAULT_DURATION,
                        help=f"Seconds per concurrency level (default: {DEFAULT_DURATION})")
    parser.add_argument("--clients", type=int, nargs="+", default=DEFAULT_CLIENTS,
                        help=f"Concurrency levels (default: {DEFAULT_CLIENTS})")
    args = parser.parse_args()

    print(f"Sustained Raw HTTP Comparison (aiohttp for all engines)")
    print(f"Duration: {args.duration}s per level, warmup: {WARMUP_SECONDS}s, cooldown: {COOLDOWN}s")
    print(f"Concurrency: {args.clients}")
    print(f"{'='*70}")

    if args.mode in ("opensearch", "all"):
        await run_engine(
            "OpenSearch (POST /_search)",
            f"{OPENSEARCH_URL}/vector_1m/_search",
            opensearch_body,
            args.clients, args.duration,
        )
    if args.mode == "all":
        print(f"\n  ... cooling down {COOLDOWN}s ...")
        await asyncio.sleep(COOLDOWN)

    if args.mode in ("vespa", "all"):
        await run_engine(
            "Vespa (POST /search/)",
            f"{VESPA_URL}/search/",
            vespa_body,
            args.clients, args.duration,
        )
    if args.mode == "all":
        print(f"\n  ... cooling down {COOLDOWN}s ...")
        await asyncio.sleep(COOLDOWN)

    if args.mode in ("milvus", "all"):
        await run_engine(
            "Milvus (POST /v2/vectordb/entities/search)",
            f"{MILVUS_URL}/v2/vectordb/entities/search",
            milvus_body,
            args.clients, args.duration,
        )

    print(f"\n{'='*70}")
    print("Done!")

asyncio.run(main())
