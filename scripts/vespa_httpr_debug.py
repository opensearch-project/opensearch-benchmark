#!/usr/bin/env python3
"""
Reproduce httpr vs requests JSON POST difference with Vespa.

Shows that `requests.post(json=body)` works but pyvespa's
httpr-backed `sync.query(body=body)` fails for certain YQL.

Run: python3.11 vespa_httpr_debug.py
"""

import requests

VESPA_HOST = "http://10.0.142.54:8080"

queries = {
    "simple match-all": {
        "yql": "select * from big5 where true",
        "timeout": "10s",
    },
    "KNN vector search": {
        "yql": "select documentid from target_index where {targetHits:100}nearestNeighbor(embedding, query_vector)",
        "input.query(query_vector)": "[" + ",".join(["0.1"] * 10) + "]",
        "ranking": "vector-similarity",
        "timeout": "10s",
    },
    "grouping with pipe": {
        "yql": "select * from big5 where true | all(group(floor(timestamp / 86400000)) max(10) each(output(count())))",
        "timeout": "10s",
    },
    "date_histogram agg": {
        "yql": "select * from big5 where true | all(group(floor(timestamp / 3600000)) each(output(count())))",
        "timeout": "10s",
    },
}


def test_requests_lib():
    """Baseline: Python requests library (known working)."""
    print("=== requests.post(json=body) ===")
    for name, body in queries.items():
        try:
            r = requests.post(f"{VESPA_HOST}/search/", json=body, timeout=10)
            errors = r.json().get("root", {}).get("errors")
            if errors:
                print(f"  {name}: FAIL - {errors[0]['message'][:80]}")
            else:
                print(f"  {name}: OK ({r.status_code})")
        except Exception as e:
            print(f"  {name}: ERROR - {e}")


def test_pyvespa_sync():
    """pyvespa syncio (uses httpr Rust client internally)."""
    try:
        from vespa.application import Vespa
    except ImportError:
        print("=== pyvespa not installed, skipping ===")
        return

    print("\n=== pyvespa sync.query(body=body) ===")
    app = Vespa(url=VESPA_HOST)
    with app.syncio(compress=False) as sync:
        for name, body in queries.items():
            try:
                r = sync.query(body=body)
                print(f"  {name}: OK")
            except Exception as e:
                msg = str(e)[:120]
                print(f"  {name}: FAIL - {msg}")


def test_httpr_direct():
    """httpr.Client directly (bypass pyvespa wrapper)."""
    try:
        import httpr
    except ImportError:
        print("\n=== httpr not installed, skipping ===")
        return

    print("\n=== httpr.Client.post(json=body) ===")
    client = httpr.Client()
    for name, body in queries.items():
        try:
            r = client.post(f"{VESPA_HOST}/search/", json=body)
            data = r.json()
            errors = data.get("root", {}).get("errors")
            if errors:
                print(f"  {name}: FAIL - {errors[0]['message'][:80]}")
            else:
                print(f"  {name}: OK ({r.status_code})")
        except Exception as e:
            print(f"  {name}: ERROR - {e}")


if __name__ == "__main__":
    print(f"Vespa: {VESPA_HOST}")
    try:
        import httpr
        print(f"httpr version: {getattr(httpr, '__version__', 'unknown')}")
    except ImportError:
        pass
    try:
        import vespa
        print(f"pyvespa version: {getattr(vespa, '__version__', 'unknown')}")
    except ImportError:
        pass
    print()

    test_requests_lib()
    test_pyvespa_sync()
    test_httpr_direct()

    print("\n" + "=" * 60)
    print("If requests works but pyvespa/httpr fails, the issue is")
    print("in how httpr sends the JSON body to Vespa's /search/ API.")
