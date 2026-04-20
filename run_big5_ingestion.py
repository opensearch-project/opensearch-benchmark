#!/usr/bin/env python3
"""Run opensearch-benchmark big5 ingestion 105 times with incrementing index names."""

import json
import logging
import subprocess
import sys
from datetime import datetime, timezone

NUM_INDICES = 105
LOG_FILE = "big5_ingestion.log"

def setup_logging():
    logger = logging.getLogger("big5_ingestion")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    fh = logging.FileHandler(LOG_FILE)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger

def main():
    log = setup_logging()
    log.info("Starting big5 ingestion: %d runs", NUM_INDICES)

    for i in range(NUM_INDICES):
        index_name = f"big5_{i:03d}"
        log.info("=" * 60)
        log.info("Starting run %d/%d — index: %s", i + 1, NUM_INDICES, index_name)
        start = datetime.now(timezone.utc)

        workload_params = json.dumps({
            "distribution_version": "3.3.0",
            "bulk_indexing_clients": 50,
            "bulk_size": 5000,
            "corpus_size": 1000,
            "index_name": index_name,
        })

        cmd = [
            "opensearch-benchmark", "execute-test",
            "--pipeline=benchmark-only",
            "--target-hosts=76vjv0flzs32x660t0a3.beta-us-east-1.aoss.amazonaws.com:443",
            "--client-options=timeout:300,amazon_aws_log_in:session,region:us-east-1,service:aoss",
            "--workload-path=/home/ec2-user/opensearch-benchmark-workloads/big5",
            f"--workload-params={workload_params}",
            "--exclude-tasks=type:search",
            "--kill-running-processes",
        ]

        result = subprocess.run(cmd)
        elapsed = datetime.now(timezone.utc) - start

        if result.returncode != 0:
            log.error("FAILED run %d/%d (index=%s) exit code %d after %s",
                      i + 1, NUM_INDICES, index_name, result.returncode, elapsed)
            sys.exit(1)

        log.info("Completed %s (%d/%d) in %s", index_name, i + 1, NUM_INDICES, elapsed)

    log.info("All %d ingestion runs complete.", NUM_INDICES)

if __name__ == "__main__":
    main()
