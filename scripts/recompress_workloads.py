#!/usr/bin/env python3
"""
Recompress OSB workload data files into all supported OpenSearch codecs.

Downloads source from S3, decompresses once, then compresses into each unique
format in parallel and uploads results back to S3 under codec-named prefixes.

Codecs: default, best_compression, zstd, zstd_no_dict, qat_deflate, qat_lz4, qat_zstd

Deduplication:
  - default & qat_deflate produce identical output (gzip level 6) — compressed once
  - zstd, zstd_no_dict & qat_zstd produce identical output (zstd level 3) — compressed once
  - best_compression is unique (gzip level 9)
  - qat_lz4 is unique (lz4 frame)
  So only 4 actual compression jobs run, then files are copied/uploaded for all 7 codec names.

Usage:
    # S3 bucket name — script constructs keys from workload corpus layout
    python scripts/recompress_workloads.py --bucket osb-workload-data --workload pmc

    # Custom S3 prefix if your bucket layout differs
    python scripts/recompress_workloads.py --bucket osb-workload-data --s3-prefix corpora/pmc --workload pmc

    # Use local files (already downloaded)
    python scripts/recompress_workloads.py --local-dir /mnt/nvme/data --workload so

    # Dry run
    python scripts/recompress_workloads.py --bucket osb-workload-data --workload http_logs --dry-run

    # Only specific codecs
    python scripts/recompress_workloads.py --bucket osb-workload-data --workload pmc --codecs qat_lz4,qat_zstd

Recommended: run on m5d.8xlarge with NVMe as work-dir:
    sudo mkfs.ext4 /dev/nvme1n1 && sudo mount /dev/nvme1n1 /mnt/nvme
    pip install zstandard lz4 boto3
    python scripts/recompress_workloads.py --bucket my-bucket --workload pmc --work-dir /mnt/nvme

Requires: pip install zstandard lz4 boto3
"""

import argparse
import bz2
import gzip
import logging
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

try:
    import zstandard as zstd
except ImportError:
    sys.exit("Missing dependency: pip install zstandard")

try:
    import lz4.frame as lz4f
except ImportError:
    sys.exit("Missing dependency: pip install lz4")

try:
    import boto3
except ImportError:
    boto3 = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(process)d] %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CHUNK_SIZE = 4 * 1024 * 1024  # 4 MB chunks — good throughput on NVMe


# ============================================================
# Codec definitions with deduplication
# ============================================================

# "Compression groups" — codecs in the same group produce identical output.
# We compress once per group, then copy the result for each codec in the group.
COMPRESSION_GROUPS = {
    "deflate6": {
        "func": "compress_deflate",
        "ext": ".gz",
        "desc": "gzip/deflate level 6",
        "codecs": ["default", "qat_deflate"],
    },
    "deflate9": {
        "func": "compress_deflate9",
        "ext": ".gz",
        "desc": "gzip/deflate level 9",
        "codecs": ["best_compression"],
    },
    "zstd3": {
        "func": "compress_zstd",
        "ext": ".zst",
        "desc": "zstandard level 3",
        "codecs": ["zstd", "zstd_no_dict", "qat_zstd"],
    },
    "lz4": {
        "func": "compress_lz4",
        "ext": ".lz4",
        "desc": "lz4 frame format",
        "codecs": ["qat_lz4"],
    },
}

ALL_CODECS = []
for group in COMPRESSION_GROUPS.values():
    ALL_CODECS.extend(group["codecs"])

# Workload corpus paths in S3 (bz2/gz sources only — we produce the other codecs)
WORKLOAD_CORPORA = {
    "big5": {
        "prefix": "corpora/big5",
        "files": [
            "documents-60.json.bz2",
            "documents-100.json.bz2",
            "documents-880.json.bz2",
            "documents-1000.json.bz2",
            "documents-1000-part0.bz2",
            "documents-1000-part1.bz2",
            "documents-1000-part2.bz2",
        ],
    },
    "clickbench": {
        "prefix": "corpora/clickbench",
        "files": ["hits.json.gz"],
    },
    "eventdata": {
        "prefix": "corpora/eventdata",
        "files": ["eventdata.json.bz2"],
    },
    "geonames": {
        "prefix": "corpora/geonames",
        "files": ["documents-2.json.bz2"],
    },
    "geopoint": {
        "prefix": "corpora/geopoint",
        "files": ["documents.json.bz2"],
    },
    "geopointshape": {
        "prefix": "corpora/geopointshape",
        "files": ["documents.json.bz2"],
    },
    "geoshape": {
        "prefix": "corpora/geoshape",
        "files": [
            "linestrings.json.bz2",
            "multilinestrings.json.bz2",
            "polygons.json.bz2",
        ],
    },
    "http_logs": {
        "prefix": "corpora/http_logs",
        "files": [
            "documents-181998.json.bz2",
            "documents-191998.json.bz2",
            "documents-201998.json.bz2",
            "documents-211998.json.bz2",
            "documents-221998.json.bz2",
            "documents-231998.json.bz2",
            "documents-241998.json.bz2",
            "documents-181998.unparsed.json.bz2",
            "documents-191998.unparsed.json.bz2",
            "documents-201998.unparsed.json.bz2",
            "documents-211998.unparsed.json.bz2",
            "documents-221998.unparsed.json.bz2",
            "documents-231998.unparsed.json.bz2",
            "documents-241998.unparsed.json.bz2",
        ],
    },
    "nested": {
        "prefix": "corpora/nested",
        "files": ["documents.json.bz2"],
    },
    "noaa": {
        "prefix": "corpora/noaa",
        "files": ["documents.json.bz2"],
    },
    "nyc_taxis": {
        "prefix": "corpora/nyc_taxis",
        "files": ["documents.json.bz2"],
    },
    "percolator": {
        "prefix": "corpora/percolator",
        "files": ["queries-2.json.bz2"],
    },
    "pmc": {
        "prefix": "corpora/pmc",
        "files": ["documents.json.bz2"],
    },
    "so": {
        "prefix": "corpora/so",
        "files": ["posts.json.bz2"],
    },
}


# ============================================================
# Compression functions (run in child processes)
# ============================================================

def compress_deflate(src_path: str, dst_path: str, threads: int = 4):
    """gzip/deflate level 6."""
    src_path, dst_path = Path(src_path), Path(dst_path)
    if shutil.which("pigz"):
        with open(dst_path, "wb") as fout:
            subprocess.run(["pigz", "-6", f"-p{threads}", "-c", str(src_path)], stdout=fout, check=True)
    else:
        with open(src_path, "rb") as fin, gzip.open(dst_path, "wb", compresslevel=6) as fout:
            while chunk := fin.read(CHUNK_SIZE):
                fout.write(chunk)
    return _file_stats(src_path, dst_path)


def compress_deflate9(src_path: str, dst_path: str, threads: int = 4):
    """gzip/deflate level 9 (best_compression)."""
    src_path, dst_path = Path(src_path), Path(dst_path)
    if shutil.which("pigz"):
        with open(dst_path, "wb") as fout:
            subprocess.run(["pigz", "-9", f"-p{threads}", "-c", str(src_path)], stdout=fout, check=True)
    else:
        with open(src_path, "rb") as fin, gzip.open(dst_path, "wb", compresslevel=9) as fout:
            while chunk := fin.read(CHUNK_SIZE):
                fout.write(chunk)
    return _file_stats(src_path, dst_path)


def compress_zstd(src_path: str, dst_path: str, threads: int = 4):
    """Zstandard level 3."""
    src_path, dst_path = Path(src_path), Path(dst_path)
    cctx = zstd.ZstdCompressor(level=3, threads=threads)
    with open(src_path, "rb") as fin, open(dst_path, "wb") as fout:
        cctx.copy_stream(fin, fout, read_size=CHUNK_SIZE, write_size=CHUNK_SIZE)
    return _file_stats(src_path, dst_path)


def compress_lz4(src_path: str, dst_path: str, threads: int = 4):
    """LZ4 frame format."""
    src_path, dst_path = Path(src_path), Path(dst_path)
    with open(src_path, "rb") as fin, lz4f.open(dst_path, "wb") as fout:
        while chunk := fin.read(CHUNK_SIZE):
            fout.write(chunk)
    return _file_stats(src_path, dst_path)


def _file_stats(src_path: Path, dst_path: Path) -> dict:
    src_size = src_path.stat().st_size
    dst_size = dst_path.stat().st_size
    return {
        "uncompressed_bytes": src_size,
        "compressed_bytes": dst_size,
        "ratio": round(src_size / dst_size, 2) if dst_size > 0 else 0,
    }


def _compress_worker(group_name: str, func_name: str, src_path: str, dst_path: str, threads: int):
    """Run a single compression job in a child process."""
    func = globals()[func_name]
    t0 = time.time()
    stats = func(src_path, dst_path, threads)
    elapsed = time.time() - t0
    stats["group"] = group_name
    stats["elapsed_s"] = round(elapsed, 1)
    stats["dst_path"] = dst_path
    return stats


# ============================================================
# S3 helpers
# ============================================================

def download_from_s3(bucket: str, key: str, local_path: Path):
    if boto3 is None:
        sys.exit("Missing dependency for S3: pip install boto3")
    s3 = boto3.client("s3")
    logger.info("Downloading s3://%s/%s", bucket, key)
    s3.download_file(bucket, key, str(local_path))
    logger.info("Downloaded: %.2f GB", local_path.stat().st_size / 1e9)


def s3_key_exists(bucket: str, key: str) -> bool:
    """Check if an S3 key exists (for skip-existing logic)."""
    if boto3 is None:
        return False
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def upload_to_s3(local_path: Path, bucket: str, key: str):
    if boto3 is None:
        sys.exit("Missing dependency for S3: pip install boto3")
    s3 = boto3.client("s3")
    size_gb = local_path.stat().st_size / 1e9
    logger.info("Uploading %s (%.2f GB) -> s3://%s/%s", local_path.name, size_gb, bucket, key)
    # Use multipart for large files
    config = boto3.s3.transfer.TransferConfig(
        multipart_threshold=256 * 1024 * 1024,  # 256 MB
        max_concurrency=10,
        multipart_chunksize=256 * 1024 * 1024,
    )
    s3.upload_file(str(local_path), bucket, key, Config=config)
    logger.info("Uploaded: %s", key)


# ============================================================
# Decompress
# ============================================================

def decompress_file(src_path: Path, dst_path: Path):
    """Decompress bz2/gz/zst. Uses parallel CLI tools when available."""
    logger.info("Decompressing %s -> %s", src_path.name, dst_path.name)

    if src_path.suffix == ".bz2":
        if shutil.which("pbzip2"):
            logger.info("Using pbzip2 for parallel decompression")
            with open(dst_path, "wb") as fout:
                subprocess.run(["pbzip2", "-d", "-k", "-c", str(src_path)], stdout=fout, check=True)
        elif shutil.which("lbzip2"):
            logger.info("Using lbzip2 for parallel decompression")
            with open(dst_path, "wb") as fout:
                subprocess.run(["lbzip2", "-d", "-k", "-c", str(src_path)], stdout=fout, check=True)
        else:
            logger.warning("No parallel bz2 tool found — using Python bz2 (slow). Install: sudo yum install pbzip2")
            with bz2.open(src_path, "rb") as fin, open(dst_path, "wb") as fout:
                while chunk := fin.read(CHUNK_SIZE):
                    fout.write(chunk)

    elif src_path.suffix == ".gz":
        if shutil.which("pigz"):
            with open(dst_path, "wb") as fout:
                subprocess.run(["pigz", "-d", "-k", "-c", str(src_path)], stdout=fout, check=True)
        else:
            with gzip.open(src_path, "rb") as fin, open(dst_path, "wb") as fout:
                while chunk := fin.read(CHUNK_SIZE):
                    fout.write(chunk)

    elif src_path.suffix == ".zst":
        dctx = zstd.ZstdDecompressor()
        with open(src_path, "rb") as fin, open(dst_path, "wb") as fout:
            for chunk in dctx.read_to_iter(fin, read_size=CHUNK_SIZE):
                fout.write(chunk)
    else:
        raise ValueError(f"Unknown compression format: {src_path.suffix}")

    logger.info("Decompressed: %s (%.2f GB)", dst_path.name, dst_path.stat().st_size / 1e9)


# ============================================================
# Main processing
# ============================================================

def process_file(src_file: str, s3_prefix: str, work_dir: Path, bucket: str = None,
                 dry_run: bool = False, threads_per_job: int = 8,
                 selected_codecs: set = None, skip_existing: bool = True):
    """Process one source file: decompress once, compress into all codecs in parallel."""

    base_name = src_file
    for ext in (".bz2", ".gz", ".zst"):
        base_name = base_name.replace(ext, "")

    compressed_path = work_dir / src_file
    raw_path = work_dir / base_name

    # Step 1: Download
    if bucket and not compressed_path.exists():
        s3_key = f"{s3_prefix}/{src_file}"
        if dry_run:
            logger.info("[DRY RUN] Would download s3://%s/%s", bucket, s3_key)
        else:
            download_from_s3(bucket, s3_key, compressed_path)

    if not dry_run and not compressed_path.exists():
        logger.error("Source file not found: %s", compressed_path)
        return

    # Step 2: Decompress once
    if not dry_run and not raw_path.exists():
        decompress_file(compressed_path, raw_path)

    # Step 3: Figure out which compression groups we need to run
    # Filter groups to only those containing selected codecs
    groups_to_run = {}
    for group_name, group_info in COMPRESSION_GROUPS.items():
        relevant_codecs = [c for c in group_info["codecs"] if not selected_codecs or c in selected_codecs]
        if not relevant_codecs:
            continue

        # Check if output already exists in S3
        if skip_existing and bucket and not dry_run:
            sample_codec = relevant_codecs[0]
            out_name = base_name + group_info["ext"]
            s3_key = f"{s3_prefix}/{sample_codec}/{out_name}"
            if s3_key_exists(bucket, s3_key):
                logger.info("Skipping group %s — already exists in S3: %s", group_name, s3_key)
                continue

        out_path = work_dir / f"{base_name}.{group_name}{group_info['ext']}"
        groups_to_run[group_name] = {
            **group_info,
            "relevant_codecs": relevant_codecs,
            "out_path": out_path,
        }

    if dry_run:
        for group_name, info in groups_to_run.items():
            logger.info("[DRY RUN] %s (%s): compress once -> upload for codecs %s",
                        group_name, info["desc"], info["relevant_codecs"])
        return

    if not groups_to_run:
        logger.info("Nothing to do for %s (all outputs exist or no codecs selected)", base_name)
        return

    # Step 4: Run compression jobs in parallel (one per unique group)
    logger.info("Compressing %s into %d unique formats (threads_per_job=%d)", base_name, len(groups_to_run), threads_per_job)
    t0 = time.time()

    with ProcessPoolExecutor(max_workers=len(groups_to_run)) as pool:
        futures = {}
        for group_name, info in groups_to_run.items():
            if info["out_path"].exists():
                logger.info("Reusing existing %s", info["out_path"].name)
                continue
            f = pool.submit(
                _compress_worker,
                group_name, info["func"], str(raw_path), str(info["out_path"]), threads_per_job,
            )
            futures[f] = group_name

        results = {}
        for future in as_completed(futures):
            group_name = futures[future]
            try:
                stats = future.result()
                results[group_name] = stats
                logger.info(
                    "  %-12s  %.2f GB -> %.2f GB  (ratio %s:1, %ss)",
                    group_name, stats["uncompressed_bytes"] / 1e9,
                    stats["compressed_bytes"] / 1e9, stats["ratio"], stats["elapsed_s"],
                )
            except Exception as e:
                logger.error("Compression failed for %s: %s", group_name, e)

    elapsed = time.time() - t0
    logger.info("All compressions done for %s in %.1fs", base_name, elapsed)

    # Step 5: Upload — for each codec, upload the group's compressed file under that codec's prefix
    all_stats = []
    for group_name, info in groups_to_run.items():
        compressed_file = info["out_path"]
        if not compressed_file.exists():
            continue

        file_size = compressed_file.stat().st_size
        for codec_name in info["relevant_codecs"]:
            out_name = base_name + info["ext"]
            if bucket:
                s3_key = f"{s3_prefix}/{codec_name}/{out_name}"
                upload_to_s3(compressed_file, bucket, s3_key)

            all_stats.append({
                "codec": codec_name,
                "group": group_name,
                "compressed_bytes": file_size,
                "uncompressed_bytes": raw_path.stat().st_size,
                "file": out_name,
            })

    # Print summary
    logger.info("")
    logger.info("=== Summary for %s ===", base_name)
    logger.info("%-18s %-12s %15s %15s %8s", "Codec", "Group", "Compressed", "Uncompressed", "Ratio")
    logger.info("-" * 75)
    for s in sorted(all_stats, key=lambda x: x["compressed_bytes"]):
        ratio = s["uncompressed_bytes"] / s["compressed_bytes"] if s["compressed_bytes"] else 0
        logger.info("%-18s %-12s %12d B %12d B %7.1f:1",
                     s["codec"], s["group"], s["compressed_bytes"], s["uncompressed_bytes"], ratio)

    # Clean up decompressed file
    if raw_path.exists():
        logger.info("Removing decompressed file: %s (%.2f GB)", raw_path.name, raw_path.stat().st_size / 1e9)
        raw_path.unlink()

    # Clean up group compressed files
    for info in groups_to_run.values():
        if info["out_path"].exists():
            info["out_path"].unlink()


def main():
    parser = argparse.ArgumentParser(
        description="Recompress OSB workload data into all supported OpenSearch codecs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Codec -> compression mapping (with deduplication):
  default, qat_deflate          -> gzip level 6  (compressed once, uploaded twice)
  best_compression              -> gzip level 9
  zstd, zstd_no_dict, qat_zstd -> zstd level 3  (compressed once, uploaded thrice)
  qat_lz4                      -> lz4 frame

S3 upload layout:
  s3://<bucket>/<prefix>/<codec>/<filename>.<ext>

Example:
  s3://osb-data/corpora/pmc/default/documents.json.gz
  s3://osb-data/corpora/pmc/qat_zstd/documents.json.zst
  s3://osb-data/corpora/pmc/qat_lz4/documents.json.lz4
""",
    )
    parser.add_argument("--bucket", help="S3 bucket name")
    parser.add_argument("--s3-prefix", help="Override S3 prefix (default: from workload corpus config)")
    parser.add_argument("--workload", required=True, choices=sorted(WORKLOAD_CORPORA), help="Workload name")
    parser.add_argument("--work-dir", default="/mnt/nvme/osb-recompress", help="Working directory (use NVMe mount)")
    parser.add_argument("--local-dir", help="Use existing local files instead of downloading from S3")
    parser.add_argument("--threads-per-job", type=int, default=8,
                        help="Threads per compression job (default: 8, total = 4 jobs * 8 = 32 threads on m5d.8xlarge)")
    parser.add_argument("--codecs", help="Comma-separated subset of codecs (default: all 7)")
    parser.add_argument("--no-skip-existing", action="store_true", help="Re-upload even if S3 key exists")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    args = parser.parse_args()

    if not args.bucket and not args.local_dir:
        parser.error("Must specify --bucket or --local-dir")

    selected_codecs = None
    if args.codecs:
        selected_codecs = {c.strip() for c in args.codecs.split(",")}
        unknown = selected_codecs - set(ALL_CODECS)
        if unknown:
            parser.error(f"Unknown codecs: {unknown}. Choose from: {ALL_CODECS}")

    corpus = WORKLOAD_CORPORA[args.workload]
    s3_prefix = args.s3_prefix or corpus["prefix"]

    work_dir = Path(args.local_dir or args.work_dir)
    wl_dir = work_dir / args.workload
    wl_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=== Workload: %s ===", args.workload)
    logger.info("  Files: %d", len(corpus["files"]))
    logger.info("  S3 prefix: %s", s3_prefix)
    logger.info("  Work dir: %s", wl_dir)
    logger.info("  Codecs: %s", ", ".join(selected_codecs) if selected_codecs else "all 7")
    logger.info("  Threads per job: %d", args.threads_per_job)

    for src_file in corpus["files"]:
        process_file(
            src_file, s3_prefix, wl_dir,
            bucket=args.bucket, dry_run=args.dry_run,
            threads_per_job=args.threads_per_job,
            selected_codecs=selected_codecs,
            skip_existing=not args.no_skip_existing,
        )

    logger.info("All done.")


if __name__ == "__main__":
    main()
