#!/usr/bin/env python3
# .ci/scripts/check_deprecated_terms.py

import os, re, sys, argparse

# ---- Term sets ----
# Block these when you're on the *2.x* branch (i.e., forbid legacy 1.x names):
TERMS_1X = [
    "provision-configs",
    "provision-config-instances",
    "results-publishing",
    "results-publisher",
    "load-worker-coordinator-hosts",
    "execute-test",
]

# Block these when you're on the *1.x* branch (i.e., forbid 2.x names):
TERMS_2X = [
    "cluster-configs",
    "reporting",
    "worker-hosts",
    "run-test",
    "test-run",
]

SKIP_DIRS = {".git", "venv", "__pycache__", ".pytest_cache", ".ci", "tests"}
VALID_EXTENSIONS = (".py", ".yml", ".yaml", ".md", ".sh", ".json", ".txt")

def generate_variants(term: str) -> set[str]:
    base = term.replace("-", " ").replace("_", " ")
    words = base.split()
    variants = set()
    # kebab, snake, Pascal, camel
    variants.add("-".join(words))
    variants.add("_".join(words))
    variants.add("".join([w.capitalize() for w in words]))  # PascalCase
    variants.add(words[0] + "".join([w.capitalize() for w in words[1:]]))  # camelCase

    # Optional: flip order for 2-word terms, but avoid silly "-ip" flips creating noise
    if len(words) == 2 and not words[1].lower() == "ip":
        variants.add("-".join(words[::-1]))
        variants.add("_".join(words[::-1]))
        variants.add(words[1] + words[0].capitalize())  # camelCase reverse
    return variants

def build_patterns(terms: list[str]) -> list[re.Pattern]:
    pats = []
    for t in terms:
        for v in generate_variants(t):
            pats.append(re.compile(re.escape(v), re.IGNORECASE))
    return pats

def should_check_file(path: str) -> bool:
    return path.endswith(VALID_EXTENSIONS)

def walk_and_check(patterns: list[re.Pattern]) -> int:
    error_found = 0
    for root, _, files in os.walk("."):
        if any(skip in root.split(os.sep) for skip in SKIP_DIRS):
            continue
        for f in files:
            full_path = os.path.join(root, f)
            if not should_check_file(full_path):
                continue
            try:
                with open(full_path, "r", encoding="utf-8") as fh:
                    for i, line in enumerate(fh, 1):
                        for patt in patterns:
                            if patt.search(line):
                                print(f"[Forbidden Term] {full_path}:{i}: {line.strip()}")
                                error_found = 1
                                break
            except Exception as e:
                print(f"[Warning] Skipped file {full_path}: {e}")
    return error_found

def main():
    p = argparse.ArgumentParser(description="Check forbidden term set by mode or env.")
    p.add_argument("--mode", choices=["block-1x", "block-2x"], default=os.getenv("OSB_TERM_MODE"))
    args = p.parse_args()

    mode = args.mode
    if not mode:
        print("No mode provided (use --mode block-1x | block-2x or set OSB_TERM_MODE). Exiting 0.")
        sys.exit(0)

    if mode == "block-1x":
        terms = TERMS_1X
        banner = "❌ 1.x terms found in 2.x branch. Replace with 2.x names."
    else:
        terms = TERMS_2X
        banner = "❌ 2.x terms found in 1.x branch. Replace with 1.x names."

    patterns = build_patterns(terms)
    failed = walk_and_check(patterns)
    if failed:
        print("\n" + banner)
        sys.exit(1)
    print("✅ No forbidden terms found for", mode)
    sys.exit(0)

if __name__ == "__main__":
    main()
