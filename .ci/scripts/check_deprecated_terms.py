#!/usr/bin/env python3

import os
import re
import sys

DEPRECATED_TERMS = [
    "provision-configs",
    "results-publisher",
    "load-worker-coordinator-hosts",
    "execute-test",
]

VARIANT_PATTERNS = []

# Generate regex variants (hyphen, underscore, camelCase, word-order naive permutations)
def generate_variants(term):
    base = term.replace("-", " ").replace("_", " ")
    words = base.split()
    variants = set()

    # kebab-case, snake_case, camelCase, PascalCase, permutations
    variants.add("-".join(words))
    variants.add("_".join(words))
    variants.add("".join([w.capitalize() for w in words]))        # PascalCase
    variants.add(words[0] + "".join([w.capitalize() for w in words[1:]]))  # camelCase

    # Word order permutations (naive)
    if len(words) == 2:
        variants.add("-".join(words[::-1]))
        variants.add("_".join(words[::-1]))
        variants.add(words[1] + words[0].capitalize())  # camelCase flip

    return variants

for term in DEPRECATED_TERMS:
    for variant in generate_variants(term):
        VARIANT_PATTERNS.append(re.compile(re.escape(variant), re.IGNORECASE))

def should_check_file(filename):
    return filename.endswith((".py", ".yml", ".yaml", ".md", ".sh", ".json", ".txt"))

def main():
    error_found = False

    for root, _, files in os.walk("."):
        if any(skip in root for skip in [".git", "venv", "__pycache__", ".pytest_cache"]):
            continue

        for f in files:
            full_path = os.path.join(root, f)
            if not should_check_file(full_path):
                continue

            try:
                with open(full_path, "r", encoding="utf-8") as file:
                    for i, line in enumerate(file, 1):
                        for pattern in VARIANT_PATTERNS:
                            if pattern.search(line):
                                print(f"[Deprecated Term] {full_path}:{i}: {line.strip()}")
                                error_found = True
            except Exception as e:
                print(f"[Warning] Skipped file {full_path}: {e}")

    if error_found:
        print("\n❌ Deprecated terms found. Please remove or rename them.")
        sys.exit(1)

    print("✅ No deprecated terms found.")
    sys.exit(0)

if __name__ == "__main__":
    main()
