#!/usr/bin/env python3
import csv
import sys
from multiprocessing import Pool
import argparse
import re

csv.field_size_limit(sys.maxsize)

TARGET = "CONGRESSIONAL RECORD"
T_LEN = len(TARGET)


# ----------------------------------------------------------
# LEVENSHTEIN DISTANCE <= 1
# ----------------------------------------------------------
def levenshtein_leq1(a, b):
    """
    Returns True if the Levenshtein distance between a and b <= 1.
    """
    if a == b:
        return True

    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return False

    # Case: same length → up to 1 substitution allowed
    if la == lb:
        diff = sum(c1 != c2 for c1, c2 in zip(a, b))
        return diff <= 1

    # Case: length differs by 1 → insertion/deletion allowed
    if la > lb:
        a, b = b, a
        la, lb = lb, la

    # Now lb = la+1, check if removing one char from b equals a
    for i in range(lb):
        if b[:i] + b[i+1:] == a:
            return True

    return False


# ----------------------------------------------------------
# CHECK ENTIRE LINE FOR FUZZY TARGET ANYWHERE
# ----------------------------------------------------------
def line_contains_fuzzy_target(line):
    """
    Remove a line if ANY substring of length len(TARGET) or len(TARGET)±1
    matches the target with Levenshtein distance <= 1.
    """
    up = line.upper()

    # We'll test windows of length 21, 20, 22 (target len ± 1)
    for L in (T_LEN - 1, T_LEN, T_LEN + 1):
        if L <= 0 or L > len(up):
            continue

        for i in range(len(up) - L + 1):
            window = up[i:i+L]

            # Remove punctuation for comparison
            window_clean = re.sub(r"[^A-Z ]", "", window)

            if levenshtein_leq1(window_clean, TARGET):
                return True

    return False


# ----------------------------------------------------------
# CLEAN SPEECH TEXT
# ----------------------------------------------------------
def clean_speech_text(text):
    cleaned = []
    for line in text.splitlines():
        if line_contains_fuzzy_target(line):
            continue
        cleaned.append(line)
    return "\n".join(cleaned)


def worker(rows):
    cleaned_rows = []
    for row in rows:
        speech = row.get("speech", "")
        row["speech"] = clean_speech_text(speech)
        cleaned_rows.append(row)
    return cleaned_rows


# ----------------------------------------------------------
# MAIN
# ----------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="parsed_CRECB_speeches_with_debates_12-07.csv")
    parser.add_argument("--output", default="parsed_CRECB_speeches_with_debates_12-07_CLEANED.csv")
    parser.add_argument("--nprocs", type=int, default=50)
    args = parser.parse_args()

    print(f"Loading CSV: {args.input}")

    with open(args.input, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"Loaded {len(rows):,} rows")

    n = args.nprocs
    chunk_size = max(1, len(rows) // n)
    chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]

    print(f"Cleaning with {n} workers...")

    with Pool(processes=n) as pool:
        cleaned_chunks = pool.map(worker, chunks)

    cleaned_rows = [r for c in cleaned_chunks for r in c]

    print(f"Writing cleaned CSV → {args.output}")

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cleaned_rows[0].keys())
        writer.writeheader()
        writer.writerows(cleaned_rows)

    print("✅ DONE — fuzzy 'CONGRESSIONAL RECORD' lines removed.")


if __name__ == "__main__":
    main()
