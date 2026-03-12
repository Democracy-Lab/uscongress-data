#!/usr/bin/env python3
import csv
import re
import sys
from difflib import SequenceMatcher
import argparse
from multiprocessing import Pool, cpu_count
import tempfile
import os

csv.field_size_limit(sys.maxsize)

# ------------------------------------------------------------
# DEFAULTS (now purely CLI defaults)
# ------------------------------------------------------------
INPUT_CSV = "parsed_CRECB_speeches.csv"
OUTPUT_CSV = "parsed_CRECB_speeches_with_debates.csv"

# ------------------------------------------------------------
# TITLE DETECTION (NO OCR CLEANING HERE — done in Step 0)
# ------------------------------------------------------------

def normalize_token(token):
    return token.strip(",.;:!?—–-()[]{}<>\"'`")

def is_allcaps_alpha_word(token):
    cleaned = normalize_token(token)
    if len(re.findall(r"[A-Za-z]", cleaned)) < 2:
        return False
    if re.search(r"[a-z]", cleaned):
        return False
    return bool(re.search(r"[A-Z]", cleaned))

def percent_uppercase_alpha(line, skip_words=None):
    if skip_words is None:
        skip_words = set()
    letters, upper = [], []
    for token in line.split():
        base = re.sub(r"[^A-Za-z]", "", token)
        if base.lower() in skip_words:
            continue
        for ch in base:
            letters.append(ch)
            if ch.isupper():
                upper.append(ch)
    if not letters:
        return 0.0
    return len(upper) / len(letters)

def exclude_line(line):
    l = line.lower()
    cleaned = re.sub(r"[^a-z]", "", l)
    if "congressionalrecord" in cleaned:
        return True
    if cleaned in ("usgovernment", "usgoverment"):
        return True
    words = re.findall(r"[A-Za-z]+", l)
    key = {"gpo", "authenticated", "authentication", "authentic", "information"}
    if words and all(w in key for w in words):
        return True
    return False

def fuzzy_match(a, b):
    return SequenceMatcher(None, a, b).ratio()

def find_titles(text):
    titles = []
    lines = text.splitlines(keepends=True)
    offset = 0
    block, block_start = [], None
    skip_words = {"report"}

    for i, line in enumerate(lines):
        stripped = line.strip()
        line_start = offset
        line_end = offset + len(line)
        offset += len(line)

        if stripped == "" and block:
            j = i + 1
            while j < len(lines) and lines[j].strip() == "":
                j += 1
            if j < len(lines):
                look = lines[j].strip()
                pct = percent_uppercase_alpha(look, skip_words)
                alpha_chars = len(re.findall(r"[A-Za-z]", look))
                if alpha_chars >= 8 and pct >= 0.60:
                    block.append((line, line_start, line_end))
                    continue

            block_text = "".join(x[0] for x in block).lstrip()
            if block_text.startswith(("Mr.", "Ms.", "Mrs.")):
                block = []
                continue

            titles.append(("".join(x[0] for x in block), block_start, block[-1][2]))
            block = []
            continue

        if exclude_line(line):
            if block:
                block_text = "".join(x[0] for x in block).lstrip()
                if not block_text.startswith(("Mr.", "Ms.", "Mrs.")):
                    titles.append(("".join(x[0] for x in block), block_start, block[-1][2]))
            block = []
            continue

        pct = percent_uppercase_alpha(stripped, skip_words)
        cap_words = len([t for t in stripped.split() if is_allcaps_alpha_word(t)])
        alpha_words = len([w for w in stripped.split() if re.search(r"[A-Za-z]", w)])
        alpha_chars_total = len(re.findall(r"[A-Za-z]", stripped))

        is_strong = (alpha_words >= 2 and pct >= 0.60 and alpha_chars_total >= 8)
        is_short_final = (alpha_words == 1 and cap_words >= 1)

        if is_strong:
            if not block:
                block_start = line_start
            block.append((line, line_start, line_end))
        elif block and is_short_final:
            block.append((line, line_start, line_end))
        else:
            if block:
                block_text = "".join(x[0] for x in block).lstrip()
                if not block_text.startswith(("Mr.", "Ms.", "Mrs.")):
                    titles.append(("".join(x[0] for x in block), block_start, block[-1][2]))
            block = []

    if block:
        block_text = "".join(x[0] for x in block).lstrip()
        if not block_text.startswith(("Mr.", "Ms.", "Mrs.")):
            titles.append(("".join(x[0] for x in block), block_start, block[-1][2]))

    return titles

def extract_sections(text, titles):
    sections = []
    for i, (t, s, e) in enumerate(titles):
        sec_start = e
        sec_end = titles[i + 1][1] if i < len(titles) - 1 else len(text)
        sections.append((t.strip(), sec_start, sec_end))
    return sections

# ------------------------------------------------------------
# PER-SPEECH TITLE REMOVAL (NO OCR NORMALIZATION HERE)
# ------------------------------------------------------------

def normalize_for_compare(s):
    s = re.sub(r"\s+", " ", s)
    return s.strip().upper()

def remove_assigned_title(text, assigned_title):
    if not assigned_title:
        return text

    norm_title = normalize_for_compare(assigned_title)

    new_lines = []
    for line in text.splitlines():
        if normalize_for_compare(line).startswith(norm_title):
            continue
        new_lines.append(line)

    return "\n".join(new_lines)

# ------------------------------------------------------------
# WORKER FUNCTION (parallel)
# ------------------------------------------------------------

def worker_process(args):
    filename, speech_rows, temp_dir = args

    reconstructed = ""
    offsets = []
    for idx, row in enumerate(speech_rows):
        offsets.append((idx, len(reconstructed)))
        reconstructed += row["speech"] + "\n\n"

    # IMPORTANT: No header/boilerplate cleaning here (done in Step 0)
    titles = find_titles(reconstructed)
    sections = extract_sections(reconstructed, titles)

    temp_path = os.path.join(temp_dir, f"debate_{os.getpid()}_{abs(hash(filename))}.csv")

    with open(temp_path, "w", newline="", encoding="utf-8") as out:
        fieldnames = list(speech_rows[0].keys()) + ["title"]
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()

        for idx, pos in offsets:
            assigned = ""
            for t, s_start, s_end in sections:
                if s_start <= pos < s_end:
                    assigned = t.strip()
                    break

            cleaned_speech = remove_assigned_title(speech_rows[idx]["speech"], assigned)
            row = speech_rows[idx]
            row["speech"] = cleaned_speech
            row["title"] = assigned
            writer.writerow(row)

    return temp_path

# ------------------------------------------------------------
# MAIN STREAMING LOGIC (parallel)
# ------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Assign debate titles to parsed CRECB speeches")
    parser.add_argument("--input_csv", default=INPUT_CSV, help="CSV from parse_speeches.py")
    parser.add_argument("--output_csv", default=OUTPUT_CSV, help="Output CSV with added 'title' column")
    parser.add_argument("--nprocs", type=int, default=min(cpu_count(), 50))
    parser.add_argument("--temp_dir", default=".", help="Where to write temporary worker CSVs")
    args = parser.parse_args()

    input_csv = args.input_csv
    output_csv = args.output_csv
    nprocs = max(1, min(args.nprocs, cpu_count(), 50))
    temp_dir = args.temp_dir
    os.makedirs(temp_dir, exist_ok=True)

    print(f"Parallel debate extraction using {nprocs} workers")
    print(f"Input:  {input_csv}")
    print(f"Output: {output_csv}")
    print(f"Temp:   {temp_dir}")

    buffer = {}
    task_list = []

    # Deterministic processing depends on deterministic row order by filename:
    # We enforce this by sorting all rows by (filename, original_row_index).
    with open(input_csv, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Preserve within-filename order deterministically using the read order
    for i, r in enumerate(rows):
        r["_row_i"] = i

    rows.sort(key=lambda r: (r.get("filename", ""), r["_row_i"]))

    # Build per-filename tasks (now deterministic regardless of CSV ordering)
    for row in rows:
        fn = row["filename"]
        row.pop("_row_i", None)
        buffer.setdefault(fn, []).append(row)

    for fn in sorted(buffer.keys()):
        task_list.append((fn, buffer[fn], temp_dir))

    with Pool(processes=nprocs) as pool:
        temp_files = pool.map(worker_process, task_list)

    print("Merging worker outputs...")

    first = True
    with open(output_csv, "w", newline="", encoding="utf-8") as out:
        for temp in temp_files:
            with open(temp, encoding="utf-8") as f:
                reader = csv.reader(f)
                if first:
                    for row in reader:
                        writer = csv.writer(out)
                        writer.writerow(row)
                    first = False
                else:
                    next(reader, None)
                    writer = csv.writer(out)
                    for row in reader:
                        writer.writerow(row)
            os.remove(temp)

    print("✅ DONE — Parallel debate assignment finished.")

if __name__ == "__main__":
    main()
