#!/usr/bin/env python3
import csv
import re
import sys
from pathlib import Path
from difflib import SequenceMatcher
import argparse
from multiprocessing import Pool, cpu_count
import tempfile
import os
import json

csv.field_size_limit(sys.maxsize)

INPUT_CSV = "parsed_CRECB_speeches_12-07.csv"
OUTPUT_CSV = "parsed_CRECB_speeches_with_debates_12-07.csv"

# ------------------------------------------------------------
#   TITLE + OCR CLEANING FUNCTIONS
# ------------------------------------------------------------

def normalize_ocr(line):
    # Remove zero-width spaces, soft hyphens, NB hyphens
    line = re.sub(r"[\u200B\u200C\u200D\u00AD\u2011]", "", line)
    
    # Normalize EM/EN dashes to simple ascii hyphen
    line = line.replace("—", "-").replace("–", "-")

    return line

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

def is_parentheses_line(line):
    s = line.strip()
    return s.startswith("(") and s.endswith(")")

def fuzzy_match(a, b):
    return SequenceMatcher(None, a, b).ratio()

def remove_headers_with_fuzz(text):
    lines = text.splitlines()
    cleaned = []
    skip_next = False

    for line in lines:
        line = normalize_ocr(line)
        up = line.strip().upper()

        stripped = line.strip()
        if re.fullmatch(r"\d{4}", stripped):
            yr = int(stripped)
            if 1873 <= yr <= 1994:
                continue

        if "CONGRESSIONAL RECORD" in up:
            continue

        if fuzzy_match(up, "HOUSE OF REPRESENTATIVES") >= 0.90:
            skip_next = True
            continue

        if fuzzy_match(up, "SENATE") >= 0.83:
            skip_next = True
            continue

        if fuzzy_match(up, "CONGRESSIONAL RECORD") >= 0.90:
            continue

        words = re.findall(r"[A-Za-z]+", up)
        key = {"AUTHENTICATED", "AUTHENTICATE", "AUTHENTICATION",
               "AUTHENTIC", "INFORMATION", "GPO"}
        if words and all(w in key for w in words):
            continue

        if skip_next:
            skip_next = False
            alpha = re.findall(r"[A-Za-z]+", line)
            if alpha:
                w = alpha[0]
                if (w.isupper() and len(w) >= 3) or \
                   (len(w) >= 6 and sum(c.islower() for c in w) <= 1):
                    continue

        cleaned.append(line)

    return "\n".join(cleaned)

# ------------------------------------------------------------
# TITLE DETECTION
# ------------------------------------------------------------

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
        next_line = lines[i+1] if i + 1 < len(lines) else ""

        if stripped == "" and block:
            j = i+1
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
        sec_end = titles[i+1][1] if i < len(titles)-1 else len(text)
        sections.append((t.strip(), sec_start, sec_end))
    return sections

# ------------------------------------------------------------
# WORKER FUNCTION (parallel)
# ------------------------------------------------------------

def worker_process(args):
    filename, speech_rows = args

    reconstructed = ""
    offsets = []
    for idx, row in enumerate(speech_rows):
        offsets.append((idx, len(reconstructed)))
        reconstructed += row["speech"] + "\n\n"

    cleaned_text = remove_headers_with_fuzz(reconstructed)
    titles = find_titles(cleaned_text)
    sections = extract_sections(cleaned_text, titles)

    title_texts = {t.strip(): t for (t, _, _) in titles}

    # ------------------------------------------------------------
    # UPDATED remove_assigned_title (ONLY CHANGE YOU REQUESTED)
    # ------------------------------------------------------------
    def normalize_for_compare(s):
        s = normalize_ocr(s)
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

    temp_path = tempfile.mktemp(prefix="debate_", suffix=".csv")

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
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_csv", default=INPUT_CSV)
    parser.add_argument("--output_csv", default=OUTPUT_CSV)
    parser.add_argument("--nprocs", type=int, default=50)
    args = parser.parse_args()

    input_csv = args.input_csv
    output_csv = args.output_csv
    nprocs = args.nprocs

    print(f"Parallel debate extraction using {nprocs} workers")

    buffer = {}
    last_filename = None
    task_list = []

    with open(input_csv, encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            fn = row["filename"]

            if fn not in buffer:
                buffer[fn] = []
            buffer[fn].append(row)

            if last_filename is not None and fn != last_filename:
                task_list.append((last_filename, buffer[last_filename]))
                del buffer[last_filename]

            last_filename = fn

    if last_filename in buffer:
        task_list.append((last_filename, buffer[last_filename]))

    with Pool(processes=nprocs) as pool:
        temp_files = pool.map(worker_process, task_list)

    print("Merging worker outputs...")
    first = True

    with open(output_csv, "w", newline="", encoding="utf-8") as out:
        merged = None
        for temp in temp_files:
            with open(temp, encoding="utf-8") as f:
                reader = csv.reader(f)
                if first:
                    for row in reader:
                        writer = csv.writer(out)
                        writer.writerow(row)
                    first = False
                else:
                    next(reader)
                    writer = csv.writer(out)
                    for row in reader:
                        writer.writerow(row)
            os.remove(temp)

    print("✅ DONE — Parallel debate assignment finished.")

if __name__ == "__main__":
    main()
