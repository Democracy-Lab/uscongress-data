#!/usr/bin/env python3
"""
parse_speeches.py

Parse speaker-labeled speeches from (optionally pre-cleaned) CRECB OCR .txt files.

Designed to follow your Step 0 preprocessing script:
- Step 0 writes a cleaned, mirrored directory tree of .txt files to an output_dir
- This script takes that directory as --input_dir

Usage (example):
  python parse_speeches.py \
    --input_dir /path/to/tesseract_CRECB_OCR_PRECLEAN \
    --output_csv parsed_CRECB_speeches.csv \
    --nprocs 50 \
    --scraper_path /local/scratch/group/guldigroup/climate_change/congress/congressional_scraper
"""

import os
import re
import csv
import sys
import argparse
from datetime import datetime
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import List, Tuple

csv.field_size_limit(sys.maxsize)

# ------------------------------------------------------------
# Regex to extract date from filename (UNCHANGED)
# ------------------------------------------------------------
DATE_REGEX = re.compile(r"([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})")

def extract_date_from_filename(filename: str) -> Tuple[str, str]:
    m = DATE_REGEX.search(filename)
    if not m:
        raise ValueError(f"Could not extract date from filename: {filename}")

    month_str, day, year = m.group(1), m.group(2), m.group(3)
    dt = datetime.strptime(f"{month_str} {day}, {year}", "%B %d, %Y")

    date_str = dt.strftime("%Y-%m-%d")
    decade = f"{(dt.year // 10) * 10}s"
    return date_str, decade

# ------------------------------------------------------------
# Recursively find all .txt files (UNCHANGED logic, Path-based return)
# ------------------------------------------------------------
def find_all_text_files(root: str) -> List[str]:
    out: List[str] = []
    for dirpath, _, files in os.walk(root):
        for f in files:
            if f.lower().endswith(".txt"):
                out.append(os.path.join(dirpath, f))
    return out

# ------------------------------------------------------------
# Chamber tracking (UNCHANGED)
# ------------------------------------------------------------
HOUSE_REGEX = re.compile(r"^\s*HOUSE\s*$", re.MULTILINE)
SENATE_REGEX = re.compile(r"^\s*SENATE\s*$", re.MULTILINE)

def determine_chamber_markers(text: str):
    markers = []

    for m in HOUSE_REGEX.finditer(text):
        markers.append((m.start(), "H"))
    for m in SENATE_REGEX.finditer(text):
        markers.append((m.start(), "S"))

    markers.sort(key=lambda x: x[0])
    return markers

def chamber_for_offset(offset: int, markers) -> str:
    chamber = ""
    for m_offset, ch in markers:
        if m_offset <= offset:
            chamber = ch
        else:
            break
    return chamber

# ------------------------------------------------------------
# Gender inference (UNCHANGED)
# ------------------------------------------------------------
def infer_gender(speaker: str) -> str:
    if speaker.startswith(("Ms.", "Mrs.")):
        return "F"
    if speaker.startswith("Mr."):
        return "M"
    return ""

# ------------------------------------------------------------
# Worker function (CHANGED: accepts output_dir + imports speaker_scraper inside worker)
# ------------------------------------------------------------
def process_batch(batch_args):
    """
    Each worker writes to its own CSV in --temp_dir:
      <temp_dir>/parsed_CRECB_speeches_part_<i>.csv
    """
    (
        batch_id,
        file_list,
        temp_dir,
        scraper_path,
    ) = batch_args

    # Import inside the worker so each process sets sys.path cleanly
    sys.path.insert(0, scraper_path)
    import speaker_scraper_CRECB_testing  # noqa: F401

    outname = os.path.join(temp_dir, f"parsed_CRECB_speeches_part_{batch_id}.csv")

    with open(outname, "w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=[
            "filename", "folder",
            "date", "decade",
            "speaker", "gender",
            "chamber",
            "speech",
        ])
        writer.writeheader()

        for filepath in file_list:
            filename = os.path.basename(filepath)
            folder = os.path.dirname(filepath)

            try:
                date_str, decade = extract_date_from_filename(filename)
            except Exception as e:
                print(f"[Batch {batch_id}] ERROR extracting date from {filename}: {e}")
                continue

            # Read text file
            try:
                with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                    text = f.read()
            except Exception as e:
                print(f"[Batch {batch_id}] ERROR reading {filepath}: {e}")
                continue

            chamber_markers = determine_chamber_markers(text)

            try:
                speeches = list(speaker_scraper_CRECB_testing.scrape(text))
            except Exception as e:
                print(f"[Batch {batch_id}] ERROR scraping {filename}: {e}")
                continue

            cursor = 0
            for speaker, speech_text in speeches:
                idx = text.find(speech_text, cursor)
                if idx == -1:
                    idx = cursor
                cursor = idx + len(speech_text)

                chamber = chamber_for_offset(idx, chamber_markers)
                gender = infer_gender(speaker)

                writer.writerow({
                    "filename": filename,
                    "folder": folder,
                    "date": date_str,
                    "decade": decade,
                    "speaker": speaker,
                    "gender": gender,
                    "chamber": chamber,
                    "speech": speech_text,
                })

    print(f"[Batch {batch_id}] Completed {len(file_list)} files → {outname}")
    return outname

# ------------------------------------------------------------
# Main parallel controller (CHANGED: CLI-driven paths + temp dir)
# ------------------------------------------------------------
def parse_all_files_parallel(input_dir: str, output_csv: str, nprocs: int, scraper_path: str, temp_dir: str):
    all_files = find_all_text_files(input_dir)
    num_files = len(all_files)

    if num_files == 0:
        raise SystemExit(f"No .txt files found under: {input_dir}")

    # Keep your cap at 50, but allow overriding via --nprocs
    ncpu = max(1, min(nprocs, cpu_count(), 50))
    print(f"Found {num_files} files under {input_dir}. Using {ncpu} CPUs.")

    os.makedirs(temp_dir, exist_ok=True)

    # Split files into N chunks (UNCHANGED logic)
    chunks = []
    chunk_size = (num_files + ncpu - 1) // ncpu

    for i in range(ncpu):
        start = i * chunk_size
        end = min(start + chunk_size, num_files)
        if start < end:
            chunks.append((i, all_files[start:end], temp_dir, scraper_path))

    # Run workers
    with Pool(ncpu) as pool:
        part_files = pool.map(process_batch, chunks)

    # --------------------------------------------------------
    # CONCATENATE RESULTS (UNCHANGED output format)
    # --------------------------------------------------------
    print("Merging partial CSVs...")

    with open(output_csv, "w", newline="", encoding="utf-8") as out:
        writer = None

        for pf in part_files:
            with open(pf, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader)

                if writer is None:
                    writer = csv.writer(out)
                    writer.writerow(header)

                for row in reader:
                    writer.writerow(row)

    print(f"\n✅ DONE! Saved final CSV → {output_csv}")
    print("Cleaning up temporary part files...")

    for pf in part_files:
        try:
            os.remove(pf)
        except OSError:
            pass

    print("All done.")

# ------------------------------------------------------------
# ENTRY POINT (NEW: argparse)
# ------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Parse CRECB speeches by speaker from OCR .txt files")
    parser.add_argument("--input_dir", required=True, help="Root directory containing (precleaned) .txt files.")
    parser.add_argument("--output_csv", required=True, help="Path to write the merged parsed speeches CSV.")
    parser.add_argument("--nprocs", type=int, default=min(cpu_count(), 50), help="Number of worker processes (capped at 50).")
    parser.add_argument(
        "--scraper_path",
        default="/local/scratch/group/guldigroup/climate_change/congress/congressional_scraper",
        help="Path containing speaker_scraper.py",
    )
    parser.add_argument(
        "--temp_dir",
        default=".",
        help="Directory to write partial CSVs (default: current working directory).",
    )
    args = parser.parse_args()

    input_dir = str(Path(args.input_dir).expanduser().resolve())
    output_csv = str(Path(args.output_csv).expanduser().resolve())
    scraper_path = str(Path(args.scraper_path).expanduser().resolve())
    temp_dir = str(Path(args.temp_dir).expanduser().resolve())

    if not os.path.isdir(input_dir):
        raise SystemExit(f"ERROR: --input_dir is not a directory: {input_dir}")
    if not os.path.isdir(scraper_path):
        raise SystemExit(f"ERROR: --scraper_path is not a directory: {scraper_path}")

    parse_all_files_parallel(
        input_dir=input_dir,
        output_csv=output_csv,
        nprocs=args.nprocs,
        scraper_path=scraper_path,
        temp_dir=temp_dir,
    )

if __name__ == "__main__":
    main()