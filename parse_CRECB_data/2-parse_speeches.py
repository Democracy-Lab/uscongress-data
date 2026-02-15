#!/usr/bin/env python3
import os
import re
import csv
import sys
from datetime import datetime
from multiprocessing import Pool, cpu_count
from pathlib import Path

csv.field_size_limit(sys.maxsize)

# ------------------------------------------------------------
# IMPORT SPEAKER SCRAPER FROM SPECIFIC PATH
# ------------------------------------------------------------
SCRAPER_PATH = "/local/scratch/group/guldigroup/climate_change/congress/congressional_scraper"
sys.path.insert(0, SCRAPER_PATH)
import speaker_scraper

# ------------------------------------------------------------
# CONFIG — hardcode your path here
# ------------------------------------------------------------
BASE_DIR = "/local/scratch/group/guldigroup/climate_change/congress/OCR_testing/tesseract_CRECB_OCR_2025-11-20"
OUTPUT_CSV = "parsed_CRECB_speeches_12-07.csv"

# ------------------------------------------------------------
# Regex to extract date from filename
# ------------------------------------------------------------
DATE_REGEX = re.compile(r'([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})')

def extract_date_from_filename(filename):
    m = DATE_REGEX.search(filename)
    if not m:
        raise ValueError(f"Could not extract date from filename: {filename}")

    month_str, day, year = m.group(1), m.group(2), m.group(3)
    dt = datetime.strptime(f"{month_str} {day}, {year}", "%B %d, %Y")

    date_str = dt.strftime("%Y-%m-%d")
    decade = f"{(dt.year // 10) * 10}s"
    return date_str, decade

# ------------------------------------------------------------
# Recursively find all .txt files
# ------------------------------------------------------------
def find_all_text_files(root):
    for dirpath, _, files in os.walk(root):
        for f in files:
            if f.lower().endswith(".txt"):
                yield os.path.join(dirpath, f)

# ------------------------------------------------------------
# Chamber tracking
# ------------------------------------------------------------
HOUSE_REGEX = re.compile(r'^\s*HOUSE\s*$', re.MULTILINE)
SENATE_REGEX = re.compile(r'^\s*SENATE\s*$', re.MULTILINE)

def determine_chamber_markers(text):
    markers = []

    for m in HOUSE_REGEX.finditer(text):
        markers.append((m.start(), "H"))
    for m in SENATE_REGEX.finditer(text):
        markers.append((m.start(), "S"))

    markers.sort(key=lambda x: x[0])
    return markers

def chamber_for_offset(offset, markers):
    chamber = ""
    for m_offset, ch in markers:
        if m_offset <= offset:
            chamber = ch
        else:
            break
    return chamber

# ------------------------------------------------------------
# Gender inference
# ------------------------------------------------------------
def infer_gender(speaker):
    if speaker.startswith(("Ms.", "Mrs.")):
        return "F"
    if speaker.startswith("Mr."):
        return "M"
    return ""

# ------------------------------------------------------------
# WORKER FUNCTION — runs on each CPU
# ------------------------------------------------------------
def process_batch(batch_args):
    """
    Each worker writes to its own CSV: parsed_CRECB_speeches_part_<i>.csv
    """
    batch_id, file_list = batch_args
    outname = f"parsed_CRECB_speeches_part_{batch_id}.csv"

    with open(outname, "w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=[
            "filename", "folder",
            "date", "decade",
            "speaker", "gender",
            "chamber",
            "speech"
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
                with open(filepath, "r", encoding="utf-8") as f:
                    text = f.read()
            except Exception as e:
                print(f"[Batch {batch_id}] ERROR reading {filepath}: {e}")
                continue

            chamber_markers = determine_chamber_markers(text)

            try:
                speeches = list(speaker_scraper.scrape(text))
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
                    "speech": speech_text
                })

    print(f"[Batch {batch_id}] Completed {len(file_list)} files → {outname}")
    return outname

# ------------------------------------------------------------
# MAIN PARALLEL CONTROLLER
# ------------------------------------------------------------
def parse_all_files_parallel():
    all_files = list(find_all_text_files(BASE_DIR))
    num_files = len(all_files)

    if num_files == 0:
        print("No text files found.")
        sys.exit(1)

    ncpu = min(cpu_count(), 50)
    print(f"Found {num_files} files. Using {ncpu} CPUs.")

    # Split files into N chunks
    chunks = []
    chunk_size = (num_files + ncpu - 1) // ncpu

    for i in range(ncpu):
        start = i * chunk_size
        end = min(start + chunk_size, num_files)
        if start < end:
            chunks.append((i, all_files[start:end]))

    # Run workers
    with Pool(ncpu) as pool:
        part_files = pool.map(process_batch, chunks)

    # --------------------------------------------------------
    # CONCATENATE RESULTS
    # --------------------------------------------------------
    print("Merging partial CSVs...")

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as out:
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

    print(f"\n✅ DONE! Saved final CSV → {OUTPUT_CSV}")
    print(f"Cleaning up temporary part files...")

    for pf in part_files:
        os.remove(pf)

    print("All done.")

# ------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------
if __name__ == "__main__":
    parse_all_files_parallel()
