#!/usr/bin/env python3
"""
Parallel OCR of 'general_with_date' PDFs in CRECB_raw_7-25-25.

UPDATES:
- Automatically detects previous-day OCR folder:
      tesseract_CRECB_OCR_<YESTERDAY>_general-and-house-senate
- Moves previously OCR’d complete general-date .txt files into new folder
- Avoids reprocessing PDFs that already have completed OCR output
- Converts pages → TIFF one at a time (low temp disk usage)
"""

import os

os.environ["OMP_NUM_THREADS"] = "1"
os.environ["OMP_THREAD_LIMIT"] = "1"

import re
import subprocess
from pathlib import Path
from multiprocessing import Pool
import argparse
import pytesseract
from PIL import Image
from datetime import datetime, timedelta
import shutil

# -------------------------------
# Regexes: Date + Not House/Senate
# -------------------------------

MONTHS = (
    "January|February|March|April|May|June|July|August|September|October|November|December"
)

DATE_RE = re.compile(
    rf"\b({MONTHS})\s+\d{{1,2}},\s*\d{{4}}\b", re.IGNORECASE
)

HOUSE_RE = re.compile(r"\bHouse(?:\s+of\s+Representatives)?\b", re.IGNORECASE)
SENATE_RE = re.compile(r"\bSenate\b", re.IGNORECASE)

def is_general_with_date(filename: str) -> bool:
    if not DATE_RE.search(filename):
        return False
    if HOUSE_RE.search(filename):
        return False
    if SENATE_RE.search(filename):
        return False
    return True


# -------------------------------
# Extract year
# -------------------------------
def extract_year_from_path(path: Path) -> int | None:
    m = re.search(r"\b(18\d{2}|19\d{2}|2000)\b", str(path))
    if m:
        return int(m.group(1))
    return None


# -------------------------------
# Count PDF pages
# -------------------------------
def get_num_pages(pdf_path: Path) -> int:
    try:
        result = subprocess.run(
            ["pdfinfo", str(pdf_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        for line in result.stdout.splitlines():
            if line.startswith("Pages:"):
                return int(line.split()[1])
    except Exception as e:
        print(f"[ERROR] pdfinfo failed for {pdf_path}: {e}")
    return 0


# -------------------------------
# OCR ONE PDF
# -------------------------------
def ocr_single_pdf(job):
    pdf_path, out_root, tmp_root = job
    pdf_path = Path(pdf_path)
    out_root = Path(out_root)
    tmp_root = Path(tmp_root)

    tmp_root.mkdir(parents=True, exist_ok=True)

    year = extract_year_from_path(pdf_path)
    if year is None:
        print(f"[ERROR] Could not detect year for {pdf_path}, skipping")
        return None

    year_dir = out_root / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)

    txt_path = year_dir / (pdf_path.stem + ".txt")

    # Skip if full OCR already exists
    if txt_path.exists():
        print(f"[INFO] Already have OCR: {txt_path}")
        return str(txt_path)

    print(f"[INFO] OCR'ing: {pdf_path} → {txt_path}")

    num_pages = get_num_pages(pdf_path)
    if num_pages == 0:
        print(f"[ERROR] Could not determine page count for {pdf_path}")
        return None

    all_text = []

    for page_num in range(1, num_pages + 1):

        prefix = tmp_root / f"{pdf_path.stem}_p{page_num}"

        cmd = [
            "pdftoppm",
            "-tiff",
            "-r", "300",
            "-gray",
            "-f", str(page_num),
            "-l", str(page_num),
            str(pdf_path),
            str(prefix)
        ]

        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError:
            print(f"[ERROR] pdftoppm failed page {page_num} of {pdf_path}")
            continue

        generated_tiffs = list(tmp_root.glob(f"{prefix.name}-*.tif"))
        if not generated_tiffs:
            print(f"[ERROR] No TIFF generated for page {page_num}")
            continue

        tiff_file = generated_tiffs[0]

        try:
            img = Image.open(tiff_file)
            text = pytesseract.image_to_string(img, lang="eng")
            all_text.append(text)
        except Exception as e:
            print(f"[ERROR] OCR failed for {tiff_file}: {e}")

        try:
            tiff_file.unlink()
        except:
            pass

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n\n".join(all_text))

    print(f"[DONE] Saved: {txt_path}")
    return str(txt_path)


# -------------------------------
# MAIN
# -------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=str, required=True,
                        help="CRECB_raw_7-25-25 directory")
    parser.add_argument("--cpus", type=int, default=4,
                        help="Number of CPUs for parallel OCR")
    args = parser.parse_args()

    root = Path(args.root)
    if not root.exists():
        raise SystemExit(f"[FATAL] Root does not exist: {root}")

    # ----------------------------
    # NEW OCR OUTPUT FOLDER (TODAY)
    # ----------------------------
    date_str = datetime.now().strftime("%Y-%m-%d")
    out_root = Path(f"tesseract_CRECB_OCR_{date_str}")
    out_root.mkdir(exist_ok=True)

    # Temp directory
    tmp_root = Path("/local/scratch/rlarso3/tmp_ocr")
    tmp_root.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------
    # AUTO-DETECT PREVIOUS OCR FOLDER (YESTERDAY)
    # ------------------------------------------------
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    previous_ocr = Path(f"tesseract_CRECB_OCR_{yesterday}_general-and-house-senate")

    if previous_ocr.exists():
        print(f"[INFO] Found previous OCR folder: {previous_ocr}")
        print("[INFO] Copying previously-completed general-date results...")

        for txt_file in previous_ocr.rglob("*.txt"):
            pdf_name = txt_file.stem + ".pdf"
            if is_general_with_date(pdf_name):
                year = extract_year_from_path(txt_file)
                if year is None:
                    continue
                dest_year = out_root / str(year)
                dest_year.mkdir(exist_ok=True)

                dest = dest_year / txt_file.name
                if not dest.exists():
                    shutil.copy2(txt_file, dest)
                    print(f"  Copied: {txt_file} → {dest}")
    else:
        print(f"[INFO] Previous OCR folder not found: {previous_ocr}")

    # -------------------------
    # FIND GENERAL-WITH-DATE PDFs
    # -------------------------
    pdf_list = []
    for pdf_path in root.rglob("*.pdf"):
        if is_general_with_date(pdf_path.name):
            pdf_list.append(pdf_path)

    print(f"[INFO] Found {len(pdf_list)} GENERAL-date PDFs.")

    # Filter out already OCR'd
    jobs = []
    for p in pdf_list:
        year = extract_year_from_path(p)
        year_dir = out_root / str(year)
        txt_path = year_dir / (p.stem + ".txt")

        if not txt_path.exists():
            jobs.append((p, out_root, tmp_root))

    print(f"[INFO] Need OCR for {len(jobs)} PDFs (others already done).")

    # -------------------------
    # Parallel OCR
    # -------------------------
    print(f"[INFO] Starting OCR with {args.cpus} CPUs...")
    with Pool(processes=args.cpus) as pool:
        pool.map(ocr_single_pdf, jobs)

    print(f"\n[COMPLETE] OCR stored in: {out_root}\n")


if __name__ == "__main__":
    main()
