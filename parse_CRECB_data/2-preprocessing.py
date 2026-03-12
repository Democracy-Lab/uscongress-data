#!/usr/bin/env python3
"""
preclean_crecb_ocr.py

Step 0 preprocessing for CRECB OCR .txt files.

Goal:
1) Apply the SAME character normalization you used in add_debate_titles.py (normalize_ocr)
2) Remove boilerplate EXACTLY as your existing code did it:
   - remove_headers_with_fuzz() logic from add_debate_titles.py (line-based header/footer removal)
   - PLUS the fuzzy "CONGRESSIONAL RECORD" line removal from clean_record_boilerplate.py
     (but applied to the raw OCR text files, line-by-line)

Important constraints to avoid breaking speaker detection:
- We DO NOT strip leading whitespace from lines globally.
- We DO NOT collapse whitespace across the whole text.
- We keep blank lines as-is (except when a whole line is removed by your boilerplate rules).
- We do NOT dehyphenate or reflow wrapped lines.

Input:  a directory tree containing OCR .txt files
Output: a mirrored directory tree in an output directory with cleaned .txt files

Usage:
  python preclean_crecb_ocr.py \
    --input_dir  /path/to/tesseract_CRECB_OCR \
    --output_dir /path/to/tesseract_CRECB_OCR_PRECLEAN \
    --nprocs 50

Notes on "EXACTLY":
- Header removal uses your exact thresholds and logic from remove_headers_with_fuzz().
- Fuzzy "CONGRESSIONAL RECORD" removal uses your exact Levenshtein<=1 sliding window approach.
- The only additions are (a) CLI + directory mirroring + multiprocessing, and
  (b) applying the fuzzy line removal at file-level (because Step 0 operates on raw OCR files).
"""

from __future__ import annotations

import argparse
import os
import re
import csv
from pathlib import Path
from difflib import SequenceMatcher
from multiprocessing import Pool, cpu_count
from typing import Iterable, Tuple, Dict, Optional

# ----------------------------
# NEW: safe file-level character normalization (Suggestions 1–6)
# Must run BEFORE any boilerplate removal.
# ----------------------------

# (1) NBSP -> space
# (2) Remove BOM / ZWNBSP markers
# (3) Remove additional invisible controls (word joiner, bidi marks, LRM/RLM)
# (4) Normalize curly quotes/apostrophes to straight
# (5) Ellipsis -> "..."
# (6) Normalize thin/narrow/figure spaces -> normal space
_EXTRA_DELETE_RE = re.compile(r"[\ufeff\uFEFF\u2060\u200E\u200F\u202A\u202B\u202C\u202D\u202E]")
_EXTRA_SPACE_RE = re.compile(r"[\u2009\u202F\u2007]")

def normalize_text_pre(text: str) -> str:
    text = text.replace("\u00A0", " ")          # (1) NBSP
    text = _EXTRA_DELETE_RE.sub("", text)       # (2) BOM/ZWNBSP + (3) invisibles
    text = _EXTRA_SPACE_RE.sub(" ", text)       # (6) thin/narrow/figure spaces

    # (4) curly quotes/apostrophes
    text = (text.replace("’", "'")
                .replace("‘", "'")
                .replace("“", '"')
                .replace("”", '"'))

    # (5) ellipsis
    text = text.replace("…", "...")

    return text

# ----------------------------
# FROM add_debate_titles.py
# ----------------------------

def normalize_ocr(line: str) -> str:
    # Remove zero-width spaces, soft hyphens, NB hyphens
    line = re.sub(r"[\u200B\u200C\u200D\u00AD\u2011]", "", line)
    # Normalize EM/EN dashes to simple ascii hyphen
    line = line.replace("—", "-").replace("–", "-")
    return line

def fuzzy_match(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()

def _log_removed_line(
    log_fh,
    rel_path: str,
    reason: str,
    line: str,
    stats: Dict[str, int],
) -> None:
    """
    Logging helper. Does NOT alter cleaning logic.
    Writes one line per removal to the worker log file.
    """
    if stats is not None:
        stats[reason] = stats.get(reason, 0) + 1
    if log_fh is not None:
        # Preserve the removed line content (post-normalization used in matching).
        # Do not strip leading spaces; only strip trailing newlines for clean logging.
        safe_line = line.rstrip("\r\n")
        log_fh.write(f"{rel_path}\t{reason}\t{safe_line}\n")

def remove_headers_with_fuzz(
    text: str,
    *,
    log_fh=None,
    rel_path: str = "",
    stats: Optional[Dict[str, int]] = None
) -> str:
    """
      - Fuzzy SENATE / HOUSE OF REPRESENTATIVES require >50% caps
      - AUTHENTIC / INFORMATION / GPO lines also require >50% caps
      - (NEW) Remove (exact/fuzzy) CONGRESSIONAL RECORD ONLY if there are
        no other non-date alpha words on the line (months/abbrs allowed).
    """
    lines = text.splitlines()
    cleaned = []
    skip_next = False

    def _gt_half_caps(s: str) -> bool:
        letters = [c for c in s if c.isalpha()]
        if not letters:
            return False
        caps = sum(1 for c in letters if c.isupper())
        return (caps / len(letters)) > 0.5

    # Month names + common abbreviations (uppercased)
    _MONTHS = {
        "JANUARY", "JAN",
        "FEBRUARY", "FEB",
        "MARCH", "MAR",
        "APRIL", "APR",
        "MAY",
        "JUNE", "JUN",
        "JULY", "JUL",
        "AUGUST", "AUG",
        "SEPTEMBER", "SEP", "SEPT",
        "OCTOBER", "OCT",
        "NOVEMBER", "NOV",
        "DECEMBER", "DEC",
        "SUNDAY", "SUN",
        "MONDAY", "MON",
        "TUESDAY", "TUES",
        "WEDNESDAY", "WEDS",
        "THURSDAY", "THURS",
        "FRIDAY", "FRI",
        "SATURDAY", "SAT"
    }

    def _should_remove_cong_record_line(original_line: str) -> bool:
        """
        Return True iff the line has no alpha tokens other than:
          - CONGRESSIONAL / RECORD
          - month names/abbreviations
        Digits/punctuation don't matter (years/numbers are allowed).
        """
        # Use the same normalization context as the main loop (line already normalize_ocr'd),
        # but be robust if called with raw.
        up_line = original_line.upper()

        # Alpha tokens only (so years/numbers won't interfere)
        toks = re.findall(r"[A-Z]+", up_line)

        # Remove the boilerplate tokens themselves
        toks = [t for t in toks if t not in {"CONGRESSIONAL", "RECORD"}]

        # Remove date-related month tokens
        toks = [t for t in toks if t not in _MONTHS]

        # If anything alpha remains, it's likely real speech content → do NOT remove
        return len(toks) == 0

    for line in lines:
        line = normalize_ocr(line)
        up = line.strip().upper()

        stripped = line.strip()
        if re.fullmatch(r"\d{4}", stripped):
            yr = int(stripped)
            if 1873 <= yr <= 1994:
                _log_removed_line(log_fh, rel_path, "hdr_year_1873_1994", line, stats)
                continue

        # ---- MODIFIED: exact CONGRESSIONAL RECORD removal now gated ----
        if "CONGRESSIONAL RECORD" in up and _should_remove_cong_record_line(line):
            _log_removed_line(log_fh, rel_path, "hdr_contains_CONGRESSIONAL_RECORD", line, stats)
            continue

        # ---- chamber headers: require >50% caps ----
        if fuzzy_match(up, "HOUSE OF REPRESENTATIVES") >= 0.90 and _gt_half_caps(line):
            skip_next = True
            _log_removed_line(log_fh, rel_path, "hdr_fuzzy_HOUSE_OF_REPRESENTATIVES", line, stats)
            continue

        if fuzzy_match(up, "SENATE") >= 0.83 and _gt_half_caps(line):
            skip_next = True
            _log_removed_line(log_fh, rel_path, "hdr_fuzzy_SENATE", line, stats)
            continue
        # --------------------------------------------

        # ---- MODIFIED: fuzzy CONGRESSIONAL RECORD removal now gated ----
        if fuzzy_match(up, "CONGRESSIONAL RECORD") >= 0.90 and _should_remove_cong_record_line(line):
            _log_removed_line(log_fh, rel_path, "hdr_fuzzy_CONGRESSIONAL_RECORD", line, stats)
            continue

        words = re.findall(r"[A-Za-z]+", up)
        key = {"AUTHENTICATED", "AUTHENTICATE", "AUTHENTICATION",
               "AUTHENTIC", "INFORMATION", "GPO"}
        if words and all(w in key for w in words) and _gt_half_caps(line):
            _log_removed_line(log_fh, rel_path, "hdr_auth_gpo_line", line, stats)
            continue

        if skip_next:
            skip_next = False
            alpha = re.findall(r"[A-Za-z]+", line)
            if alpha:
                w = alpha[0]
                if (w.isupper() and len(w) >= 3) or \
                   (len(w) >= 6 and sum(c.islower() for c in w) <= 1):
                    _log_removed_line(log_fh, rel_path, "hdr_skip_next_after_chamber_header", line, stats)
                    continue

        cleaned.append(line)

    return "\n".join(cleaned)

# ----------------------------
# FROM clean_record_boilerplate.py
# ----------------------------

TARGET = "CONGRESSIONAL RECORD"
T_LEN = len(TARGET)

def levenshtein_leq1(a: str, b: str) -> bool:
    """
    EXACT logic from your script.
    Returns True if Levenshtein distance <= 1.
    """
    if a == b:
        return True

    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return False

    if la == lb:
        diff = sum(c1 != c2 for c1, c2 in zip(a, b))
        return diff <= 1

    if la > lb:
        a, b = b, a
        la, lb = lb, la

    for i in range(lb):
        if b[:i] + b[i+1:] == a:
            return True

    return False

def line_contains_fuzzy_target(line: str) -> bool:
    """
    EXACT logic from your script, but used on raw OCR file lines.
    Remove a line if ANY substring of length len(TARGET) or len(TARGET)±1
    matches the target with Levenshtein distance <= 1.
    """
    up = line.upper()

    for L in (T_LEN - 1, T_LEN, T_LEN + 1):
        if L <= 0 or L > len(up):
            continue

        for i in range(len(up) - L + 1):
            window = up[i:i+L]
            window_clean = re.sub(r"[^A-Z ]", "", window)
            if levenshtein_leq1(window_clean, TARGET):
                return True

    return False

def remove_fuzzy_congressional_record_lines(
    text: str,
    *,
    log_fh=None,
    rel_path: str = "",
    stats: Optional[Dict[str, int]] = None
) -> str:
    """
    Apply your clean_record_boilerplate logic at the raw-text level.
    IMPORTANT: This removes entire lines where the fuzzy target appears.

    Logging/stats are OPTIONAL and do not alter matching logic.
    """
    out_lines = []
    for ln in text.splitlines():
        if line_contains_fuzzy_target(ln):
            _log_removed_line(log_fh, rel_path, "fuzzy_CONGRESSIONAL_RECORD_lev_leq1", ln, stats)
            continue
        out_lines.append(ln)
    return "\n".join(out_lines)

# ----------------------------
# STEP 0 PIPELINE
# ----------------------------

def preclean_text(text: str, *, log_fh=None, rel_path: str = "", stats: Optional[Dict[str, int]] = None) -> str:
    """
    Step 0 cleaning:
      0) normalize_text_pre (Suggestions 1–6) at the whole-text level
      1) normalize_ocr applied line-by-line inside remove_headers_with_fuzz
      2) remove_headers_with_fuzz (header/footer + year + authenticated/gpo blocks)
      3) remove_fuzzy_congressional_record_lines (Levenshtein<=1 fuzzy target)
    """
    # (Not logged) safe character normalization BEFORE boilerplate removal
    text = normalize_text_pre(text)

    t = remove_headers_with_fuzz(text, log_fh=log_fh, rel_path=rel_path, stats=stats)
    t = remove_fuzzy_congressional_record_lines(t, log_fh=log_fh, rel_path=rel_path, stats=stats)

    return t

def iter_txt_files(root: Path) -> Iterable[Path]:
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if fn.lower().endswith(".txt"):
                yield Path(dirpath) / fn

def mirror_out_path(in_path: Path, in_root: Path, out_root: Path) -> Path:
    rel = in_path.relative_to(in_root)
    return out_root / rel

def _safe_log_name(rel_path: Path) -> str:
    # Stable filename for per-file log, avoids directory separators
    s = str(rel_path).replace(os.sep, "__")
    # Keep it simple; long names are okay on HPC filesystems.
    return s + ".removed.tsv"

def process_one(args: Tuple[Path, Path, Path, Path]) -> Tuple[Path, int, int, Dict[str, int], Path]:
    """
    Read -> preclean -> write
    Also writes a per-file log of removed content and returns per-file stats.

    Returns:
      (out_path, in_bytes, out_bytes, stats_dict, per_file_log_path)
    """
    in_path, in_root, out_root, logs_root = args
    out_path = mirror_out_path(in_path, in_root, out_root)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    logs_root.mkdir(parents=True, exist_ok=True)

    rel_path = in_path.relative_to(in_root)

    raw = in_path.read_text(encoding="utf-8", errors="replace")

    stats: Dict[str, int] = {}
    per_file_log_path = logs_root / _safe_log_name(rel_path)

    with open(per_file_log_path, "w", encoding="utf-8") as log_fh:
        # Header row for readability
        log_fh.write("rel_path\treason\tremoved_line\n")
        cleaned = preclean_text(raw, log_fh=log_fh, rel_path=str(rel_path), stats=stats)

    out_path.write_text(cleaned, encoding="utf-8")

    in_b = len(raw.encode("utf-8", errors="replace"))
    out_b = len(cleaned.encode("utf-8", errors="replace"))
    return out_path, in_b, out_b, stats, per_file_log_path

def main():
    p = argparse.ArgumentParser(description="Step 0 precleaning for CRECB OCR .txt files")
    p.add_argument("--input_dir", required=True, help="Root directory of OCR .txt files (recursive).")
    p.add_argument("--output_dir", required=True, help="Output root directory for cleaned .txt files (mirrors structure).")
    p.add_argument("--nprocs", type=int, default=min(cpu_count(), 50), help="Number of worker processes.")
    args = p.parse_args()

    in_root = Path(args.input_dir).expanduser().resolve()
    out_root = Path(args.output_dir).expanduser().resolve()
    logs_root = out_root / "_preclean_logs"

    if not in_root.exists() or not in_root.is_dir():
        raise SystemExit(f"ERROR: input_dir is not a directory: {in_root}")

    files = sorted(iter_txt_files(in_root))
    if not files:
        raise SystemExit(f"ERROR: no .txt files found under: {in_root}")

    tasks = [(fp, in_root, out_root, logs_root) for fp in files]

    nprocs = max(1, min(args.nprocs, cpu_count()))
    print(f"Found {len(files):,} .txt files")
    print(f"Input:  {in_root}")
    print(f"Output: {out_root}")
    print(f"Workers: {nprocs}")
    print(f"Logs:   {logs_root}")

    total_in = 0
    total_out = 0
    agg_stats: Dict[str, int] = {}
    per_file_logs: list[Path] = []

    with Pool(processes=nprocs) as pool:
        for out_path, in_b, out_b, stats, log_path in pool.imap_unordered(process_one, tasks, chunksize=10):
            total_in += in_b
            total_out += out_b
            per_file_logs.append(log_path)
            for k, v in stats.items():
                agg_stats[k] = agg_stats.get(k, 0) + v

    # Merge per-file logs into a single rolling text file (TSV) at the end
    merged_log_path = out_root / "preclean_removed_lines.tsv"
    with open(merged_log_path, "w", encoding="utf-8") as outlog:
        outlog.write("rel_path\treason\tremoved_line\n")
        for lp in sorted(per_file_logs):
            with open(lp, "r", encoding="utf-8") as f:
                # skip header
                next(f, None)
                for line in f:
                    outlog.write(line)

    # Write summary CSV of removal TYPES (excluding character normalization by design)
    summary_csv_path = out_root / "preclean_removal_summary.csv"
    with open(summary_csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["reason", "count"])
        for reason in sorted(agg_stats.keys()):
            w.writerow([reason, agg_stats[reason]])

    # Also print the summary CSV to stdout
    print("\n✅ DONE")
    print(f"Total bytes: {total_in:,} -> {total_out:,} ({(total_out/total_in*100 if total_in else 0):.1f}% kept)")
    print(f"Removed-lines log written to: {merged_log_path}")
    print(f"Summary CSV written to:       {summary_csv_path}\n")

    print("reason,count")
    for reason in sorted(agg_stats.keys()):
        print(f"{reason},{agg_stats[reason]}")

if __name__ == "__main__":
    main()