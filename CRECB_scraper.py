import argparse
import logging
import sys
import threading
import time
import os
import re
from typing import Any, Dict, List, Set
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor
import requests

# ─── Configuration ────────────────────────────────────────────────────────────
BASE_API_URL = "https://api.govinfo.gov"
PAGE_SIZE    = 1000
RETRY_DELAY  = 15    # seconds on HTTP 429
MAX_RETRIES  = 3
CHUNK_YEARS  = 25    # split published scan into 25-year chunks to ensure all packages are found

# ─── Thread-safety for rotating API keys ──────────────────────────────────────
_key_lock   = threading.Lock()
_key_index  = 0
API_KEYS: List[str] = []

# ─── Per-year state containers ────────────────────────────────────────────────
year_states      : Dict[str, Dict[str, Any]] = {}  # year -> { pdf_ctr, xml_ctr, html_dir, xml_dir, lock }
year_states_lock = threading.Lock()

# ─── Logging Setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# ─── Helpers ──────────────────────────────────────────────────────────────────
def sanitize(text: str) -> str:
    """Remove filesystem-unfriendly characters."""
    return re.sub(r'[\\/*?:"<>|]', "", text)

def get_api_key() -> str:
    """Round-robin selection of API key."""
    global _key_index
    with _key_lock:
        key = API_KEYS[_key_index]
        _key_index = (_key_index + 1) % len(API_KEYS)
    return key

def rate_limit():
    """Fixed delay to avoid hammering the API."""
    time.sleep(0.1)

def fetch_json(url: str, params: Dict[str,Any]) -> Dict[str,Any]:
    """GET with retries and basic rate-limiting."""
    for _ in range(MAX_RETRIES):
        rate_limit()
        resp = requests.get(url, params=params)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", RETRY_DELAY))
            logging.warning(f"429 rate-limited, sleeping {wait}s…")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()

# ─── 1) Discover all CRECB packages in 25-year chunks ─────────────────────────
def get_all_packages(start_year: int) -> List[str]:
    pkgs_set: Set[str] = set()
    today = datetime.utcnow().date()
    for sy in range(start_year, today.year + 1, CHUNK_YEARS):
        chunk_start = date(sy, 1, 1)
        end_year    = min(sy + CHUNK_YEARS - 1, today.year)
        chunk_end   = date(end_year, 12, 31) if end_year < today.year else today
        url = f"{BASE_API_URL}/published/{chunk_start.isoformat()}/{chunk_end.isoformat()}"
        params = {
            "api_key":    get_api_key(),
            "collection": "CRECB",
            "pageSize":   PAGE_SIZE,
            "offsetMark": "*"
        }
        logging.info(f"Fetching CRECB packages from {chunk_start} to {chunk_end}…")
        chunk_count = 0
        while True:
            data = fetch_json(url, params)
            for p in data.get("packages", []):
                pkg_id = p.get("packageId")
                if pkg_id and pkg_id not in pkgs_set:
                    pkgs_set.add(pkg_id)
                    chunk_count += 1
            nxt = data.get("nextOffsetMark")
            logging.info(f"  fetched {chunk_count} packages so far; nextOffsetMark={nxt!r}")
            if not nxt:
                break
            params["offsetMark"] = nxt
        logging.info(f"Chunk {chunk_start}–{chunk_end}: found {chunk_count} packages")
    logging.info(f"Total packages discovered: {len(pkgs_set)}")
    return sorted(pkgs_set)

# ─── 2) List every granuleId in a package via the granules endpoint ─────────
def get_granules(pkg: str) -> List[str]:
    all_ids: List[str] = []
    url    = f"{BASE_API_URL}/packages/{pkg}/granules"
    params = {"api_key": get_api_key(), "pageSize": PAGE_SIZE, "offsetMark": "*"}

    while True:
        data = fetch_json(url, params)
        for g in data.get("granules", []):
            if (gid := g.get("granuleId")):
                all_ids.append(gid)
        if data.get("nextPage"):
            url    = data["nextPage"]
            params = {"api_key": get_api_key()}
            logging.info(f"    → following nextPage link for '{pkg}'")
            continue
        if (nxt := data.get("nextOffsetMark")):
            url    = f"{BASE_API_URL}/packages/{pkg}/granules"
            params = {"api_key": get_api_key(), "pageSize": PAGE_SIZE, "offsetMark": nxt}
            logging.info(f"    → paging '{pkg}', nextOffsetMark={nxt!r}")
            continue
        break

    return all_ids

def get_summary(pkg: str, gran: str) -> Dict[str,Any]:
    url    = f"{BASE_API_URL}/packages/{pkg}/granules/{gran}/summary"
    params = {"api_key": get_api_key()}
    for _ in range(MAX_RETRIES):
        rate_limit()
        resp = requests.get(url, params=params)
        if resp.status_code == 429:
            time.sleep(RETRY_DELAY)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()
    return {}

# ─── Worker: download PDF & MODS into per-year buckets ────────────────────────
def worker(pkg_gran: Any):
    pkg, gran = pkg_gran
    summ = get_summary(pkg, gran)
    dstr = summ.get("dateIssued", "").split("T")[0]
    if not dstr:
        return
    year  = dstr.split("-")[0]
    title = sanitize(summ.get("title", "NO TITLE"))

    with year_states_lock:
        if year not in year_states:
            year_dir = os.path.join(OUTPUT, year)
            pdf_dir  = os.path.join(year_dir, f"raw_pdf_{year}")
            xml_dir  = os.path.join(year_dir, f"raw_xml_{year}")
            os.makedirs(pdf_dir, exist_ok=True)
            os.makedirs(xml_dir, exist_ok=True)
            year_states[year] = {
                "pdf_ctr":  1,
                "xml_ctr":  1,
                "pdf_dir":  pdf_dir,
                "xml_dir":  xml_dir,
                "lock":     threading.Lock()
            }

    st = year_states[year]
    base = f"{pkg}_{gran}"

    with st["lock"]:
        pdf_url  = f"{BASE_API_URL}/packages/{pkg}/granules/{gran}/pdf"
        resp = requests.get(pdf_url, params={"api_key": get_api_key()})
        if resp.status_code == 200:
            idx   = st["pdf_ctr"]
            fname = f"{idx}-{title}-{base}.pdf"
            path  = os.path.join(st["pdf_dir"], fname)
            with open(path, "wb") as f:
                f.write(resp.content)
            st["pdf_ctr"] += 1
        else:
            logging.warning(f"No PDF for {base} (status {resp.status_code})")

        mods_url = f"{BASE_API_URL}/packages/{pkg}/granules/{gran}/mods"
        resp = requests.get(mods_url, params={"api_key": get_api_key()})
        if resp.status_code == 200:
            idx   = st["xml_ctr"]
            fname = f"{idx}-{title}-{base}.xml"
            path  = os.path.join(st["xml_dir"], fname)
            with open(path, "w", encoding="utf-8") as f:
                f.write(resp.text)
            st["xml_ctr"] += 1
        else:
            logging.warning(f"No MODS for {base} (status {resp.status_code})")

# ─── Orchestration ────────────────────────────────────────────────────────────
def crawl_bound(workers: int, start_year: int, parallel: bool):
    packages = get_all_packages(start_year)

    def pkg_sort_key(pkg: str):
        m = re.search(r'(\d{4})-pt(\d+)', pkg)
        if m:
            year, part = int(m.group(1)), int(m.group(2))
        else:
            year = int(re.search(r'(\d{4})', pkg).group(1))
            part = 0
        return (year, part)

    packages.sort(key=pkg_sort_key)

    logging.info(f"Parallel mode: {'ON' if parallel else 'OFF'} (workers={workers if parallel else 1})")

    if parallel and workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for pkg in packages:
                logging.info(f"Enumerating granules for package '{pkg}'…")
                ids = get_granules(pkg)
                logging.info(f"  → found {len(ids)} granules in '{pkg}', dispatching downloads…")
                pool.map(worker, ((pkg, gran) for gran in ids))
    else:
        for pkg in packages:
            logging.info(f"Enumerating granules for package '{pkg}'…")
            ids = get_granules(pkg)
            logging.info(f"  → found {len(ids)} granules in '{pkg}', downloading sequentially…")
            for gran in ids:
                worker((pkg, gran))

    logging.info("CRECB scrape complete!")

# ─── CLI Entry Point ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Full CRECB scraper: PDF + MODS per year")
    p.add_argument("output",    help="Base folder for YYYY/raw_pdf_YYYY & raw_xml_YYYY")
    p.add_argument("--api-keys", required=True, help="Comma-separated GovInfo API keys")
    p.add_argument("--workers",  type=int, default=4, help="Parallel threads")
    p.add_argument("--start-year", type=int, default=1873, help="Start year for scraping (default: 1873)")
    p.add_argument("--parallel", action="store_true",
               help="Run downloads in parallel (default: off)")

    args = p.parse_args()

    API_KEYS = [k.strip() for k in args.api_keys.split(",") if k.strip()]
    if not API_KEYS:
        logging.error("At least one API key is required")
        sys.exit(1)

    OUTPUT     = args.output
    WORKERS    = args.workers
    START_YEAR = args.start_year
    os.makedirs(OUTPUT, exist_ok=True)

    crawl_bound(WORKERS, START_YEAR, args.parallel)
