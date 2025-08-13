import argparse
import os
import sys
import time
import csv
import logging
import threading
from datetime import date, datetime, timedelta
from typing import Optional, List, Dict, Any, Set, Tuple
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

# ───────────────────────── Configuration ─────────────────────────
BASE_API_URL = "https://api.govinfo.gov"
PAGE_SIZE    = 1000
RETRY_DELAY  = 15   # seconds on 429
MAX_RETRIES  = 3

# ───────────────────────── Logging ───────────────────────────────
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=[handler]
)

# ─────────────────────── Rate limiting ───────────────────────────
_rate_lock      = threading.Lock()
_last_call_time = 0.0
RATE_INTERVAL  : Optional[float] = None  # seconds between calls across all keys

def rate_limit():
    """Sleep to enforce an overall calls/second throttle if RATE_INTERVAL is set."""
    global _last_call_time, RATE_INTERVAL
    if RATE_INTERVAL is None:
        return
    with _rate_lock:
        now = time.time()
        to_wait = RATE_INTERVAL - (now - _last_call_time)
        if to_wait > 0:
            time.sleep(to_wait)
        _last_call_time = time.time()

# ─────────────────────── API keys rotation ───────────────────────
_api_keys : List[str] = []
_key_index = 0
_key_lock  = threading.Lock()

def get_api_key() -> str:
    global _key_index
    with _key_lock:
        key = _api_keys[_key_index]
        _key_index = (_key_index + 1) % len(_api_keys)
    return key

# ─────────────────────── GovInfo helpers ─────────────────────────
def get_search_results(query: str, page_size: int, offset_mark: Optional[str] = None) -> dict:
    url = f"{BASE_API_URL}/search"
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    params  = {'api_key': get_api_key(), 'historical': True}
    payload = {
        'query': query,
        'pageSize': page_size,
        'offsetMark': offset_mark or '*',
        'sorts': [{'field':'publishdate','sortOrder':'DESC'}],
        'resultLevel':'default'
    }
    for attempt in range(1, MAX_RETRIES+1):
        rate_limit()
        resp = requests.post(url, headers=headers, params=params, json=payload)
        if resp.status_code == 429:
            wait = int(resp.headers.get('Retry-After', RETRY_DELAY))
            logging.warning(f"429 rate limit, sleeping {wait}s…")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError("Max retries exceeded for search")

def get_granules(package_id: str) -> List[dict]:
    all_g = []
    url = f"{BASE_API_URL}/packages/{package_id}/granules"
    params = {'api_key': get_api_key(), 'pageSize': PAGE_SIZE, 'offsetMark': '*'}
    while True:
        rate_limit()
        resp = requests.get(url, params=params)
        if resp.status_code == 429:
            wait = int(resp.headers.get('Retry-After', RETRY_DELAY))
            logging.warning(f"429 granules limit, sleeping {wait}s…")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        data = resp.json()
        all_g.extend(data.get('granules', []))
        nxt = data.get('nextOffsetMark')
        if not nxt:
            break
        params['offsetMark'] = nxt
    return all_g

def get_granule_summary(pkg: str, gran: str) -> dict:
    url    = f"{BASE_API_URL}/packages/{pkg}/granules/{gran}/summary"
    params = {'api_key': get_api_key()}
    for attempt in range(1, MAX_RETRIES+1):
        rate_limit()
        resp = requests.get(url, params=params)
        if resp.status_code == 429:
            time.sleep(RETRY_DELAY)
            continue
        try:
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                return {}
    return {}

def get_htm(pkg: str, gran: str) -> str:
    url    = f"{BASE_API_URL}/packages/{pkg}/granules/{gran}/htm"
    params = {'api_key': get_api_key()}
    for attempt in range(1, MAX_RETRIES+1):
        rate_limit()
        resp = requests.get(url, params=params)
        if resp.status_code == 429:
            time.sleep(RETRY_DELAY)
            continue
        try:
            resp.raise_for_status()
            return resp.text
        except requests.exceptions.HTTPError:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                return ''
    return ''

def get_mods(pkg: str, gran: str) -> str:
    url    = f"{BASE_API_URL}/packages/{pkg}/granules/{gran}/mods"
    params = {'api_key': get_api_key()}
    for attempt in range(1, MAX_RETRIES+1):
        rate_limit()
        resp = requests.get(url, params=params)
        if resp.status_code == 429:
            time.sleep(RETRY_DELAY)
            continue
        try:
            resp.raise_for_status()
            return resp.text
        except requests.exceptions.HTTPError:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                return ''
    return ''

# ─────────────────────── HTML helpers ────────────────────────────
def extract_pre_blocks(html: str) -> List[str]:
    soup = BeautifulSoup(html, 'html.parser')
    pres = soup.find_all('pre')
    return [p.get_text() for p in pres if p.get_text().strip()]

# ─────────────────────── MODS parsing ────────────────────────────
def parse_mods_cong_members(mods_xml: str) -> List[Dict[str, str]]:
    out : List[Dict[str, str]] = []
    try:
        root = ET.fromstring(mods_xml)
    except ET.ParseError:
        return out
    ns = {'m': 'http://www.loc.gov/mods/v3'}
    for cm in root.findall('.//m:congMember', ns):
        if (cm.get('role','') or '').upper() != 'SPEAKING':
            continue
        parsed_elem = cm.find('m:name[@type="parsed"]', ns)
        if parsed_elem is None or not parsed_elem.text:
            continue
        fnf_elem = cm.find('m:name[@type="authority-fnf"]', ns)
        full_name = fnf_elem.text.strip() if (fnf_elem is not None and fnf_elem.text) else ''
        out.append({
            'parsed_name'    : parsed_elem.text.strip(),
            'bioGuideId'     : (cm.get('bioGuideId','') or '').strip(),
            'full_name'      : full_name,
            'party'          : (cm.get('party','') or '').strip(),
            'state'          : (cm.get('state','') or '').strip(),
            'speaker_chamber': (cm.get('chamber','') or '').strip(),  # H/S for the member
        })
    return out

def parse_mods_title(mods_xml: str) -> str:
    try:
        root = ET.fromstring(mods_xml)
    except ET.ParseError:
        return ''
    ns = {'m': 'http://www.loc.gov/mods/v3'}
    el = root.find('.//m:titleInfo/m:title', ns)
    return el.text.strip() if (el is not None and el.text) else ''

# ─────────────────────── Per-day scrape ──────────────────────────
def _fetch_one_granule(pkg: str, gid: str, target_day: date) -> Optional[Dict[str, Any]]:
    """
    Worker: verify same-day granule, fetch HTML & MODS, compute title and section.
    Returns a dict with keys: gid, pkg, html, mods_xml, title, section  (or None to skip).
    """
    # Confirm same-day record
    summary = get_granule_summary(pkg, gid)
    dstr = (summary.get('dateIssued','') or '').strip()
    try:
        rec_date = datetime.fromisoformat(dstr).date()
    except Exception:
        return None
    if rec_date != target_day:
        return None

    # Only House/Senate pages
    section = 'house' if 'PgH' in gid else 'senate' if 'PgS' in gid else None
    if section is None:
        return None

    html = get_htm(pkg, gid)
    mods_xml = get_mods(pkg, gid)
    title = parse_mods_title(mods_xml) if mods_xml else ''
    return {'gid': gid, 'pkg': pkg, 'html': html, 'mods_xml': mods_xml, 'title': title, 'section': section}

def scrape_day(day: date, output_dir: str, parallel: bool, workers: int) -> Tuple[str, str, Dict[str, str]]:
    """
    Scrape a single day.
    Returns (html_dir, xml_dir, title_map_by_granule_id)
    """
    y, m, d = day.year, day.month, day.day
    day_str = day.isoformat()
    logging.info(f"=== Starting scrape for {day_str} ===")

    day_dir  = os.path.join(output_dir, f"{y}", f"{m:02}", f"{d:02}")
    html_dir = os.path.join(day_dir, f"raw_html_{day_str}")
    xml_dir  = os.path.join(day_dir, f"raw_xml_{day_str}")
    os.makedirs(html_dir, exist_ok=True)
    os.makedirs(xml_dir,  exist_ok=True)

    query = (
        "collection:CREC AND docClass:CREC AND (section:House OR section:Senate) "
        f"AND publishdate:range({day_str},{day_str})"
    )

    # Gather unique package IDs for the date
    package_ids : Set[str] = set()
    offset = None
    while True:
        try:
            data = get_search_results(query, PAGE_SIZE, offset)
        except Exception as e:
            logging.error(f"Search for {day_str} failed: {e}")
            return html_dir, xml_dir, {}
        for res in data.get('results', []):
            pkg = res.get('packageId')
            if pkg:
                package_ids.add(pkg)
        offset = data.get('nextOffsetMark')
        if not offset:
            break

    if not package_ids:
        logging.info(f"No packages for {day_str} — skipping.")
        return html_dir, xml_dir, {}

    # Collect candidate (pkg, gid) pairs first
    pairs : List[Tuple[str,str]] = []
    for pkg in package_ids:
        granules = get_granules(pkg)
        for g in granules:
            gid = g.get('granuleId')
            if not gid:
                continue
            # Filter early to H/S for fewer tasks
            if ('PgH' not in gid) and ('PgS' not in gid):
                continue
            pairs.append((pkg, gid))

    results : List[Dict[str, Any]] = []

    if parallel and workers > 1:
        logging.info(f"{day_str}: Fetching {len(pairs)} granules in parallel (workers={workers})…")
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {pool.submit(_fetch_one_granule, pkg, gid, day): (pkg, gid) for (pkg, gid) in pairs}
            for fut in as_completed(futs):
                try:
                    item = fut.result()
                except Exception as e:
                    logging.warning(f"Granule fetch failed: {e}")
                    continue
                if item:
                    results.append(item)
    else:
        logging.info(f"{day_str}: Fetching {len(pairs)} granules sequentially…")
        for pkg, gid in pairs:
            item = _fetch_one_granule(pkg, gid, day)
            if item:
                results.append(item)

    if not results:
        logging.info(f"{day_str}: No same-day House/Senate granules after filtering.")
        return html_dir, xml_dir, {}

    # Write files with deterministic numbering (sorted by granuleId)
    results.sort(key=lambda r: r['gid'])
    html_counter = 1
    xml_counter  = 1
    title_map : Dict[str, str] = {}

    for item in results:
        gid      = item['gid']
        html     = item['html']
        mods_xml = item['mods_xml']
        title    = item['title']

        if html:
            hname = f"{html_counter}-{gid}.html"
            with open(os.path.join(html_dir, hname), 'w', encoding='utf-8') as f:
                f.write(html)
            html_counter += 1

        if mods_xml:
            xname = f"{xml_counter}-{gid}.xml"
            with open(os.path.join(xml_dir, xname), 'w', encoding='utf-8') as f:
                f.write(mods_xml)
            xml_counter += 1
            title_map[gid] = title or ''

    logging.info(f"=== Finished scrape for {day_str} ===")
    return html_dir, xml_dir, title_map

# ─────────────────────── Per-day parse ───────────────────────────
def parse_day(day: date, html_dir: str, xml_dir: str, title_map: Dict[str,str]) -> None:
    """
    Build parsed_speeches_<date>.csv for a day from html/xml dirs.
    """
    import speaker_scraper  # local module you already use

    day_str  = day.isoformat()
    day_root = os.path.dirname(html_dir)  # YYYY/MM/DD
    out_csv  = os.path.join(day_root, f"parsed_speeches_{day_str}.csv")

    if not (os.path.isdir(html_dir) and os.path.isdir(xml_dir)):
        logging.warning(f"{day_str}: Missing raw_html or raw_xml directory, skipping parse.")
        return

    # 1) Build combined <pre> text with filename markers
    def sort_key(fname: str):
        try:
            return int(fname.split('-', 1)[0])  # NNN-*.html
        except Exception:
            return float('inf')

    html_files = sorted([f for f in os.listdir(html_dir) if f.endswith('.html')], key=sort_key)

    full_text = ""
    filename_by_offset : List[Tuple[int, str]] = []  # (start_index, filename)
    for fname in html_files:
        with open(os.path.join(html_dir, fname), encoding='utf-8') as f:
            html = f.read()
        blocks = extract_pre_blocks(html)
        if not blocks:
            continue
        marker = f"\n===FILE: {fname}===\n"
        filename_by_offset.append((len(full_text), fname))
        for block in blocks:
            full_text += marker + block + "\n\n"

    if not full_text.strip():
        logging.info(f"{day_str}: No valid <pre> content found, skipping parse.")
        return

    # 2) Load speaker metadata from all MODS
    speaker_meta : Dict[str, Dict[str,str]] = {}
    for xml_file in os.listdir(xml_dir):
        if not xml_file.endswith('.xml'):
            continue
        with open(os.path.join(xml_dir, xml_file), encoding='utf-8') as xf:
            mods_xml = xf.read()
        for m in parse_mods_cong_members(mods_xml):
            speaker_meta[m['parsed_name']] = m  # last one wins if duplicates

    # 3) Run speaker scraper
    speeches = list(speaker_scraper.scrape(full_text))
    if not speeches:
        logging.info(f"{day_str}: No speeches detected, skipping parse.")
        return

    # 4) Helpers to map offsets → file / chamber / granule
    def file_for_offset(start_idx: int) -> Optional[str]:
        last_file = None
        for off, fname in filename_by_offset:
            if off > start_idx:
                break
            last_file = fname
        return last_file

    def chamber_from_file(fname: Optional[str]) -> str:
        if not fname:
            return ''
        if 'PgH' in fname:
            return 'H'
        if 'PgS' in fname:
            return 'S'
        return ''

    # 5) Write CSV
    with open(out_csv, 'w', newline='', encoding='utf-8') as out:
        fieldnames = [
            'date', 'granule_id', 'title', 'speaker', 'text', 'bioGuideId',
            'full_name', 'party', 'state', 'gender',
            'chamber', 'speaker_chamber'
        ]
        w = csv.DictWriter(out, fieldnames=fieldnames)
        w.writeheader()

        cursor = 0
        for speaker, text in speeches:
            start_idx = full_text.find(text, cursor)
            if start_idx == -1:
                start_idx = cursor  # conservative fallback
            src_file = file_for_offset(start_idx)
            chamber  = chamber_from_file(src_file)
            cursor   = start_idx + len(text)

            granule_id = ''
            if src_file and '-' in src_file:
                granule_id = src_file.split('-', 1)[1].rsplit('.html', 1)[0]

            title = title_map.get(granule_id, '')

            meta = speaker_meta.get(speaker, {})
            gender = ''
            if speaker.startswith(('Ms.', 'Mrs.')):
                gender = 'F'
            elif speaker.startswith('Mr.'):
                gender = 'M'

            w.writerow({
                'date'            : day_str,
                'granule_id'      : granule_id,
                'title'           : title,
                'speaker'         : speaker,
                'text'            : text,
                'bioGuideId'      : meta.get('bioGuideId',''),
                'full_name'       : meta.get('full_name',''),
                'party'           : meta.get('party',''),
                'state'           : meta.get('state',''),
                'gender'          : gender,
                'chamber'         : chamber,
                'speaker_chamber' : meta.get('speaker_chamber',''),
            })

    logging.info(f"{day_str}: Saved {len(speeches)} parsed speeches → {out_csv}")

# ─────────────────────── Main driver ─────────────────────────────
def main(output_folder: str,
         years: int,
         api_keys: List[str],
         year_start: Optional[int],
         per_key_hourly_limit: Optional[int],
         parallel: bool,
         workers: int):
    global _api_keys, RATE_INTERVAL
    _api_keys = api_keys
    if not _api_keys:
        raise SystemExit("At least one API key is required.")

    # Optional pacing: if you know the per-key hourly cap, spread requests
    if per_key_hourly_limit and per_key_hourly_limit > 0:
        aggregate = per_key_hourly_limit * len(_api_keys)
        RATE_INTERVAL = 3600.0 / aggregate
        logging.info(f"Throttle enabled: ~{aggregate}/hr across {len(_api_keys)} keys → {RATE_INTERVAL:.3f}s between calls.")
    else:
        RATE_INTERVAL = None  # rely on 429 handling

    os.makedirs(output_folder, exist_ok=True)

    # Determine date range
    end_base  = date.today() - timedelta(days=1)
    end_date  = date(year_start, end_base.month, end_base.day) if year_start else end_base
    start_date = end_date.replace(year=end_date.year - years)

    # Walk dates newest→oldest
    days : List[date] = []
    cur = end_date
    while cur >= start_date:
        days.append(cur)
        cur -= timedelta(days=1)

    logging.info(f"Will process {len(days)} days. Parallel={'ON' if parallel else 'OFF'} (workers={workers if parallel else 1})")
    for day in days:
        html_dir, xml_dir, title_map = scrape_day(day, output_folder, parallel=parallel, workers=workers if parallel else 1)
        parse_day(day, html_dir, xml_dir, title_map)

# ─────────────────────── CLI ────────────────────────────────────
if __name__ == '__main__':

    p = argparse.ArgumentParser(description="CREC end-to-end scraper/parser (single script, optional parallelism)")
    p.add_argument('output_folder', help="Top-level output directory")
    p.add_argument('--years', type=int, default=2, help="How many years back to go")
    p.add_argument('--year-start', type=int, help="Optional starting year (e.g., 1995) for the end date's year")
    p.add_argument('--api-keys', required=True, help="Comma-separated list of GovInfo API keys")
    p.add_argument('--per-key-hourly-limit', type=int,
                   help="Optional per-key hourly limit to set a global throttle (e.g., 1000)")
    p.add_argument('--workers', type=int, default=8, help="Worker threads when --parallel is on")
    p.add_argument('--parallel', action=bool_action, default=True, help="Enable/disable parallel fetch (default: enabled)")
    args = p.parse_args()

    keys = [k.strip() for k in args.api_keys.split(',') if k.strip()]
    try:
        main(
            output_folder=args.output_folder,
            years=args.years,
            api_keys=keys,
            year_start=args.year_start,
            per_key_hourly_limit=args.per_key_hourly_limit,
            parallel=args.parallel,
            workers=args.workers,
        )
    except KeyboardInterrupt:
        logging.info("Interrupted by user — exiting.")
        sys.exit(0)
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        sys.exit(1)
