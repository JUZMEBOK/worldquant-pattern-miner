import requests
import pandas as pd
import time
import os
import json
import shutil
import logging
import threading
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from queue import Queue

# ---------------------- Logging Setup ----------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("RegionalMaster")
logging.getLogger("urllib3").setLevel(logging.WARNING)

# ---------------------- User Configuration ----------------------
USER_CONFIG = {
    # Region to run: "USA", "JPN", "EUR", "CHN", "ASI", "IND", "GLB"
    "REGION": "USA",
    
    # Delays to process: [1] or [0] or [1, 0]
    "DELAYS": [1, 0],
    
    # Data Types: ["VECTOR", "MATRIX"]
    "TYPES": ["MATRIX", "VECTOR"]
}

# ---------------------- Constants ----------------------
BRAIN_API_URL = "https://api.worldquantbrain.com"
TOKEN_PATH = os.getenv("BRAIN_TOKEN_PATH", "../credentials/brain_token.txt")

REGION_UNIVERSE = {
    "USA": "TOP3000",
    "GLB": "TOP3000",
    "EUR": "TOP2500",
    "ASI": "MINVOL1M",
    "CHN": "TOP2000U",
    "JPN": "TOP1600",
    "IND": "TOP500"
}



# ---------------------- Session & Auth ----------------------
def make_session():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    return s

SESSION = make_session()

_TOKEN_LOCK = threading.Lock()

def _sanitize_token(tok):
    tok = (tok or "").strip()
    if tok.lower().startswith("t="):
        tok = tok.split("=", 1)[1].strip()
    if (tok.startswith('"') and tok.endswith('"')) or (tok.startswith("'") and tok.endswith("'")):
        tok = tok[1:-1]
    return tok

def load_token():
    """Always read the freshest token from TOKEN_PATH (file is rotated periodically)."""
    with _TOKEN_LOCK:
        try:
            with open(TOKEN_PATH, "r", encoding="utf-8") as f:
                return _sanitize_token(f.read())
        except Exception:
            return ""

def _wait_for_token_refresh(old_token):
    """Block until token file contains a different token. Simple poll."""
    try:
        new_token = load_token()
        if new_token and new_token != old_token:
            return new_token
    except Exception:
        pass
    logger.warning("Token expired. Waiting for token file refresh...")
    started = time.time()
    while True:
        time.sleep(3)
        try:
            new_token = load_token()
            if new_token and new_token != old_token:
                logger.info(f"Resumed after {time.time() - started:.0f}s")
                return new_token
        except Exception:
            pass

def get_headers(token=None):
    tok = token if token is not None else load_token()
    return {"Cookie": f"t={_sanitize_token(tok)}"}

def GET(url, timeout=(5, 10)):
    """GET with indefinite token-refresh wait on 401/403 (doesn't count against retries)."""
    token = load_token()
    attempts = 3
    i = 0
    while i < attempts:
        try:
            resp = SESSION.get(url, headers=get_headers(token), timeout=timeout)
            if resp.status_code in (401, 403):
                token = _wait_for_token_refresh(token)
                continue  # retry with fresh token, don't consume attempt
            return resp
        except Exception as e:
            logger.warning(f"Request failed: {url} | {e}")
            time.sleep(1)
            i += 1
    return None

def expand_dict_columns(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, dict)).all():
            expanded = df[col].apply(pd.Series)
            expanded.columns = [f"{col}_{subcol}" for subcol in expanded.columns]
            df = pd.concat([df.drop(columns=[col]), expanded], axis=1)
    return df

# ---------------------- Logic: URL Builders ----------------------
def _build_url(instrument_type, region, delay, universe, data_type, category=None, user_count=None, limit=50, offset=0):
    base = (
        f"{BRAIN_API_URL}/data-fields?"
        f"instrumentType={instrument_type}"
        f"&region={region}&delay={delay}"
        f"&universe={universe}"
    )
    if data_type: base += f"&type={data_type}"
    if category:  base += f"&category={category}"
    if user_count is not None: base += f"&userCount={user_count}"
    
    base += f"&limit={limit}&offset={offset}"
    return base

def _probe_count(url_params):
    """Probes the count for a given setup."""
    # Temporarily set limit=1 for probing
    probe_url = _build_url(**{**url_params, "limit": 1, "offset": 0})
    resp = GET(probe_url)
    if resp and resp.status_code == 200:
        return resp.json().get("count", 0)
    return -1 # Error or unknown

# ---------------------- Logic: Fetchers ----------------------

def fetch_linear(url_params, count_estimate=0):
    """Linearly fetch pages."""
    rows = []
    page_size = 50
    offset = 0
    
    # If we have a good estimate, we can log progress
    total_pages = (count_estimate + page_size - 1) // page_size if count_estimate > 0 else "?"
    
    logger.info(f"Starting Linear Fetch: ~{count_estimate} items...")
    
    while True:
        if offset % (page_size * 10) == 0:
            logger.info(f"   Fetching offset {offset}...")
            
        url = _build_url(**{**url_params, "limit": page_size, "offset": offset})
        resp = GET(url)
        if not resp: break
        
        try:
            data = resp.json()
            page = data.get("results", []) if isinstance(data, dict) else []
            if not page: break
            
            rows.extend([r for r in page if isinstance(r, dict)])
            if len(page) < page_size: break
            
            offset += page_size
        except Exception:
            break
            
    return rows

def fetch_user_count_loop(url_params):
    """Iterate userCount 0..1000."""
    logger.info("Starting UserCount Loop (0..1000)...")
    all_rows = []
    
    # This is "one worker" overall script, but we can do this loop linearly 
    # since the user requested "one worker so we dont hit api limit".
    # We won't use ThreadPool here to be ultra-safe/compliant with "one worker".
    
    for uc in range(1001):
        # We can probe first to save time
        params = {**url_params, "user_count": uc}
        cnt = _probe_count(params)
        
        if cnt == 0:
            # logger.info(f"   userCount={uc}: 0 items")
            continue
            
        # If cnt > 0, fetch pages for this userCount
        if cnt > 0:
             logger.info(f"   userCount={uc}: Found {cnt} items. Fetching...")
        
        # Reuse linear fetch logic but constrained to this userCount
        rows = fetch_linear(params, count_estimate=cnt)
        if rows:
            all_rows.extend(rows)
            
    return all_rows

# ---------------------- Master Orchestra ----------------------

# ---------------------- Logic: Helpers ----------------------

_pyramid_cache = None

def fetch_categories_from_api(target_region, target_delay):
    """
    Fetch active categories from user activities for specific Region/Delay.
    Returns list of category IDs.
    """
    global _pyramid_cache
    
    # 1. Fetch from API if not cached
    if _pyramid_cache is None:
        # Use future-dated range to catch active stuff
        url = f"{BRAIN_API_URL}/users/self/activities/pyramid-alphas?startDate=2026-01-01&endDate=2026-04-01"
        logger.info(f"Fetching active categories from: {url}")
        resp = GET(url)
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                if isinstance(data, list):
                    _pyramid_cache = data
                elif isinstance(data, dict):
                    # Handle both 'pyramids' and 'results' wrapper
                    _pyramid_cache = data.get("pyramids") or data.get("results") or []
                else:
                    _pyramid_cache = []
            except Exception as e:
                logger.error(f"Failed to parse pyramid response: {e}")
                _pyramid_cache = []
        else:
             logger.warning("Failed to fetch pyramid-alphas. No categories discovered.")
             return []

    # 2. Filter from cache
    active = []
    seen = set()
    
    target_dly_str = str(target_delay)
    
    for item in _pyramid_cache:
        # Parse item
        r = item.get("region")
        d = str(item.get("delay", ""))
        c = item.get("category", {}).get("id")
        
        if r == target_region and d == target_dly_str and c:
            if c not in seen:
                active.append(c)
                seen.add(c)
                
    if not active:
        logger.warning(f"No active categories found for {target_region}/Delay {target_delay}.")
        return []
        
    active.sort()
    return active

def archive_old_file(filepath):
    """Move file to archive/timestamp_filename."""
    if os.path.exists(filepath):
        dirname = os.path.dirname(filepath)
        filename = os.path.basename(filepath)
        archive_dir = os.path.join(dirname, "archive")
        os.makedirs(archive_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_name = f"{timestamp}_{filename}"
        dest = os.path.join(archive_dir, new_name)
        
        logger.info(f"Archiving old file: {filepath} -> {dest}")
        shutil.move(filepath, dest)

def process_variant(region, universe, delay, data_type, out_dir):
    """
    Process a single variant (e.g. VECTOR or MATRIX).
    Implements the Waterfall Logic.
    """
    logger.info(f"--- Processing Variant: {data_type} (Delay {delay}) ---")
    
    base_params = {
        "instrument_type": "EQUITY",
        "region": region,
        "delay": delay,
        "universe": universe,
        "data_type": data_type
    }
    
    # Step 1: Global Probe
    total_count = _probe_count(base_params)
    logger.info(f"Global Probe for {data_type}: Count = {total_count}")
    
    final_rows = []
    
    # Condition A: Count < 10000 (and valid)
    if 0 <= total_count < 10000:
        logger.info("Condition A: Count < 10000. Proceeding with Linear Fetch (No Category/UserCount filters).")
        final_rows = fetch_linear(base_params, count_estimate=total_count)
        
    else:
        # Condition B: Count >= 10000 -> Category Waterfall
        logger.info("Condition B: Count >= 10000. Proceeding with Category Filter.")
        
        # DYNAMIC FETCH: Get strict list of categories for this Region/Delay
        target_cats = fetch_categories_from_api(region, delay)
        logger.info(f"Targeting {len(target_cats)} categories: {target_cats}")
        
        for cat in target_cats:
            cat_params = {**base_params, "category": cat}
            cat_count = _probe_count(cat_params)
            
            if cat_count <= 0:
                continue
            
            logger.info(f"   Category '{cat}': Count = {cat_count}")
            
            if cat_count < 10000:
                # Condition B.1: Category fits
                logger.info(f"   -> Fetching Linear for category '{cat}'")
                rows = fetch_linear(cat_params, count_estimate=cat_count)
                final_rows.extend(rows)
            else:
                # Condition B.2: Category hits limit -> UserCount Waterfall
                logger.info(f"   -> HIT LIMIT (10k). Switching to UserCount Loop for category '{cat}'")
                rows = fetch_user_count_loop(cat_params)
                final_rows.extend(rows)

    # Save
    if final_rows:
        # User requested: Delay 1 -> "TYPE.csv", Delay 0 -> "TYPE0.csv"
        suffix = "" if delay == 1 else str(delay)
        filename = f"{data_type}{suffix}.csv"
        
        out_path = os.path.join(out_dir, filename)
        
        # Archiving
        archive_old_file(out_path)
        
        # Write
        os.makedirs(out_dir, exist_ok=True)
        df = pd.DataFrame(final_rows)
        if "id" in df.columns:
            df = df.drop_duplicates(subset="id", keep="last")
        df = expand_dict_columns(df)
        
        # Atomic write
        tmp_path = out_path + ".tmp"
        df.to_csv(tmp_path, index=False)
        os.replace(tmp_path, out_path)
        
        logger.info(f"Saved {len(df)} rows to {out_path}")
    else:
        logger.warning(f"No data found for {data_type} (Delay {delay}).")

def run_regional_master(region_arg=None):
    # 1. Determine Region (CLI arg > Env Var > User Config)
    target_region = region_arg or os.getenv("REGION") or USER_CONFIG.get("REGION", "USA")
    
    if target_region not in REGION_UNIVERSE:
        logger.error(f"Unknown Region: {target_region}. Supported: {list(REGION_UNIVERSE.keys())}")
        return

    universe = REGION_UNIVERSE[target_region]
    
    out_dir = f"./{target_region}" # Relative to runtime or absolute
    out_dir = os.path.abspath(out_dir)
    
    logger.info(f"=== Starting Regional Master for {target_region} (Univ: {universe}) ===")
    
    # 2. Process Variants (Delays and Types)
    delays = USER_CONFIG.get("DELAYS", [1, 0])
    types = USER_CONFIG.get("TYPES", ["VECTOR", "MATRIX"])
    
    logger.info(f"Config: Delays={delays}, Types={types}")
    
    for d in delays:
        for t in types:
            process_variant(target_region, universe, d, t, out_dir)
        
    logger.info("=== All Variants Completed ===")

if __name__ == "__main__":
    # Support CLI arg simply via env or trivial check
    import sys
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    run_regional_master(arg)
