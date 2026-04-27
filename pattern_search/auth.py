"""Authentication, API session and account-level API helpers."""

import json
import os
import select
import sys
import threading
import time
from time import sleep
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter

from pattern_search import state
from pattern_search.paths import _CRED_DIR


# === Runtime globals ===
brain_api_url = "https://api.worldquantbrain.com"
headers = {}
http = requests.Session()
http.mount("https://", HTTPAdapter(pool_connections=20, pool_maxsize=20))

token_lock = threading.Lock()
force_reauth = threading.Event()
retry_requested = threading.Event()


 # === Authentication: expiry probe ===
def check_token_timeout(token: str) -> float:
    for _ in range(3):
        try:
            url = brain_api_url + "/authentication"
            headers = {"Cookie": f"t={token}"}
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            return resp.json()["token"]["expiry"]
        except Exception:
            time.sleep(1)
    return 0

 # === Payments API ===
# === Payments API ===
def get_total_base_payment_and_yesterday_increment():
    """
    Returns:
        total (float): Total lifetime base payment
        yesterday (float): Total base payment for yesterday only
    """
    token = open(os.path.join(_CRED_DIR, "brain_token.txt")).read().strip()
    headers = {
        "Cookie": f"t={token}",
        "Accept": "application/json;version=3.0",
    }
    url = "https://api.worldquantbrain.com/users/self/activities/base-payment"
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    total = data["total"]["value"]
    yesterday = data.get("yesterday", {}).get("value", 0.0)

    return total, yesterday

# === Consultant Summary API ===
def get_consultant_summary_current():
    """
    Fetch current-quarter consultant summary and return a compact dict with:
    alphaCount, pyramidCount, combinedAlphaPerformance, quarter name.
    """
    token = open(os.path.join(_CRED_DIR, "brain_token.txt")).read().strip()
    headers = {
        "Cookie": f"t={token}",
        "Accept": "application/json;version=3.0",
    }
    url = "https://api.worldquantbrain.com/users/self/consultant/summary"
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json() or {}

    # Prefer 'performance.current' for live quarter stats; fall back to latest history if missing
    perf = (data.get("performance") or {})
    current = (perf.get("current") or {})
    quarter = (current.get("quarter") or {}).get("name") or (perf.get("currentQuarter") or {}).get("name") or "N/A"

    alpha_count = current.get("alphaCount")
    pyramid_count = current.get("pyramidCount")
    combined_alpha_perf = current.get("combinedAlphaPerformance")

    # Fallback: use most recent non-empty entry from history
    if alpha_count is None or pyramid_count is None or combined_alpha_perf is None:
        for h in (perf.get("history") or []):
            if h.get("alphaCount") is not None and h.get("pyramidCount") is not None and h.get("combinedAlphaPerformance") is not None:
                alpha_count = alpha_count if alpha_count is not None else h.get("alphaCount")
                pyramid_count = pyramid_count if pyramid_count is not None else h.get("pyramidCount")
                combined_alpha_perf = combined_alpha_perf if combined_alpha_perf is not None else h.get("combinedAlphaPerformance")
                if not quarter:
                    q = h.get("quarter") or {}
                    quarter = q.get("name") or quarter
                if alpha_count is not None and pyramid_count is not None and combined_alpha_perf is not None:
                    break

    return {
        "quarter": quarter,
        "alphaCount": int(alpha_count) if isinstance(alpha_count, (int, float)) and alpha_count is not None else None,
        "pyramidCount": int(pyramid_count) if isinstance(pyramid_count, (int, float)) and pyramid_count is not None else None,
        "combinedAlphaPerformance": float(combined_alpha_perf) if combined_alpha_perf is not None else None,
    }


 # === Authentication: token lifecycle (READ-ONLY) ===
# This process never re-authenticates by itself. Run
# `python credentials/token_refresh.py --loop` in a separate terminal — it
# refreshes the JWT on a 3h55m schedule. The miner just reads brain_token.txt
# and pauses if it ever finds the file missing or the token expired.
TOKEN_WAIT_POLL_SECONDS = 30


def get_valid_token():
    global headers
    with token_lock:
        token_path = os.path.join(_CRED_DIR, "brain_token.txt")
        while True:
            try:
                with open(token_path, "r") as f:
                    token = f.read().strip()
            except FileNotFoundError:
                state.pause("no brain_token.txt — run token_refresh.py --loop")
                print(f"⏸️ No token at {token_path}. "
                      f"Waiting {TOKEN_WAIT_POLL_SECONDS}s for the refresher daemon…")
                time.sleep(TOKEN_WAIT_POLL_SECONDS)
                continue

            expiry = check_token_timeout(token)
            if expiry > 60:
                headers = {"Cookie": f"t={token}"}
                state.resume("token valid")
                return token

            # Expired or unverifiable. Pause and wait for the refresher to write a fresh token.
            state.pause(f"token expired (expiry={expiry}s) — waiting for refresher daemon")
            print(f"⏸️ Token expired/invalid (expiry={expiry}s). "
                  f"Waiting {TOKEN_WAIT_POLL_SECONDS}s for token_refresh.py --loop to renew it…")
            time.sleep(TOKEN_WAIT_POLL_SECONDS)


 # === Authentication: Persona flow ===
def authenticate_with_persona(auto_poll_interval=5, max_retries=30):
    global headers

    sess = requests.Session()

    with open(os.path.join(_CRED_DIR, "pw")) as f:
        sess.auth = tuple(json.load(f))

    while True:
        response = sess.post(f"{brain_api_url}/authentication")

        if response.status_code == 201:
            break

        if response.status_code == 401 and response.headers.get("WWW-Authenticate") == "persona":
            biometric_url = urljoin(response.url, response.headers["Location"])
            msg = (
                "🔐 Persona Biometric Verification required.\n"
                f"Open this link:\n{biometric_url}"
            )
            print(msg)
            state.send_notification(msg)

            for retry in range(max_retries):
                print(f"🔄 Polling biometric attempt {retry + 1}/{max_retries}...")
                response = sess.post(biometric_url)

                if response.status_code == 201:
                    print("✅ Biometric verified.")
                    break

                sleep(auto_poll_interval)

            else:
                print("⏸️ Biometric not verified after 30 attempts.")
                print("🔁 Waiting for /retry or manual Enter...")
                state.send_notification("❗ Biometric failed. Press Enter to retry.")
                retry_requested.clear()
                print("🔁 Waiting for manual Enter... (press Enter to manually retry)")
                while not retry_requested.is_set():
                    try:
                        if sys.stdin in select.select([sys.stdin], [], [], 1)[0]:
                            input()  # Enter pressed
                            break
                    except KeyboardInterrupt:
                        break
                print("🔁 Retry triggered.")
                continue  # Re-loop
            break

        else:
            print(f"❌ Auth failed: {response.status_code} - {response.text}")
            exit(1)

    token = sess.cookies.get("t")
    headers = {"Cookie": f"t={token}"}
    with open(os.path.join(_CRED_DIR, "brain_token.txt"), "w") as f:
        f.write(token)
    print("🔑 New token saved.")
    state.send_notification("Successfully verified ✅")
    return token
