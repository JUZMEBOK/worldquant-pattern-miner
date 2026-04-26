"""Simulation lifecycle: start, poll, fetch."""

import concurrent.futures
import random
import time
from datetime import datetime

import requests

from pattern_search import auth, state
from pattern_search.config import (
    FETCH_BACKOFF,
    FETCH_MAX_ATTEMPTS,
    RATE_LIMIT_BACKOFF,
    SIMULATION_CONFIG,
    DEBUG_API,
)
from pattern_search.ratelimit import (
    _recent_enq_allows,
    _recent_enq_mark,
    _start_bucket,
)


# === Simulation helpers ===
 # === Simulation API: start ===
def start_simulation(expression: str, config=SIMULATION_CONFIG):
    if state.is_paused():
        print(f"⏸️ Paused: deferring start; re-queueing expression.")
        try:
            with state.datafields_lock:
                state.datafields.appendleft(expression)
        except Exception:
            # Fallback: at least don't lose the expression; log it.
            print("⚠️ Could not re-queue expression; keeping it unlaunched.")
        return None

    max_attempts = 5

    # Helper: Retry-After/expo backoff with jitter
    def _compute_backoff_seconds(resp, attempt_idx: int, base: float = RATE_LIMIT_BACKOFF, cap: float = 60.0) -> float:
        """Honor Retry-After (seconds or HTTP-date), else use exponential with jitter."""
        # Parse Retry-After if present
        try:
            ra_hdr = getattr(resp, "headers", {}).get("Retry-After", "")
        except Exception:
            ra_hdr = ""
        ra_sec = 0
        if ra_hdr:
            try:
                ra_sec = max(0, int(str(ra_hdr).strip()))
            except Exception:
                try:
                    from email.utils import parsedate_to_datetime
                    dt = parsedate_to_datetime(str(ra_hdr))
                    if dt:
                        ra_sec = max(0, int((dt - datetime.utcnow()).total_seconds()))
                except Exception:
                    ra_sec = 0
        expo = base * (2 ** max(0, attempt_idx - 1))
        raw = max(ra_sec, expo)
        jitter = raw * (0.9 + 0.2 * random.random())
        return min(jitter, cap)

    for attempt in range(1, max_attempts + 1):
        try:
            # Token bucket pacing to reduce correlation of start requests across threads
            _tb_delay = _start_bucket.take()
            if _tb_delay > 0:
                time.sleep(_tb_delay * (0.9 + 0.2 * random.random()))
            response = requests.post(
                f"{auth.brain_api_url}/simulations",
                headers=auth.headers,
                json={
                    'type': 'REGULAR',
                    'settings': {
                        'instrumentType': 'EQUITY',
                        'region': config['region'],
                        'universe': config['universe'],
                        'delay': config['delay'],
                        'decay': config['decay'],
                        'neutralization': config['neutralization'],
                        'truncation': config['truncation'],
                        'pasteurization': 'ON',
                        'testPeriod': 'P2Y0M',
                        'unitHandling': 'VERIFY',
                        'nanHandling': 'OFF',
                        'language': 'FASTEXPR',
                        'visualization': False,
                    },
                    'regular': expression,
                },
                timeout=10
            )

            if response.status_code == 201:
                # tiny stagger to de-synchronize subsequent polls
                time.sleep(0.05 + 0.15 * random.random())
                status_url = response.headers['Location']
                sim_id = status_url.strip("/").split("/")[-1]
                return {
                    "expression": expression,
                    "status_url": status_url,
                    "id": sim_id
                }
            elif response.status_code == 401:
                print("🔄 Token possibly expired. Refreshing and retrying start_simulation...")
                auth.get_valid_token()
                continue
            elif response.status_code in (429, 502, 503, 504):
                delay = _compute_backoff_seconds(response, attempt)
                print(f"⚠️ Rate limited / transient ({response.status_code}) starting simulation. Backing off {delay:.1f} seconds.")
                time.sleep(delay)
                continue
            else:
                # Print a useful, detailed error message instead of only the status code
                def _extract_api_error(resp):
                    try:
                        js = resp.json()
                    except Exception:
                        js = None
                    if isinstance(js, dict):
                        # Common fields the Brain API uses for error payloads
                        for k in ("message", "error", "detail", "errors", "reason"):
                            if k in js and js[k]:
                                return js[k]
                        try:
                            import json as _json
                            return _json.dumps(js, ensure_ascii=False)
                        except Exception:
                            pass
                    return (resp.text or "<empty error body>").strip()

                err_body = _extract_api_error(response)
                print(
                    f"❌ Failed to start simulation ({response.status_code}). "
                    f"Server said: {err_body}"
                )

                if 400 <= response.status_code < 500:
                    return None

                delay = _compute_backoff_seconds(response, attempt)
                time.sleep(delay)
                continue
        except (requests.ConnectionError, requests.Timeout) as e:
            delay = _compute_backoff_seconds(resp=type('obj', (), {'headers': {}})(), attempt_idx=attempt)
            print(f"⚠️ Network error starting simulation for {expression}: {e}. Retrying ({attempt}/{max_attempts}) after {delay:.1f} seconds…")
            auth.get_valid_token()
            time.sleep(delay)
            continue
    # Set a short cooldown to avoid hammering the same expression immediately
    try:
        state.REQUEUE_COOLDOWN[expression] = time.time() + state.REQUEUE_COOLDOWN_SECONDS
    except Exception:
        pass
    try:
        with state.datafields_lock:
            if _recent_enq_allows(expression):
                state.datafields.appendleft(expression)
                _recent_enq_mark(expression)
                print(
                    f"❌ Failed to start after {max_attempts} attempts. Re-queued expression for later (cooldown {state.REQUEUE_COOLDOWN_SECONDS}s).")
            else:
                print(
                    f"⏳ Failed to start after {max_attempts} attempts. Expression was recently enqueued; skipping duplicate re-queue (cooldown {state.REQUEUE_COOLDOWN_SECONDS}s).")
    except Exception:
        print(f"❌ Failed to start after {max_attempts} attempts and could not re-queue expression.")
    return None

# === Simulation API: poll ===
def poll_simulation(sim, log_progress=True):
    max_attempts = 5

    # Helper: Retry-After/expo backoff with jitter for polling
    def _compute_backoff_seconds_poll(resp, attempt_idx: int, base: float = RATE_LIMIT_BACKOFF, cap: float = 60.0) -> float:
        """Honor Retry-After (seconds or HTTP-date), else exponential with jitter."""
        try:
            ra_hdr = getattr(resp, "headers", {}).get("Retry-After", "")
        except Exception:
            ra_hdr = ""
        ra_sec = 0
        if ra_hdr:
            try:
                ra_sec = max(0, int(str(ra_hdr).strip()))
            except Exception:
                try:
                    from email.utils import parsedate_to_datetime
                    dt = parsedate_to_datetime(str(ra_hdr))
                    if dt:
                        ra_sec = max(0, int((dt - datetime.utcnow()).total_seconds()))
                except Exception:
                    ra_sec = 0
        expo = base * (2 ** max(0, attempt_idx - 1))
        raw = max(ra_sec, expo)
        jitter = raw * (0.9 + 0.2 * random.random())
        return min(jitter, cap)

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(sim['status_url'], headers=auth.headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                status = data.get("status", "").upper()
                status_message = data

                # Completed / warning -> return success (persistence happens in main loop)
                if status in ("COMPLETE", "WARNING"):
                    if "alpha" not in data:
                        # Completion state reported but alpha object not attached yet; treat as in-progress
                        if log_progress:
                            sid = sim.get("id") or (sim.get("simulation", {}) or {}).get("id")
                            prev_s = state.LAST_STATUS.get(sid)
                            if prev_s != status:
                                print(f"[poll] {sid}: status={status} (awaiting alpha payload)")
                                state.LAST_STATUS[sid] = status
                        return {"ok": None, "status": status or "", "simulation": sim, "data": data}
                    return {"ok": True, "status": status, "simulation": sim, "data": data}

                # Error states
                if status in ("ERROR", "FAIL"):
                    print(f"❌ Simulation failed for {sim.get('expression','?')}. Status message: {status_message}")
                    return {"ok": False, "status": status, "simulation": sim, "error": data}

                # Any other/in-progress status -> only log if changed or progressed
                # (throttle to avoid spam, especially while paused)
                prog = None
                try:
                    prog = data.get("progress")
                except Exception:
                    prog = None

                if log_progress:
                    sid = sim.get("id") or (sim.get("simulation", {}) or {}).get("id")
                    changed = (state.LAST_STATUS.get(sid) != status) or (state.LAST_PROGRESS.get(sid) != prog)
                    if changed:
                        if prog is not None:
                            print(f"[poll] {sid}: status={status} | progress={prog}")
                        else:
                            print(f"[poll] {sid}: status={status}")
                        state.LAST_STATUS[sid] = status
                        state.LAST_PROGRESS[sid] = prog

                return {"ok": None, "status": status or "", "simulation": sim, "data": data}

            elif response.status_code == 401:
                print(f"🔄 Token expired during polling {sim.get('expression','?')}. Refreshing...")
                auth.get_valid_token()
                continue
            elif response.status_code in (429, 502, 503, 504):
                delay = _compute_backoff_seconds_poll(response, attempt)
                print(f"⚠️ Poll {sim.get('id') or sim.get('expression','?')}: {response.status_code} — backing off {delay:.1f}s (attempt {attempt}/{max_attempts}).")
                time.sleep(delay)
                continue
            else:
                print(f"⚠️ Unexpected response while polling simulation {sim.get('expression','?')}: {response.status_code}. Response: {response.text}")
                return {"ok": None, "status": "", "simulation": sim}
        except (requests.ConnectionError, requests.Timeout) as e:
            delay = _compute_backoff_seconds_poll(resp=type('obj', (), {'headers': {}})(), attempt_idx=attempt)
            print(f"⚠️ Network error during polling for {sim.get('expression','?')}: {e}. Retrying ({attempt}/{max_attempts}) after {delay:.1f}s…")
            auth.get_valid_token()
            time.sleep(delay)
            continue
        except Exception as e:
            print(f"❌ Unknown error during polling for {sim.get('expression','?')}: {e}")
            return {"ok": None, "status": "", "simulation": sim}
    print(f"❌ Maximum retry attempts reached for simulation {sim.get('expression','?')}.")
    return {"ok": None, "status": "", "simulation": sim}


def fetch_alpha_final(alpha_id: str, request_timeout: float = 8.0) -> dict:
    """Fetch the full alpha and wait for all required checks to finish or fail."""
    token = auth.get_valid_token()
    headers = {"Cookie": f"t={token}"}
    alpha_url = f"https://api.worldquantbrain.com/alphas/{alpha_id}"
    # Explicit timeout stops threads from hanging indefinitely.
    response = requests.get(alpha_url, headers=headers, timeout=request_timeout)
    response.raise_for_status()
    return response.json()


def safe_fetch_alpha(alpha_id, timeout=10, backoff=5, request_timeout=None):
    """
    Keep fetching the alpha data until successful with hard timeouts.
    Uses a thread to enforce a wall-clock timeout while also setting
    per-request network timeouts to prevent hangs.
    """
    attempt = 1
    if request_timeout is None:
        # keep request shorter than future.result timeout
        request_timeout = max(1, timeout - 1)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    alpha = None
    try:
        attempt = 0
        while attempt < FETCH_MAX_ATTEMPTS:
            attempt += 1
            try:
                resp = auth.http.get(auth.brain_api_url + f"/alphas/{alpha_id}", headers=auth.headers, timeout=(3, 15))
                resp.raise_for_status()
                alpha = resp.json()
                if DEBUG_API:
                    print(f"🐛 Debug API Response for alpha_id={alpha_id}: {alpha}")
                else:
                    print(f"🐛 Alpha {alpha_id} fetched.")
                return alpha
            except (requests.ConnectionError, requests.Timeout):
                print(f"⏳ Timeout fetching alpha {alpha_id} (attempt {attempt})")
                time.sleep(FETCH_BACKOFF + random.random())
                continue
        else:
            print(f"❌ Gave up fetching alpha {alpha_id} after {FETCH_MAX_ATTEMPTS} attempts. Skipping for now.")
            return None
    finally:
        # Do NOT wait for possibly-stuck tasks
        executor.shutdown(wait=False, cancel_futures=True)
