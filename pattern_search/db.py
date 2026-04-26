"""File-based alpha persistence + async saver thread.

Alphas are appended verbatim as JSON Lines (one alpha per line) to
``data/alphas.jsonl``; failed-fetch IDs go to ``data/failed_alphas.jsonl``.
Lookups scan the files — fine for the volumes this miner produces. For
ad-hoc analytical queries see ``query.py`` at the project root (DuckDB).
"""

import json
import os
import threading
from datetime import datetime
from queue import Empty, Queue

from pattern_search import state
from pattern_search.expressions import is_expression_negated, negate_expression
from pattern_search.paths import _DATA_DIR
from pattern_search.ratelimit import _recent_enq_allows, _recent_enq_mark


# === Storage paths ===
os.makedirs(_DATA_DIR, exist_ok=True)
ALPHAS_FILE = os.path.join(_DATA_DIR, "alphas.jsonl")
FAILED_FILE = os.path.join(_DATA_DIR, "failed_alphas.jsonl")

_file_lock = threading.Lock()
_seen_ids: set[str] = set()


def _bootstrap_seen_ids() -> None:
    """Populate the in-memory id set from existing alphas.jsonl."""
    if not os.path.exists(ALPHAS_FILE):
        return
    try:
        with open(ALPHAS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                aid = obj.get("id")
                if aid:
                    _seen_ids.add(aid)
    except OSError as e:
        print(f"⚠️ Could not read {ALPHAS_FILE}: {e}")


_bootstrap_seen_ids()


def _append_jsonl(path: str, obj: dict) -> None:
    line = json.dumps(obj, default=str, ensure_ascii=False)
    with _file_lock:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")


def _read_jsonl(path: str):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def _rewrite_jsonl(path: str, objs) -> None:
    tmp = path + ".tmp"
    with _file_lock:
        with open(tmp, "w", encoding="utf-8") as f:
            for obj in objs:
                f.write(json.dumps(obj, default=str, ensure_ascii=False) + "\n")
        os.replace(tmp, path)


# === Public API used by the runner ===
def list_simulated_expressions(filters: dict) -> set[str]:
    """Return expressions of stored alphas whose settings match ``filters``.

    Keys in ``filters`` map to ``alpha["settings"]`` fields:
        neutralization, truncation, decay, delay, universe, region.
    """
    out: set[str] = set()
    for obj in _read_jsonl(ALPHAS_FILE):
        settings = obj.get("settings") or {}
        if all(settings.get(k) == v for k, v in filters.items()):
            expr = (obj.get("regular") or {}).get("code")
            if expr:
                out.add(expr)
    return out


def save_alpha(alpha: dict) -> None:
    """Persist a complete alpha record as one JSONL line; dedupe by id."""
    aid = alpha.get("id")
    if not aid:
        print("⚠️ save_alpha called without id; skipping.")
        return

    with _file_lock:
        already = aid in _seen_ids
        if not already:
            _seen_ids.add(aid)

    # Store the entire payload verbatim so nothing the API returns is dropped.
    # Only inject a sidecar metadata key under a leading underscore so it
    # cannot collide with any current or future API field.
    record = dict(alpha)
    record["_savedAt"] = datetime.utcnow().isoformat()

    if already:
        # Rewrite file replacing the prior record (rare — keeps file authoritative).
        existing = list(_read_jsonl(ALPHAS_FILE))
        replaced = False
        for i, obj in enumerate(existing):
            if obj.get("id") == aid:
                existing[i] = record
                replaced = True
                break
        if not replaced:
            existing.append(record)
        _rewrite_jsonl(ALPHAS_FILE, existing)
    else:
        _append_jsonl(ALPHAS_FILE, record)

    # === Negation policy: prioritized queue rerun (no immediate start) ===
    try:
        regular = alpha.get("regular") or {}
        code = regular.get("code")
        is_block = alpha.get("is") or {}
        is_sharpe = is_block.get("sharpe")

        if isinstance(is_sharpe, (int, float)) and is_sharpe < -1.1 and code and not is_expression_negated(code):
            neg_code = negate_expression(code)

            already_active = False
            with state.active_sims_lock:
                for _sid, _info in state.active_sims.items():
                    if isinstance(_info, dict) and _info.get("expression") == neg_code:
                        already_active = True
                        break

            if not already_active:
                with state.datafields_lock:
                    already_queued = any(item == neg_code for item in (state.datafields or []))

                if not already_queued:
                    with state.datafields_lock:
                        if _recent_enq_allows(neg_code):
                            state.datafields.appendleft(neg_code)
                            _recent_enq_mark(neg_code)
                        else:
                            print("⏳ Negated rerun recently enqueued; skipping for now.")
                    print(f"♻️ IS Sharpe {is_sharpe} < -1.1 for {aid} — queued NEGATED rerun at front.")
                else:
                    print("↩️ Negated rerun already queued; skipping duplicate.")
            else:
                print("⏩ Negated rerun already active; skipping enqueue.")
    except Exception as _neg_e:
        print(f"⚠️ Negation policy error: {_neg_e}")


def save_failed_alphas(pairs):
    """Append (datafield_id, alpha_id) pairs to failed_alphas.jsonl."""
    if not pairs:
        return
    try:
        for datafield_id, alpha_id in pairs:
            _append_jsonl(FAILED_FILE, {
                "datafield_id": datafield_id,
                "alpha_id": alpha_id,
                "ts": datetime.utcnow().isoformat(),
            })
        print(f"✅ Stored {len(pairs)} failed alpha fetches to {FAILED_FILE}")
    except Exception as e:
        print(f"❌ Error saving failed alphas: {e}")


def retry_failed_alphas():
    """Re-fetch failed alphas; on success enqueue for save and drop from failed file."""
    from pattern_search.simulation import safe_fetch_alpha

    if not os.path.exists(FAILED_FILE):
        return

    try:
        failed = list(_read_jsonl(FAILED_FILE))
        remaining = []
        for entry in failed:
            alpha_id = entry.get("alpha_id")
            if not alpha_id:
                continue

            if alpha_id in _seen_ids:
                print(f"⏩ Skipping already saved alpha: {alpha_id}")
                continue

            alpha_data = safe_fetch_alpha(alpha_id, timeout=15)
            if alpha_data:
                enqueue_save(alpha_data)
                print(f"✅ Retried and enqueued alpha {alpha_id} for save")
            else:
                print(f"❌ Still failed: {alpha_id}")
                remaining.append(entry)

        _rewrite_jsonl(FAILED_FILE, remaining)
    except Exception as e:
        print(f"❌ Error during retry: {e}")


# --- Async saver: decouple I/O from worker threads ---
SAVE_QUEUE_MAX = 512
save_queue = Queue(maxsize=SAVE_QUEUE_MAX)


def enqueue_save(alpha: dict):
    """Non-blocking enqueue; drops oldest if queue is full to keep workers moving."""
    try:
        save_queue.put_nowait(alpha)
    except Exception:
        try:
            _ = save_queue.get_nowait()
        except Empty:
            pass
        try:
            save_queue.put_nowait(alpha)
        except Exception:
            print("⚠️ Save queue saturated; dropping one payload.")


def _saver_loop():
    while True:
        try:
            alpha = save_queue.get(timeout=0.5)
        except Empty:
            continue
        try:
            save_alpha(alpha)
        except Exception as e:
            print(f"❌ Save failed: {e}")
        finally:
            try:
                save_queue.task_done()
            except Exception:
                pass


threading.Thread(target=_saver_loop, name="saver", daemon=True).start()
