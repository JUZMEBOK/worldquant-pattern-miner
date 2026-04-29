"""Orchestration: ``main_main_thread_wrapper`` and the main worker loop."""

import concurrent.futures
import random
import time
import traceback
from collections import deque

from pattern_search import auth, state
from pattern_search.catalog import (
    extract_placeholders,
    load_type_catalog,
    make_typed_bucket,
    resolve_category,
    resolve_required_type,
    CSV_PATH_VECTOR,
    CSV_PATH_MATRIX,
    CSV_PATH_GROUP,
)
from pattern_search.config import (
    FETCH_INITIAL_DELAY,
    GIANT_STREAM_MODE,
    MAX_CONCURRENT_SIMS,
    MAX_PAIRS,
    RATE_LIMIT_BACKOFF,
    SAMPLE_CAPS,
    SEED,
    SIMULATION_CONFIG,
    STREAM_REFILL_BATCH,
    template,
)
from pattern_search.db import (
    enqueue_save,
    list_simulated_expressions,
    retry_failed_alphas,
)
from pattern_search.state import is_paused, send_notification
from pattern_search.expressions import (
    COMBO_STATS,
    giant_stream_two_placeholders,
    stream_combinations,
)
from pattern_search.simulation import (
    poll_simulation,
    safe_fetch_alpha,
    start_simulation,
)


 # === Orchestration wrapper ===
def main_main_thread_wrapper():
    send_notification("🎮 Starting API...")
    while True:
        try:
            retry_failed_alphas()
            main_loop()
            send_notification("🏁 All work completed.")
            break  # normal exit when main_loop completes
        except Exception as e:
            traceback.print_exc()
            msg = f"⏸️ Unexpected error in main_loop: {e}. Pausing 15s before retry…"
            print(msg)
            send_notification(msg)
            time.sleep(15)


def main_loop():
    using_stream = False
    expr_generator = None

    already_simulated_exprs = list_simulated_expressions({
        "neutralization": SIMULATION_CONFIG["neutralization"],
        "truncation": SIMULATION_CONFIG["truncation"],
        "decay": SIMULATION_CONFIG["decay"],
        "delay": SIMULATION_CONFIG["delay"],
        "universe": SIMULATION_CONFIG["universe"],
        "region": SIMULATION_CONFIG["region"],
    })
    print(f"Total already simulated expressions: {len(already_simulated_exprs)}")

    pending_exprs = []
    placeholders = extract_placeholders(template)

    if len(placeholders) == 1:
        # Single-placeholder mode for ANY placeholder name, via Option A typing
        single_key = placeholders[0]
        rtype = resolve_required_type(single_key)
        cat = resolve_category(single_key)

        # Load once and build the bucket by (category=placeholder, required_type)
        rng = random.Random(SEED)
        catalog = load_type_catalog((CSV_PATH_VECTOR, CSV_PATH_MATRIX, CSV_PATH_GROUP))
        cap = int(SAMPLE_CAPS.get(cat, 0) or 0)
        ids = make_typed_bucket(catalog, cat, rtype, cap, rng)

        queued_before = len(pending_exprs)
        for rid in ids:
            expr = template.format(**{single_key: rid})
            if expr not in already_simulated_exprs:
                pending_exprs.append(expr)
        queued_after = len(pending_exprs) - queued_before

        state.SINGLE_STATS.update({
            "placeholder": single_key,
            "required_type": rtype,
            "category": cat,
            "source_csvs": list(catalog.get("_sources", [])),
            "ids": len(ids),
            "queued_after_dedup": queued_after
        })
        print(f"[single] placeholder={single_key} type={rtype} ids={len(ids)} queued_after_dedup={queued_after}")

    elif 2 <= len(placeholders) <= 5:
        if GIANT_STREAM_MODE and len(placeholders) == 2:
            # Giant N^2 stream: do NOT materialize the full pending list.
            using_stream = True
            expr_generator = giant_stream_two_placeholders(template, SAMPLE_CAPS, SEED, already_simulated_exprs)
            try:
                COMBO_STATS["queued_after_dedup"] = None  # unknown upfront in stream mode
            except Exception:
                pass
            print(f"[combo-2/stream] streaming all pairs without prebuild; dedup at enqueue")
        else:
            # Legacy multi-category (3..5) or if GIANT_STREAM_MODE disabled
            for expr in stream_combinations(template, SAMPLE_CAPS, MAX_PAIRS, SEED):
                if expr not in already_simulated_exprs:
                    pending_exprs.append(expr)
            try:
                COMBO_STATS["queued_after_dedup"] = len(pending_exprs)
            except Exception:
                pass
            print(f"[combo-{len(placeholders)}] queued_after_dedup={len(pending_exprs)}")

    else:
        raise SystemExit(f"Template must have between 1 and 5 placeholders; found {len(placeholders)}: {placeholders}")

    if not using_stream:
        random.shuffle(pending_exprs)

        # De-duplicate pending expressions while preserving order (defensive)
        _seen_expr = set()
        pending_exprs = [e for e in pending_exprs if not (e in _seen_expr or _seen_expr.add(e))]

        with state.datafields_lock:
            state.datafields = deque(pending_exprs)
    else:
        # Start with an empty queue; we'll continuously refill from expr_generator.
        with state.datafields_lock:
            state.datafields = deque()

    with state.active_sims_lock:
        state.active_sims.clear()

    poll_interval = 6
    last_poll = 0
    gen_exhausted = False  # set once when the streaming generator raises StopIteration

    while True:
        # --- Refill queue from giant stream (no pre-cap; enumerates all pairs) ---
        if using_stream and not gen_exhausted and not is_paused():
            # Pull in batches so we start quickly; this does NOT cap the total space.
            with state.datafields_lock:
                need = max(0, STREAM_REFILL_BATCH - len(state.datafields))
            pulled = 0
            while pulled < need:
                try:
                    expr = next(expr_generator)
                except StopIteration:
                    gen_exhausted = True
                    break
                # Final cross-check against the simulated set to ensure no repeats
                if expr in already_simulated_exprs:
                    continue
                with state.datafields_lock:
                    state.datafields.append(expr)
                pulled += 1
            if pulled:
                print(f"[stream] Refilled {pulled} expressions (queue now {len(state.datafields)})")

        with state.datafields_lock:
            _has_datafields = bool(state.datafields)
        with state.active_sims_lock:
            _has_active = bool(state.active_sims)

        # Exit only when nothing is queued, nothing is in flight, and (in stream mode)
        # the generator has been fully drained.
        if using_stream:
            if not _has_datafields and not _has_active and gen_exhausted:
                break
        else:
            if not (_has_datafields or _has_active):
                break
        try:
            now = time.time()

            # Refresh token before each cycle
            try:
                token = auth.get_valid_token()
                expiry = auth.check_token_timeout(token)
                auth.headers = {"Cookie": f"t={token}"}

                with state.datafields_lock:
                    _remaining_df = len(state.datafields)
                with state.active_sims_lock:
                    _active_keys = list(state.active_sims.keys())
                if not is_paused():
                    print(f"Remaining datafields={_remaining_df}, Loop start: active_sims={_active_keys}")
            except Exception as e:
                print(f"⚠️ Error refreshing token: {e}. Retrying in 10 seconds...")
                time.sleep(10)
                continue

            # Poll active simulations periodically
            if now - last_poll >= poll_interval:
                sims_to_remove = []

                with state.active_sims_lock:
                    _snapshot_items = list(state.active_sims.items())
                # Submit all active sims to a pool of 8 workers
                with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_SIMS) as pool:
                    future_to_sim = {}
                    for sim_id, info in _snapshot_items:
                        try:
                            sim_obj = info.get('simulation') if isinstance(info, dict) else None
                            if not sim_obj:
                                # Legacy/malformed entry: sometimes info itself is the sim dict
                                if isinstance(info, dict) and 'status_url' in info and 'id' in info:
                                    sim_obj = info
                                else:
                                    print(f"⚠️ Skipping malformed active_sims entry {sim_id}: {type(info)}")
                                    sims_to_remove.append(sim_id)
                                    continue
                            future = pool.submit(poll_simulation, sim_obj, not is_paused())
                            future_to_sim[future] = (sim_id, info)
                        except Exception as e:
                            print(f"⚠️ Failed to schedule polling for {sim_id}: {e}. Removing.")
                            sims_to_remove.append(sim_id)

                    for future in concurrent.futures.as_completed(future_to_sim):
                        sim_id, info = future_to_sim[future]
                        datafield_id = info.get('expression', '?') if isinstance(info, dict) else '?'
                        try:
                            result = future.result()
                            if not isinstance(result, dict):
                                print(f"⚠️ Unexpected poll_simulation return type for {datafield_id}: {type(result)}. Removing.")
                                sims_to_remove.append(sim_id)
                                continue

                            if result.get("ok") is True:
                                alpha_id = (result.get("data") or {}).get("alpha")
                                print(f"✅ Simulation complete for expression: {datafield_id} with alpha ID {alpha_id}")
                                time.sleep(FETCH_INITIAL_DELAY)
                                if alpha_id:
                                    for attempt in range(2):
                                        try:
                                            alpha_data = safe_fetch_alpha(alpha_id)
                                            enqueue_save(alpha_data)
                                            print(f"📥 Alpha {alpha_id} enqueued for save")
                                            break
                                        except Exception as e:
                                            print(f"⚠️ Error fetching/saving alpha {alpha_id}: {e}")
                                            time.sleep(RATE_LIMIT_BACKOFF)
                                sims_to_remove.append(sim_id)

                            elif result.get("ok") is False:
                                print(f"❌ Simulation reported failure for {datafield_id}. Status={result.get('status')}")
                                sims_to_remove.append(sim_id)

                            else:
                                # ok is None → still in progress; do nothing this cycle
                                pass

                        except Exception as e:
                            print(f"⚠️ Unexpected error polling simulation {sim_id}: {e}. Skipping this one.")
                            sims_to_remove.append(sim_id)

                # Remove finished/errored sims and free capacity
                with state.active_sims_lock:
                    for sim_id in sims_to_remove:
                        if sim_id in state.active_sims:
                            del state.active_sims[sim_id]
                        try:
                            state.active_sims_semaphore.release()
                        except ValueError:
                            pass
                last_poll = now

            # Treat token expiry as a separate reason; combine with paused state when logging below
            token_expiry_near = (expiry <= 300)
            prevent_new_simulations = token_expiry_near

            # Start new simulations within concurrency limits
            if not prevent_new_simulations and not is_paused():
                while True:
                    with state.active_sims_lock:
                        _can_start_more = len(state.active_sims) < MAX_CONCURRENT_SIMS
                    if not _can_start_more:
                        break
                    with state.datafields_lock:
                        if not state.datafields:
                            break
                        next_expr = state.datafields.popleft()

                    # Cooldown gate: if this expr recently failed to start, push it back and try another
                    next_ok = state.REQUEUE_COOLDOWN.get(next_expr)
                    if next_ok and time.time() < next_ok:
                        with state.datafields_lock:
                            state.datafields.append(next_expr)
                        continue

                    # Skip if already running to avoid duplicate starts (defensive if queue was edited elsewhere)
                    with state.active_sims_lock:
                        if any((isinstance(info, dict) and info.get('expression') == next_expr) for info in
                               state.active_sims.values()):
                            print(f"🔁 Skipping duplicate start; already running: {next_expr}")
                            continue

                    print(f"Starting simulation for expression: {next_expr}")

                    # Enforce hard cap on concurrent simulations via semaphore
                    if not state.active_sims_semaphore.acquire(blocking=False):
                        with state.datafields_lock:
                            # Couldn’t start due to capacity; keep it hot by pushing to the front
                            state.datafields.appendleft(next_expr)
                        break  # leave the start loop; let polling free slots
                    try:
                        sim = start_simulation(next_expr)
                        if sim and 'id' in sim:
                            sim_id = sim['id']
                            with state.active_sims_lock:
                                state.active_sims[sim_id] = {
                                    'expression': next_expr,
                                    'simulation': sim,
                                    'start_time': time.time()
                                }
                            print(f"Simulation {sim_id} started successfully.")
                            time.sleep(state.START_DELAY)
                        else:
                            if is_paused():
                                # start_simulation re-queued this expression already; keep quiet to avoid spam
                                pass
                            else:
                                try:
                                    state.active_sims_semaphore.release()
                                except ValueError:
                                    pass
                                print(f"Failed to start simulation for {next_expr}. Skipping.")
                    except Exception as e:
                        try:
                            state.active_sims_semaphore.release()
                        except ValueError:
                            pass
                        print(f"⚠️ Error starting simulation for {next_expr}: {e}. Skipping.")
                        continue  # Skip problematic expression
            else:
                # Combined-reason logging for skipping new simulations
                if is_paused() and token_expiry_near:
                    print("⏸️ Paused & token expiry near — skipping new simulations this cycle.")
                elif is_paused():
                    print("⏸️ Paused — skipping new simulations this cycle.")
                elif token_expiry_near:
                    print("🛑 Token expiry near — skipping new simulations this cycle.")
                else:
                    # Fallback (shouldn’t happen): generic skip
                    print("🛑 Skipping new simulations this cycle.")

            # Handle long-running simulations that exceed the timeout limit
            TIMEOUT_LIMIT = 1200  # seconds
            now = time.time()
            with state.active_sims_lock:
                timed_out_sims = []
                for sim_id, info in list(state.active_sims.items()):
                    try:
                        st = info.get('start_time') if isinstance(info, dict) else None
                        if st is None:
                            print(f"⚠️ Missing start_time for {sim_id}; removing malformed entry.")
                            timed_out_sims.append(sim_id)
                            continue
                        if now - st > TIMEOUT_LIMIT:
                            print(f"Simulation {sim_id} timed out and is being removed.")
                            timed_out_sims.append(sim_id)
                    except Exception as e:
                        print(f"⚠️ Error checking timeout for {sim_id}: {e}. Removing.")
                        timed_out_sims.append(sim_id)
                for sim_id in timed_out_sims:
                    if sim_id in state.active_sims:
                        del state.active_sims[sim_id]
                    try:
                        state.active_sims_semaphore.release()
                    except ValueError:
                        pass

            print("Sleeping briefly before next cycle...")
            time.sleep(6)

        except Exception as e:
            print(f"❌ Unexpected error in main_loop: {e}. Skipping to next iteration...")
            continue

    # Final cleanup and notifications
    print("🎉 All simulations completed.")

    print("🏁 All work completed.")
