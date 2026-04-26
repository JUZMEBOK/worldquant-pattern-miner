"""Shared mutable runtime state and lightweight pause/notify primitives.

Holds globals mutated across modules (``datafields``, ``active_sims``,
locks/events, stats caches) plus the simple in-process pause + notify
helpers that replaced the old Discord control surface.
"""

import threading
from collections import deque

from pattern_search.config import MAX_CONCURRENT_SIMS

# === Runtime globals shared across modules ===
datafields = deque()
active_sims = {}
# Semaphore to enforce the hard cap on concurrent simulations
active_sims_semaphore = threading.Semaphore(MAX_CONCURRENT_SIMS)
main_loop_running = False

 # === Concurrency primitives ===
active_sims_lock = threading.Lock()
datafields_lock = threading.Lock()
# Global shutdown signal (used by saver loop and graceful exit)
stop_event = threading.Event()

# --- Stats for single and combo modes
SINGLE_STATS = {}

# Progress/status cache for throttled polling logs
LAST_STATUS = {}
LAST_PROGRESS = {}

# Cooldown for expressions that repeatedly fail to start (prevents hot-looping)
REQUEUE_COOLDOWN_SECONDS = 90
REQUEUE_COOLDOWN = {}  # expr -> next_allowed_epoch_seconds

START_DELAY = 1.5        # seconds delay between start_simulation calls

# === Pause / notify (replaces the old discord control surface) ===
# Set => API-side work (new sims, expression streaming) pauses; existing polls
# continue. Auth flow sets this before re-authentication and clears on success.
pause_event = threading.Event()


def is_paused() -> bool:
    return pause_event.is_set()


def pause(reason: str = "") -> None:
    if not pause_event.is_set():
        pause_event.set()
        print(f"⏸️  PAUSED" + (f": {reason}" if reason else ""))


def resume(reason: str = "") -> None:
    if pause_event.is_set():
        pause_event.clear()
        print(f"▶️  RESUMED" + (f": {reason}" if reason else ""))


def send_notification(message: str) -> None:
    """Local replacement for the old Discord notifier. Prints to stdout."""
    print(f"[notify] {message}")
