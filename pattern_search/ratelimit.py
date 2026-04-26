import threading
import time
from time import monotonic


# Smooth out bursts of start requests across threads to avoid herding 429s
class _TokenBucket:
    def __init__(self, rate_per_sec: float, capacity: int):
        self.rate = float(rate_per_sec)
        self.capacity = int(capacity)
        self.tokens = float(capacity)
        self.t = monotonic()
        self._lock = threading.Lock()
    def take(self, amount: float = 1.0) -> float:
        """Attempt to take `amount` tokens. Return sleep seconds if insufficient (0 if ok)."""
        with self._lock:
            now = monotonic()
            elapsed = now - self.t
            self.t = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            if self.tokens >= amount:
                self.tokens -= amount
                return 0.0
            deficit = amount - self.tokens
            return deficit / self.rate


# Tune: ~2 starts/sec with a burst of 2 lets 8 workers ramp without spikes
START_RATE_PER_SEC = 2.0
START_BURST = 2
_start_bucket = _TokenBucket(START_RATE_PER_SEC, START_BURST)

# Prevent rapid duplicate queueing of the same expression across quick cycles
RECENT_ENQUEUE_TTL_SECONDS = 120  # 2 minutes
_RECENT_ENQUEUE = {}  # expr -> expiry_epoch


def _recent_enq_allows(expr: str) -> bool:
    now = time.time()
    # prune expired
    try:
        expired = [k for k, t in _RECENT_ENQUEUE.items() if t <= now]
        for k in expired:
            _RECENT_ENQUEUE.pop(k, None)
    except Exception:
        pass
    t = _RECENT_ENQUEUE.get(expr)
    return (t is None) or (t <= now)


def _recent_enq_mark(expr: str):
    try:
        _RECENT_ENQUEUE[expr] = time.time() + RECENT_ENQUEUE_TTL_SECONDS
    except Exception:
        pass
