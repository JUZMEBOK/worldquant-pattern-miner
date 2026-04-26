import os
from types import MappingProxyType

# === Shared simulation configuration (immutable) ===
SIMULATION_CONFIG = MappingProxyType({
    'region': 'USA',
    'universe': 'TOP3000',
    'neutralization': 'SUBINDUSTRY',
    'truncation': 0.01,
    'decay': 0,
    'delay': 1,
})

# === Placeholder → required type mapping & category mapping ===
# Make sure every placeholder used in your template appears here.
REQUIRED_TYPE = {  # Explicit per-placeholder type requirement (VECTOR/MATRIX/GROUP). Every placeholder in EXPRESSION_TEMPLATE must be listed here.
    "a": "MATRIX",
    "b": "MATRIX",
}

# Map placeholders to CSV categories. This lets multiple placeholders (e.g., option_a/option_b) pull from the same 'option' bucket.
PLACEHOLDER_CATEGORY = {
    "a": "news",
    "b": "news",
}

# If True, when multiple placeholders resolve to the SAME base category (e.g., both 'option'), enforce that their selected IDs are different.
# Set to False to allow the same ID on both sides (i.e., {option_a} == {option_b}).
REQUIRE_DIFFERENT_SAME_CATEGORY = True

# Term-structure (e.g., 30d vs 90d) — set REQUIRE_DIFFERENT_SAME_CATEGORY as desired.
# 21, 63, 120, 252
#EXPRESSION_TEMPLATE = ""
#EXPRESSION_TEMPLATE = "rank(ts_decay_linear(-zscore(0.5*({a}+{b})),60))"
#EXPRESSION_TEMPLATE = "rank(ts_mean(vec_avg({fundamental_a}),10)/ts_mean(vec_avg({fundamental_b}),252))" # yet finished for EUR0
#EXPRESSION_TEMPLATE = "rank(ts_mean(vec_avg({a}),10)/ts_mean(vec_avg({b}),252))" # good for news
EXPRESSION_TEMPLATE = "rank(ts_mean({a},10)/ts_mean({b},252))" # good for news
#EXPRESSION_TEMPLATE = "rank(ts_mean(vec_avg({news}),10)/ts_mean(vec_avg({earnings}),252))"
#EXPRESSION_TEMPLATE = "rank(ts_decay_linear(trade_when({fundamental},252)>0.8, ts_rank({fundamental},120), 0), 60))"
#EXPRESSION_TEMPLATE = "ts_backfill(sqrt(rank(ts_mean({a},3)/ts_mean({b},252))),21)" #this was good
#EXPRESSION_TEMPLATE = "ts_backfill(sqrt(rank(ts_mean(vec_sum({fundamental}),3)/ts_mean(vec_sum({fundamental}),252))),21)" #this was good
#EXPRESSION_TEMPLATE = "rank(ts_decay_linear(zscore({fundamental}) * tanh(3*(ts_rank({fundamental},252)-0.5)), 63))"
#EXPRESSION_TEMPLATE = "alpha = ts_corr(open/close,{risk},504);if_else(alpha<0,alpha,0)"
#EXPRESSION_TEMPLATE = "signal=zscore({option_a}-{option_b});ts_decay_linear(signal, 5) * rank(volume*close) + ts_decay_linear(signal, 300) * (1-rank(volume*close))"
#EXPRESSION_TEMPLATE = "trade_when(rank(vec_sum({earnings}))>0.6,rank(ts_mean(vec_sum({other}),63)),-1)"
#EXPRESSION_TEMPLATE = "rank(ts_mean(tanh({b}),63))"
#EXPRESSION_TEMPLATE = "ts_backfill(sqrt(rank(ts_mean({analyst},5)/ts_mean({analyst},252))),63)"
#EXPRESSION_TEMPLATE = "group_neutralize({fundamental},bucket(rank(ts_corr({fundamental},1/{fundamental},5)),range='0,1,0.1'))"

# Maximum number of simultaneous active simulations allowed.
# Adjust freely — this is the single knob for parallelism.
MAX_CONCURRENT_SIMS = 3


template = "".join(EXPRESSION_TEMPLATE.split())

# --- Giant streaming (N^2) mode for 2-placeholders ---
try:
    import numpy as _np  # optional; falls back to Python if unavailable
except Exception:
    _np = None

GIANT_STREAM_MODE = True          # enable streaming for 2-placeholders (no materialized giant string list)
ROW_CHUNK_SIZE = 128              # rows per refill when using numpy; safe on memory
STREAM_REFILL_BATCH = 20000       # how many expressions to prefill when the queue runs low (not a hard cap, just refill size)

# === Expression template (FASTEXPR) ===
MAX_PAIRS  = 0      # 0 = unlimited (be careful if buckets are huge)
SEED       = 123    # deterministic shuffle for reproducibility
SAMPLE_CAPS = {      # 'model': 200, 'risk': 50, 'pv': 0, 'earnings': 0, ... 0 means all
}

# === Runtime constants ===

RATE_LIMIT_BACKOFF = 3   # seconds; used for 429/502 retries
FETCH_MAX_ATTEMPTS = 8
FETCH_BACKOFF = 3
FETCH_INITIAL_DELAY = 2
POLL_RETRY_MAX = 3
POLL_RETRY_DELAY = 3

DEBUG_API = str(os.getenv("DEBUG_API", "0")).lower() in {"1", "true", "yes", "on"}
