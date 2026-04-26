"""Expression generation utilities."""

import itertools
import random
import time

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
from pattern_search.config import REQUIRE_DIFFERENT_SAME_CATEGORY, _np


# === Helpers: negation policy ===
def is_expression_negated(expr: str) -> bool:
    s = (expr or "").strip()
    if not s:
        return False
    # Check only the tail expression (after the last ';' if any)
    tail = s.rsplit(';', 1)[-1].strip()
    # Consider common negate forms on the tail
    return tail.startswith("-") or tail.startswith("(-1)*")


def negate_expression(expr: str) -> str:
    """Negate ONLY the final expression (after the last ';').
    If there's no ';', negate the whole string. Avoid double-negation if the tail already starts with '-' or '(-1)*'.
    """
    s = (expr or "").strip()
    if not s:
        return s

    if ';' in s:
        head, tail = s.rsplit(';', 1)
        t = tail.strip()
        # Already negated? leave as-is
        if t.startswith('-') or t.startswith('(-1)*'):
            return s
        # Negate only the tail
        neg_tail = f"-({t})"
        # Preserve exact formatting around the last ';'
        return f"{head};{neg_tail}"
    else:
        # Single-expression program: negate whole
        if s.startswith('-') or s.startswith('(-1)*'):
            return s
        return f"-({s})"


 # === Combination streaming & stats ===
# Expose latest counts for combo mode
COMBO_STATS = {}


def stream_combinations(template: str, sample_caps: dict, max_pairs: int, seed: int):
    """
    Generator that yields expressions for the Cartesian product of up to 5 placeholders
    found in the template. Per-placeholder sampling can be controlled via `sample_caps`.
    Now selects IDs by REQUIRED_TYPE (VECTOR/MATRIX) + category.
    """
    from pattern_search.state import is_paused

    rng = random.Random(seed)
    ph_exact = extract_placeholders(template)
    if not (2 <= len(ph_exact) <= 5):
        raise SystemExit(f"Template must have 2..5 placeholders; found: {ph_exact}")

    # Load once and reuse
    catalog = load_type_catalog((CSV_PATH_VECTOR, CSV_PATH_MATRIX, CSV_PATH_GROUP))

    # Build buckets per placeholder
    buckets = []
    counts = {}
    sources = {}
    for name in ph_exact:
        cat = resolve_category(name)
        rtype = resolve_required_type(name)
        cap = int(sample_caps.get(cat, 0) or 0)
        ids = make_typed_bucket(catalog, cat, rtype, cap, rng)
        buckets.append(ids)
        counts[name] = len(ids)
        sources[name] = {"type": rtype, "csv": list(catalog.get("_sources", []))}

    # Stats
    raw = 1
    for ids in buckets:
        raw *= len(ids)
    cap_total = min(raw, max_pairs or raw)
    COMBO_STATS.update({
        "placeholders": ph_exact,
        "counts": counts,
        "sources": sources,
        "raw_pairs": raw,
        "capped_pairs": cap_total,
    })
    print(f"[combo-{len(ph_exact)}] counts={counts} raw_pairs={raw} capped={cap_total}")

    # Emit product
    emitted = 0
    for combo in itertools.product(*buckets):
        # If paused, hold generation here without consuming more combinations
        while is_paused():
            time.sleep(0.2)
        # Enforce uniqueness across placeholders that share the same base category (e.g., option_a vs option_b)
        if REQUIRE_DIFFERENT_SAME_CATEGORY:
            base_to_idxs = {}
            for i, nm in enumerate(ph_exact):
                base = resolve_category(nm)
                base_to_idxs.setdefault(base, []).append(i)
            must_skip = False
            for base, idxs in base_to_idxs.items():
                if len(idxs) >= 2:
                    chosen = [combo[i] for i in idxs]
                    if len(set(chosen)) != len(chosen):
                        must_skip = True
                        break
            if must_skip:
                continue
        mapping = {ph_exact[i]: combo[i] for i in range(len(ph_exact))}
        code = template.format(**mapping)
        yield code
        emitted += 1
        if max_pairs and emitted >= max_pairs:
            return


# === Giant streaming helpers for 2-placeholders ===
def _build_two_placeholder_buckets_for_stream(template: str, sample_caps: dict, seed: int):
    """
    Resolve exactly two placeholders from `template`, build typed buckets for each,
    and return (ph_names, ids_a, ids_b, counts, sources, catalog).
    """
    rng = random.Random(seed)
    ph_exact = extract_placeholders(template)
    if len(ph_exact) != 2:
        raise SystemExit(f"GIANT_STREAM_MODE requires exactly 2 placeholders; found: {ph_exact}")

    # Load catalog once
    catalog = load_type_catalog((CSV_PATH_VECTOR, CSV_PATH_MATRIX, CSV_PATH_GROUP))

    phA, phB = ph_exact[0], ph_exact[1]
    catA = resolve_category(phA)
    catB = resolve_category(phB)
    rtypeA = resolve_required_type(phA)
    rtypeB = resolve_required_type(phB)

    capA = int(sample_caps.get(catA, 0) or 0)
    capB = int(sample_caps.get(catB, 0) or 0)

    ids_a = make_typed_bucket(catalog, catA, rtypeA, capA, rng)
    ids_b = make_typed_bucket(catalog, catB, rtypeB, capB, rng)

    counts = {phA: len(ids_a), phB: len(ids_b)}
    sources = {phA: {"type": rtypeA, "csv": list(catalog.get("_sources", []))},
               phB: {"type": rtypeB, "csv": list(catalog.get("_sources", []))}}
    return (phA, phB), ids_a, ids_b, counts, sources, catalog


def _make_expr_builder_for_two_placeholders(template: str, phA: str, phB: str):
    """
    Pre-split the template around {phA} and {phB} (in textual order) to build strings faster
    via concatenation rather than full format() each time.
    """
    mA = "{" + phA + "}"
    mB = "{" + phB + "}"
    t = template
    # Ensure order is left-to-right in the template
    idxA = t.find(mA)
    idxB = t.find(mB)
    if idxA == -1 or idxB == -1:
        raise SystemExit(f"Placeholders {{{phA}}} or {{{phB}}} not found in template.")
    if idxB < idxA:
        # swap so A is left of B
        phA, phB = phB, phA
        mA, mB = mB, mA
        idxA, idxB = idxB, idxA

    # Split into prefix, mid, suffix
    prefix, rest = t.split(mA, 1)
    mid, suffix = rest.split(mB, 1)

    def build_expr(a_val: str, b_val: str) -> str:
        return prefix + a_val + mid + b_val + suffix

    return (phA, phB), build_expr


def giant_stream_two_placeholders(template: str, sample_caps: dict, seed: int,
                                  already_simulated_exprs: set):
    """
    Yields ALL expressions for the 2-placeholder template in a randomized order, without
    materializing a giant list. Dedup is enforced against `already_simulated_exprs`
    at enqueue-time. REQUIRE_DIFFERENT_SAME_CATEGORY is honored.
    """
    # Prepare buckets and fast builder
    (phA, phB), ids_a, ids_b, counts, sources, _catalog = _build_two_placeholder_buckets_for_stream(
        template, sample_caps, seed
    )
    (phA, phB), build_expr = _make_expr_builder_for_two_placeholders(template, phA, phB)

    NA, NB = len(ids_a), len(ids_b)
    raw = NA * NB
    COMBO_STATS.update({
        "placeholders": [phA, phB],
        "counts": counts,
        "sources": sources,
        "raw_pairs": raw,
        "capped_pairs": raw,  # no cap
    })
    print(f"[combo-2/stream] counts={counts} raw_pairs={raw} capped={raw}")

    # Random permutations for rows/cols
    if _np is not None:
        rng = _np.random.RandomState(seed)
        row_perm = rng.permutation(NA)
        col_perm = rng.permutation(NB)
    else:
        rng = random.Random(seed)
        row_perm = list(range(NA))
        col_perm = list(range(NB))
        rng.shuffle(row_perm)
        rng.shuffle(col_perm)

    # Category-difference rule (same base category must choose different IDs)
    baseA = resolve_category(phA)
    baseB = resolve_category(phB)
    require_diff = REQUIRE_DIFFERENT_SAME_CATEGORY and (baseA == baseB)

    # Stream over the full Cartesian product in permuted row-major order
    for ri in row_perm:
        a_val = ids_a[ri]
        # If requires-different and both placeholders share the same category,
        # we emit all B except the one equal to A.
        if require_diff:
            # Iterate columns in randomized order, skipping the equal one
            for cj in col_perm:
                b_val = ids_b[cj]
                if b_val == a_val:
                    continue
                expr = build_expr(a_val, b_val)
                if expr in already_simulated_exprs:
                    continue
                yield expr
        else:
            for cj in col_perm:
                b_val = ids_b[cj]
                expr = build_expr(a_val, b_val)
                if expr in already_simulated_exprs:
                    continue
                yield expr
