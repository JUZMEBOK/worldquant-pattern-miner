import csv
import random
from pathlib import Path

from pattern_search.config import (
    PLACEHOLDER_CATEGORY,
    REQUIRED_TYPE,
    SIMULATION_CONFIG,
)


# === Data catalogs (paths) ===
# === Combo-stream config (for multi-placeholder combos: 2..5) ===
# Paths are resolved from ./datafields/{REGION}/ using uppercase filenames.
# Supported new scheme (no underscores):
#   ./datafields/{REGION}/VECTOR{D}
#   ./datafields/{REGION}/MATRIX{D}   or ./datafields/{REGION}/MATRIX
#   ./datafields/{REGION}/GROUP{D}
# Each may optionally include a ".csv" extension.

# --- CSV path resolver (VECTOR/MATRIX/GROUP; uppercase, no underscores) ---
def build_csv_paths(region: str, delay: int):
    """
    Return (vector_path, matrix_path, group_path) from ./datafields/{REGION}/.

    Filenames (uppercase), extension optional:
      VECTOR{delay} or VECTOR
      MATRIX{delay} or MATRIX
      GROUP{delay}  or GROUP
    """
    here = Path(__file__).resolve().parent.parent
    R = (region or "").upper()
    d = int(delay or 0)
    region_dir = here / "datafields" / R

    def try_variants(base_names):
        """Return the first existing candidate path (with or without .csv), else None."""
        for name in base_names:
            for candidate in (region_dir / name, region_dir / f"{name}.csv"):
                if candidate.exists():
                    return str(candidate)
        return None

    vec = try_variants([f"VECTOR{d}", "VECTOR"])  # prefer delay-specific then generic
    mat = try_variants([f"MATRIX{d}", "MATRIX"])  # prefer delay-specific then generic
    grp = try_variants([f"GROUP{d}",  "GROUP"])   # prefer delay-specific then generic

    # Only the types referenced by REQUIRED_TYPE are mandatory.
    needed_types = {(v or "").upper() for v in REQUIRED_TYPE.values()}

    missing = []
    if "VECTOR" in needed_types and not vec:
        missing.append(f"{region_dir}/VECTOR{d}(.csv) or VECTOR(.csv)")
    if "MATRIX" in needed_types and not mat:
        missing.append(f"{region_dir}/MATRIX{d}(.csv) or MATRIX(.csv)")
    if "GROUP" in needed_types and not grp:
        missing.append(f"{region_dir}/GROUP{d}(.csv) or GROUP(.csv)")
    if missing:
        raise SystemExit("CSV not found for:\n  - " + "\n  - ".join(missing))

    return vec, mat, grp

# Assign CSV path constants early so downstream code can use them
CSV_PATH_VECTOR, CSV_PATH_MATRIX, CSV_PATH_GROUP = build_csv_paths(
    SIMULATION_CONFIG['region'],
    SIMULATION_CONFIG['delay']
)
print(f"[datafields] VECTOR={CSV_PATH_VECTOR or '(unused)'}")
print(f"[datafields] MATRIX={CSV_PATH_MATRIX or '(unused)'}")
print(f"[datafields] GROUP={CSV_PATH_GROUP or '(unused)'}")


 # === Helpers: CSV path resolution ===
def _resolve_csv(path_str: str) -> Path:
    p = Path(path_str)
    if p.exists():
        return p
    here = Path(__file__).resolve().parent.parent
    q = here / Path(path_str).name
    if q.exists():
        return q
    raise SystemExit(f"CSV not found: {path_str}\nTried: {p} and {q}")

 # === CSV loaders (strict & tolerant) ===
def load_catalog(csv_path: Path):
    """Yield (id, category_lowercased). CSV needs columns: id|datafield_id and category_id|category."""
    with csv_path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        fmap = {k.lower(): k for k in r.fieldnames}
        idcol  = fmap.get("id") or fmap.get("datafield_id")
        catcol = fmap.get("category_id") or fmap.get("category")
        if not idcol or not catcol:
            raise SystemExit("CSV must include 'id' (or 'datafield_id') and 'category_id' (or 'category').")
        for row in r:
            rid = (row[idcol] or "").strip()
            cat = (row[catcol] or "").strip().lower()
            if rid and cat:
                yield rid, cat

# --- Tolerant loader for CSVs that may not have a category column
def load_catalog_or_simple(csv_path: Path, default_category_lower: str):
    """
    Load (id, category_lower) pairs from csv_path.
    - If CSV has 'category'/'category_id': use it.
    - Else: treat ALL ids as belonging to `default_category_lower`.
    """
    with csv_path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        fmap = {k.lower(): k for k in r.fieldnames}
        idcol  = fmap.get("id") or fmap.get("datafield_id")
        catcol = fmap.get("category_id") or fmap.get("category")
        if not idcol:
            raise SystemExit("CSV must include 'id' (or 'datafield_id').")
        if catcol:
            for row in r:
                rid = (row[idcol] or "").strip()
                cat = (row[catcol] or "").strip().lower() or default_category_lower
                if rid:
                    yield rid, cat
        else:
            for row in r:
                rid = (row[idcol] or "").strip()
                if rid:
                    yield rid, default_category_lower

 # === Template utilities ===
def extract_placeholders(template: str):
    """Return ordered unique placeholder names exactly as written in the template."""
    import re
    ph = re.findall(r"\{([a-zA-Z0-9_]+)\}", template)
    uniq = list(dict.fromkeys(ph))
    return uniq


# === Placeholder typing + type-split catalog ===
def resolve_required_type(name: str) -> str:
    """
    Return 'VECTOR', 'MATRIX', or 'GROUP' for the given placeholder name (case-insensitive).
    Requires an explicit entry in REQUIRED_TYPE.
    """
    t = REQUIRED_TYPE.get(name.lower())
    if not t:
        raise SystemExit(
            f"REQUIRED_TYPE missing for placeholder '{name}'. "
            f"Add it to REQUIRED_TYPE (VECTOR/MATRIX/GROUP)."
        )
    t = t.strip().upper()
    if t not in ("VECTOR", "MATRIX", "GROUP"):
        raise SystemExit(f"Invalid REQUIRED_TYPE for '{name}': '{t}'. Use VECTOR, MATRIX, or GROUP.")
    return t

# === Placeholder → base category mapping helper ===
def resolve_category(name: str) -> str:
    """
    Map a placeholder name to its CSV category (e.g., 'option_a' -> 'option').
    Defaults to the placeholder name itself if not found in PLACEHOLDER_CATEGORY.
    """
    return PLACEHOLDER_CATEGORY.get(name.lower(), name.lower())

def load_type_catalog(paths) -> dict:
    """
    Read one or more CSVs and build a type-split catalog:
        {'VECTOR': [(id, category_lower)], 'MATRIX': [(id, category_lower)], 'GROUP': [(id, category_lower)], '_sources': [path,...]}
    Each CSV should have 'id' (or 'datafield_id'), optional 'category' (or 'category_id'),
    and 'type' columns. If 'type' is missing, we try to infer from filename as a last resort.
    """
    catalog = {"VECTOR": [], "MATRIX": [], "GROUP": [], "_sources": []}
    seen_files = set()
    for path_str in paths:
        if not path_str:
            continue
        csv_path = _resolve_csv(path_str)
        if str(csv_path) in seen_files:
            continue
        seen_files.add(str(csv_path))
        catalog["_sources"].append(str(csv_path))

        with csv_path.open(newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            fmap = {k.lower(): k for k in r.fieldnames}
            idcol   = fmap.get("id") or fmap.get("datafield_id")
            catcol  = fmap.get("category_id") or fmap.get("category")
            typecol = fmap.get("type")
            if not idcol:
                raise SystemExit("CSV must include 'id' (or 'datafield_id').")

            for row in r:
                rid = (row[idcol] or "").strip()
                if not rid:
                    continue
                cat = (row[catcol] or "").strip().lower() if catcol else ""
                typ = (row[typecol] or "").strip().upper() if typecol else ""
                if not typ:
                    # Infer from filename as a last resort
                    name = csv_path.name.lower()
                    if "vector" in name:
                        typ = "VECTOR"
                    elif "matrix" in name:
                        typ = "MATRIX"
                    elif "group" in name:
                        typ = "GROUP"
                    else:
                        raise SystemExit(f"CSV {csv_path} lacks a 'type' column and cannot infer VECTOR/MATRIX/GROUP.")

                if typ not in ("VECTOR", "MATRIX", "GROUP"):
                    raise SystemExit(f"Invalid 'type' value '{typ}' in {csv_path}. Expected VECTOR, MATRIX, or GROUP.")

                catalog[typ].append((rid, cat))

    # De-duplicate while preserving order
    for typ in ("VECTOR", "MATRIX", "GROUP"):
        seen = set()
        dedup = []
        for rid, cat in catalog[typ]:
            key = (rid, cat)
            if key not in seen:
                seen.add(key)
                dedup.append((rid, cat))
        catalog[typ] = dedup
    return catalog

 # === Bucket builders ===
def make_typed_bucket(catalog: dict, category_lower: str, required_type: str, k: int, rng: random.Random):
    """
    Build an ID list for a specific (category, required_type: VECTOR/MATRIX/GROUP) from the pre-loaded catalog.
    If a row's category is empty (CSV without category column), treat it as matching any category.
    """
    rows = catalog.get(required_type, [])
    ids = [rid for rid, cat in rows if (cat == category_lower) or (cat == "")]
    ids = list(dict.fromkeys(ids))  # de-dup keep order
    if not ids:
        raise SystemExit(f"No datafields found for category '{category_lower}' with type={required_type}.")
    if k and k < len(ids):
        ids = rng.sample(ids, k)
    else:
        rng.shuffle(ids)
    return ids

def make_bucket(rows, category_lower: str, k: int, rng: random.Random):
    """Build a unique ID list for the given lowercased category, shuffled/sampled."""
    ids = [rid for rid, cat in rows if cat == category_lower]
    ids = list(dict.fromkeys(ids))  # de-dup keep order
    if not ids:
        raise SystemExit(f"No datafields found for category '{category_lower}'.")
    if k and k < len(ids):
        ids = rng.sample(ids, k)
    else:
        rng.shuffle(ids)
    return ids
