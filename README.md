# worldquant-pattern-miner

Pattern-mining toolkit for the [WorldQuant Brain](https://platform.worldquantbrain.com/) platform.
Discovers data fields, generates alpha expression candidates from templates, simulates them
concurrently, and persists results.

> **Tier:** Built for the **non-consultant** (free) Brain environment, where the platform
> caps simultaneous simulations to **3** per user. The default `MAX_CONCURRENT_SIMS = 3`
> in `pattern_search/config.py` matches that limit. If you have a consultant account
> with a higher concurrency budget (e.g. 8 / 10 / 16), bump that single constant —
> nothing else needs to change.

## Project layout

```
worldquant-pattern-miner/
├── pattern_search.py          # entry script: python pattern_search.py
├── pattern_search/            # main package (alpha generator + simulator)
│   ├── runner.py              # main_loop orchestration
│   ├── auth.py                # token lifecycle, persona flow, pauses on expiry
│   ├── catalog.py             # CSV resolvers, type/category buckets
│   ├── expressions.py         # placeholder/template streamers
│   ├── simulation.py          # start/poll/fetch alpha simulations
│   ├── db.py                  # JSONL persistence, save queue, retry helpers
│   ├── ratelimit.py           # token bucket + dedup
│   ├── state.py               # shared runtime state + pause/notify primitives
│   ├── config.py              # SIMULATION_CONFIG, EXPRESSION_TEMPLATE, constants
│   └── paths.py               # project-anchored paths
├── credentials/
│   ├── pw                     # WorldQuant Brain login (JSON: ["email", "password"])
│   ├── brain_token.txt        # JWT issued by token_refresh.py (auto-generated)
│   └── token_refresh.py       # persona-biometric auth → writes brain_token.txt
├── datafields/
│   ├── datafields_regional_master.py  # fetches data-field catalogs from the Brain API
│   └── {REGION}/                      # CSV catalogs (VECTOR{D}.csv, MATRIX{D}.csv, GROUP{D}.csv)
├── data/                      # outputs & checkpoints (gitignored)
├── requirements.txt
└── .gitignore
```

All scripts anchor paths to the project root via `Path(__file__).resolve().parent[.parent]`,
so they run correctly regardless of the current working directory and OS — Windows,
macOS and Linux all behave identically.

## Requirements

- Python 3.10+
- A WorldQuant Brain account

Simulated alphas are persisted as JSON Lines to ``data/alphas.jsonl`` (one
record per line). Failed-fetch IDs go to ``data/failed_alphas.jsonl`` and are
retried at startup. No external database is required.

When the Brain JWT expires or fails validation, ``auth.get_valid_token()``
sets the in-process ``state.pause_event``. All API-side work (new sim starts,
expression streaming) checks ``is_paused()`` and waits; existing polls finish
gracefully. The pause clears once re-authentication succeeds.

## Installation

```bash
git clone https://github.com/<your-user>/worldquant-pattern-miner.git
cd worldquant-pattern-miner

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

## Configuration

1. **Credentials** — create a plain-text file at `credentials/pw` (no extension)
   containing **exactly one line**: a JSON array with your Brain login email and
   password.

   ```json
   ["your_email@example.com", "your_password"]
   ```

   Make it in any editor (VS Code, Notepad, TextEdit in *plain text* mode, etc.) —
   no terminal needed. Just save the file as `credentials/pw`. The miner reads it
   verbatim with `json.load`, so the brackets and double quotes are required and
   the password must be JSON-escaped (`\\` for `\`, `\"` for `"`).

   *Optional, macOS/Linux only:* `chmod 600 credentials/pw` to restrict
   permissions. On Windows just keep the file out of any synced/shared folder.

2. **Token refresh** — generates `credentials/brain_token.txt`:
   ```bash
   python credentials/token_refresh.py            # daemon (default): re-auths every 3h55m forever
   python credentials/token_refresh.py --once     # one-shot, then exit
   ```
   On first run you may need to complete biometric (Persona) verification in your browser;
   the script polls until the JWT is issued. **Run it in a separate terminal alongside
   `pattern_search.py`** — the miner is read-only with respect to the token: it never
   re-authenticates by itself, it just waits for the daemon to refresh `brain_token.txt`.

3. **Data-field catalogs** — populate `datafields/{REGION}/`:
   ```bash
   python datafields/datafields_regional_master.py
   ```
   Tweak the target region/universe/delay in `DEFAULT_UNIVERSAL_CONFIG` inside the script,
   or override via env vars (`UNIVERSE`, `WORKERS`, `DEPTH`, `CAP`, `PAGE_SIZE`, …).

## Running

```bash
python pattern_search.py
```

`pattern_search.runner.main_loop` loads CSV catalogs from `datafields/{REGION}/`,
streams expression candidates from `EXPRESSION_TEMPLATE` (configured in
`pattern_search/config.py`), and submits up to `MAX_CONCURRENT_SIMS` simulations
in parallel against the Brain API. Successful alphas are appended verbatim as
JSON Lines to `data/alphas.jsonl` by `pattern_search/db.py`.

### Concurrency & account tier

`MAX_CONCURRENT_SIMS` in `pattern_search/config.py` is the single throughput knob.
Pick the value that matches what your Brain account is allowed to run at once:

| Account tier        | Typical limit | `MAX_CONCURRENT_SIMS` |
|---------------------|---------------|-----------------------|
| **Non-consultant**  | 3             | `3` (default)         |
| Consultant          | ~8            | `8`                   |
| Higher tiers        | varies        | match your quota      |

Setting it higher than your account allows just produces 429/`limit-exceeded`
errors from the Brain API and slows you down — so leave it at `3` unless you
know you have headroom.

## Workshop notebooks

Two notebooks at the project root help students replicate the workflow:

| Notebook | What it does | API calls? |
|---|---|---|
| **`inspect_usa.ipynb`** | Full inspection of USA datafields (4416 MATRIX + 437 VECTOR), category-by-category. Includes a *recipe library* — 8 ready-to-paste template + category combos, search-space sizes, sample rendered expressions, and a check-failure → next-template diagnostic table. **Run this first** to pick a starting point. | No — local CSVs only |
| **`workshop.ipynb`** | 20-minute live walkthrough — auth → datafield tour → config → 2-minute mining run → DuckDB queries → iterate. | Yes — needs `credentials/pw` |

Open with `jupyter lab inspect_usa.ipynb` (install: `pip install jupyterlab`).

## Querying the alpha store

Use the bundled DuckDB helper to run SQL against `data/alphas.jsonl` directly —
no schema, no import step:

```bash
pip install duckdb
python query.py "SELECT id, \"is\".sharpe, \"is\".fitness FROM alphas \
                 WHERE \"is\".sharpe > 1.5 ORDER BY \"is\".sharpe DESC LIMIT 20"
```

Or interactively:

```bash
python query.py    # drops you into a DuckDB shell with `alphas` already bound
```

### Useful queries

**Find alphas with no FAILed checks** — the "submission-ready" set:

```sql
SELECT id,
       regular.code        AS expression,
       "is".sharpe,
       "is".fitness,
       "is".turnover,
       "is".returns,
       list_transform("is".checks, c -> c.name || '=' || c.result) AS checks
FROM alphas
WHERE NOT list_contains(list_transform("is".checks, c -> c.result), 'FAIL')
ORDER BY "is".sharpe DESC NULLS LAST;
```

**See where alphas are failing** — distribution of check results:

```sql
SELECT c.name, c.result, count(*) AS n
FROM alphas, UNNEST("is".checks) AS t(c)
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
```

**Closest near-misses** — alphas that fail the *fewest* checks:

```sql
SELECT id,
       regular.code AS expression,
       "is".sharpe,
       len(list_filter("is".checks, c -> c.result = 'FAIL')) AS n_fail,
       list_transform(
         list_filter("is".checks, c -> c.result = 'FAIL'),
         c -> c.name
       ) AS failed_checks
FROM alphas
ORDER BY n_fail ASC, "is".sharpe DESC NULLS LAST
LIMIT 20;
```

## Security notes

- `credentials/pw` and `credentials/brain_token.txt` are gitignored. **Never commit them.**
  If you ever pushed them, rotate your password immediately.
- The `data/` directory is gitignored — it holds outputs (`alphas.jsonl`, `failed_alphas.jsonl`,
  checkpoints) that should not be in version control.

## License

MIT (or specify your own).
