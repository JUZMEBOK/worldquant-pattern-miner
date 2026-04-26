# worldquant-pattern-miner

Pattern-mining toolkit for the [WorldQuant Brain](https://platform.worldquantbrain.com/) platform.
Discovers data fields, generates alpha expression candidates from templates, simulates them
concurrently, and persists results.

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
│   ├── datafields_query.py    # fetches data-field catalogs from the Brain API
│   └── {REGION}/              # CSV catalogs (VECTOR{D}.csv, MATRIX{D}.csv, GROUP{D}.csv)
├── data/                      # outputs & checkpoints (gitignored)
├── requirements.txt
└── .gitignore
```

All scripts anchor paths to the project root via `Path(__file__).resolve().parent[.parent]`,
so they run correctly regardless of the current working directory.

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

1. **Credentials** — copy the template and fill in your Brain login:
   ```bash
   cp credentials/pw.example credentials/pw
   chmod 600 credentials/pw
   ```
   `pw` must be a JSON array: `["your_email", "your_password"]`.

2. **Token refresh** — generates `credentials/brain_token.txt`:
   ```bash
   python credentials/token_refresh.py
   ```
   On first run you may need to complete biometric (Persona) verification in your browser;
   the script polls until the JWT is issued.

3. **Data-field catalogs** — populate `datafields/{REGION}/`:
   ```bash
   python datafields/datafields_query.py
   ```
   Tweak the target region/universe/delay in `DEFAULT_UNIVERSAL_CONFIG` inside the script,
   or override via env vars (`UNIVERSE`, `WORKERS`, `DEPTH`, `CAP`, `PAGE_SIZE`, …).

## Running

```bash
python pattern_search.py
```

`pattern_search.runner.main_loop` loads CSV catalogs from `datafields/{REGION}/`,
streams expression candidates from `EXPRESSION_TEMPLATE` (configured in
`pattern_search/config.py`), and submits simulations concurrently against the
Brain API. Successful alphas are appended verbatim as JSON Lines to
`data/alphas.jsonl` by `pattern_search/db.py`.

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

## Security notes

- `credentials/pw`, `credentials/brain_token.txt`, and `credentials/discord.json` are
  gitignored. **Never commit them.** If you ever pushed them, rotate the secrets.
- The `data/` directory is gitignored — it holds outputs, checkpoints, and SQLite/CSV
  artefacts that should not be in version control.

## License

MIT (or specify your own).
