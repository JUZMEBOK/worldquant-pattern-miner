"""DuckDB query helper for data/alphas.jsonl.

Usage:
    python query.py "SELECT id, \"is\".sharpe FROM alphas WHERE \"is\".sharpe > 1.5"
    python query.py                              # interactive REPL
    python query.py --file data/other.jsonl ... # query a different file

Inside queries, the JSONL file is exposed as the view ``alphas``. Failed
fetches are exposed as ``failed`` (if data/failed_alphas.jsonl exists).
"""

import os
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    sys.exit("duckdb not installed. Run: pip install duckdb")

PROJECT_DIR = Path(__file__).resolve().parent
ALPHAS_FILE = PROJECT_DIR / "data" / "alphas.jsonl"
FAILED_FILE = PROJECT_DIR / "data" / "failed_alphas.jsonl"


def _connect(alphas_path: Path) -> "duckdb.DuckDBPyConnection":
    con = duckdb.connect(":memory:")
    if alphas_path.exists() and alphas_path.stat().st_size > 0:
        con.execute(
            f"CREATE VIEW alphas AS SELECT * FROM read_json_auto('{alphas_path}', format='newline_delimited')"
        )
    else:
        print(f"⚠️  {alphas_path} is missing or empty — `alphas` view not created.")
    if FAILED_FILE.exists() and FAILED_FILE.stat().st_size > 0:
        con.execute(
            f"CREATE VIEW failed AS SELECT * FROM read_json_auto('{FAILED_FILE}', format='newline_delimited')"
        )
    return con


def _print_table(rel) -> None:
    rel.show(max_width=200)


def _run_one(con, sql: str) -> int:
    try:
        rel = con.sql(sql)
        if rel is not None:
            _print_table(rel)
        return 0
    except Exception as e:
        print(f"❌ {e}")
        return 1


def _repl(con) -> None:
    print("DuckDB shell. Views: `alphas` (and `failed` if present). End queries with `;`. Ctrl-D to exit.")
    buf: list[str] = []
    while True:
        try:
            prompt = "> " if not buf else "  "
            line = input(prompt)
        except (EOFError, KeyboardInterrupt):
            print()
            return
        buf.append(line)
        if line.rstrip().endswith(";"):
            sql = "\n".join(buf).rstrip(";")
            # Forgiving: tolerate shell-escaped \" pasted back into the REPL.
            sql = sql.replace('\\"', '"')
            buf.clear()
            if sql.strip():
                _run_one(con, sql)


def main(argv: list[str]) -> int:
    alphas_path = ALPHAS_FILE
    args = list(argv)

    if "--file" in args:
        i = args.index("--file")
        alphas_path = Path(args[i + 1]).resolve()
        del args[i:i + 2]

    con = _connect(alphas_path)

    if not args:
        _repl(con)
        return 0

    sql = " ".join(args)
    return _run_one(con, sql)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
