"""Named analytical queries loaded from queries.sql."""

import argparse
import logging
import re
import sys
from pathlib import Path

import polars as pl

from pedestrian_pipeline.config import Settings
from pedestrian_pipeline.warehouse import Warehouse

QUERIES_PATH = Path(__file__).with_name("queries.sql")

_NAME_MARKER = re.compile(r"^--\s*name:\s*([a-z0-9_]+)\s*$", re.MULTILINE)


def load_queries(path: Path = QUERIES_PATH) -> dict[str, str]:
    """Split queries.sql on its `-- name:` markers."""
    text = path.read_text(encoding="utf-8")
    markers = list(_NAME_MARKER.finditer(text))

    queries: dict[str, str] = {}
    for index, marker in enumerate(markers):
        end = markers[index + 1].start() if index + 1 < len(markers) else len(text)
        body = text[marker.end() : end].strip()
        if body:
            queries[marker.group(1)] = body

    return queries


def run_report(warehouse: Warehouse, names: list[str] | None = None) -> dict[str, pl.DataFrame]:
    queries = load_queries()
    selected = names or list(queries)

    unknown = [n for n in selected if n not in queries]
    if unknown:
        raise KeyError(f"unknown queries: {', '.join(sorted(unknown))}")

    return {name: warehouse.query(queries[name]) for name in selected}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="report", description="Run analytical queries against the warehouse."
    )
    parser.add_argument("queries", nargs="*", help="query names; defaults to all")
    parser.add_argument("--database", type=Path, help="path to the DuckDB file")
    parser.add_argument("--list", action="store_true", help="list available queries and exit")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.WARNING)

    if args.list:
        for name in load_queries():
            print(name)
        return 0

    settings = Settings()
    database = args.database or settings.database_path
    if not database.exists():
        print(f"no warehouse at {database}; run `ingest` first", file=sys.stderr)
        return 1

    with (
        Warehouse(database) as warehouse,
        pl.Config(tbl_rows=20, tbl_cols=12, tbl_hide_dataframe_shape=True),
    ):
        for name, frame in run_report(warehouse, args.queries or None).items():
            print(f"\n=== {name} ===")
            print(frame)

    return 0


if __name__ == "__main__":
    sys.exit(main())
