"""DuckDB warehouse: star schema plus idempotent upserts."""

import logging
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType

import duckdb
import polars as pl

from pedestrian_pipeline.config import MELBOURNE_TZ
from pedestrian_pipeline.transform import DIM_COLUMNS, FACT_COLUMNS

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dim_sensor (
    location_id        INTEGER     NOT NULL,
    sensor_description VARCHAR     NOT NULL,
    sensor_name        VARCHAR     NOT NULL,
    status             VARCHAR     NOT NULL,
    latitude           DOUBLE      NOT NULL,
    longitude          DOUBLE      NOT NULL,
    location_type      VARCHAR,
    installation_date  DATE,
    direction_1_label  VARCHAR,
    direction_2_label  VARCHAR,
    note               VARCHAR,
    ingested_at        TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (location_id)
);

CREATE TABLE IF NOT EXISTS fact_pedestrian_count (
    location_id         INTEGER     NOT NULL,
    sensing_datetime    TIMESTAMPTZ NOT NULL,
    local_datetime      TIMESTAMPTZ NOT NULL,
    local_date          DATE        NOT NULL,
    local_hour          INTEGER     NOT NULL,
    direction_1         INTEGER     NOT NULL,
    direction_2         INTEGER     NOT NULL,
    total_of_directions INTEGER     NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (location_id, sensing_datetime)
);
"""


@dataclass(frozen=True)
class LoadResult:
    table: str
    rows_supplied: int
    rows_after: int


class Warehouse:
    def __init__(self, database_path: Path, *, read_only: bool = False) -> None:
        self._database_path = database_path
        self._read_only = read_only
        self._connection: duckdb.DuckDBPyConnection | None = None

    def __enter__(self) -> "Warehouse":
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def connect(self) -> duckdb.DuckDBPyConnection:
        if self._connection is None:
            # Read-only lets readers (the dashboard) share the file with a writer
            # instead of contending for DuckDB's exclusive lock, and makes it
            # impossible for a reader to alter the warehouse.
            if self._read_only:
                self._connection = duckdb.connect(str(self._database_path), read_only=True)
                self._pin_timezone()
                return self._connection

            if str(self._database_path) != ":memory:":
                self._database_path.parent.mkdir(parents=True, exist_ok=True)
            self._connection = duckdb.connect(str(self._database_path))
            self._pin_timezone()
            self._connection.execute(SCHEMA_SQL)
        return self._connection

    def _pin_timezone(self) -> None:
        """Render TIMESTAMPTZ in Melbourne time regardless of the host clock.

        DuckDB resolves TIMESTAMPTZ against the session timezone, which it takes
        from the OS. Left alone, `extract('hour' FROM local_datetime)` returns a
        Melbourne hour on a Melbourne machine and a UTC hour on a UTC CI runner,
        so identical data yields different query results per host.
        """
        assert self._connection is not None
        self._connection.execute(f"SET TimeZone = '{MELBOURNE_TZ}'")

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return self.connect()

    def upsert_sensors(self, frame: pl.DataFrame) -> LoadResult:
        return self._upsert("dim_sensor", frame, DIM_COLUMNS, ["location_id"])

    def upsert_counts(self, frame: pl.DataFrame) -> LoadResult:
        return self._upsert(
            "fact_pedestrian_count",
            frame,
            FACT_COLUMNS,
            ["location_id", "sensing_datetime"],
        )

    def _upsert(
        self, table: str, frame: pl.DataFrame, columns: list[str], key: list[str]
    ) -> LoadResult:
        connection = self.connection

        if frame.is_empty():
            return LoadResult(table=table, rows_supplied=0, rows_after=self.row_count(table))

        updatable = [c for c in columns if c not in key]
        assignments = ", ".join(f"{c} = excluded.{c}" for c in updatable)
        column_list = ", ".join(columns)

        connection.register("_staged", frame.select(columns))
        try:
            connection.execute(
                f"INSERT INTO {table} ({column_list}) SELECT {column_list} FROM _staged "
                f"ON CONFLICT ({', '.join(key)}) DO UPDATE SET {assignments}"
            )
        finally:
            connection.unregister("_staged")

        result = LoadResult(
            table=table, rows_supplied=frame.height, rows_after=self.row_count(table)
        )
        logger.info(
            "%s: upserted %s rows, %s total", table, result.rows_supplied, result.rows_after
        )
        return result

    def row_count(self, table: str) -> int:
        row = self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()
        return int(row[0]) if row else 0

    def query(self, sql: str) -> pl.DataFrame:
        return self.connection.execute(sql).pl()
