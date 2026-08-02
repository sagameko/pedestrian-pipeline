"""Pipeline entrypoint: fetch, validate, transform, load, then assert quality."""

import argparse
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path

from pedestrian_pipeline.config import Settings
from pedestrian_pipeline.models import (
    PedestrianCount,
    RejectedRecord,
    SensorLocation,
    parse_records,
    utcnow,
)
from pedestrian_pipeline.quality import QualityReport, run_checks
from pedestrian_pipeline.sources import MelbourneOpenData
from pedestrian_pipeline.transform import build_dim_sensor, build_fact_counts
from pedestrian_pipeline.warehouse import Warehouse

logger = logging.getLogger(__name__)


@dataclass
class RunSummary:
    sensors_loaded: int = 0
    readings_fetched: int = 0
    readings_loaded: int = 0
    restated_keys: int = 0
    rejected: list[RejectedRecord] = field(default_factory=list)
    fact_rows_total: int = 0
    quality: QualityReport | None = None

    @property
    def succeeded(self) -> bool:
        return self.quality is not None and self.quality.passed

    def render(self) -> str:
        lines = [
            "ingestion summary",
            f"  sensors loaded      {self.sensors_loaded}",
            f"  readings fetched    {self.readings_fetched}",
            f"  readings loaded     {self.readings_loaded}",
            f"  restated keys       {self.restated_keys}",
            f"  rejected records    {len(self.rejected)}",
            f"  fact rows in store  {self.fact_rows_total}",
        ]
        if self.quality is not None:
            for result in self.quality.results:
                mark = "pass" if result.passed else result.check.severity.value.upper()
                lines.append(f"  [{mark:>7}] {result.check.name}")
        return "\n".join(lines)


def ingest(settings: Settings, warehouse: Warehouse, source: MelbourneOpenData) -> RunSummary:
    summary = RunSummary()
    ingested_at = utcnow()

    raw_sensors = source.fetch_dataset(settings.sensors_dataset)
    sensors, rejected_sensors = parse_records(raw_sensors, SensorLocation)
    summary.rejected.extend(rejected_sensors)
    summary.sensors_loaded = warehouse.upsert_sensors(
        build_dim_sensor(sensors, ingested_at)
    ).rows_supplied

    raw_counts = source.fetch_dataset(settings.counts_dataset)
    summary.readings_fetched = len(raw_counts)
    counts, rejected_counts = parse_records(raw_counts, PedestrianCount)
    summary.rejected.extend(rejected_counts)

    transformed = build_fact_counts(counts, ingested_at)
    summary.restated_keys = transformed.restated_keys
    load = warehouse.upsert_counts(transformed.frame)
    summary.readings_loaded = load.rows_supplied
    summary.fact_rows_total = load.rows_after

    for rejection in summary.rejected[:10]:
        logger.warning("rejected record: %s", rejection.reason)

    summary.quality = run_checks(warehouse)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ingest",
        description="Ingest Melbourne CBD pedestrian sensor readings into DuckDB.",
    )
    parser.add_argument("--database", type=Path, help="path to the DuckDB file")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    settings = Settings()
    if args.database:
        settings = settings.model_copy(update={"database_path": args.database})

    with Warehouse(settings.database_path) as warehouse, MelbourneOpenData(settings) as source:
        summary = ingest(settings, warehouse, source)

    print(summary.render())

    if not summary.succeeded:
        logger.error("quality gate failed; see failures above")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
