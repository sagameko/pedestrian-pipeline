"""Post-load data quality gate."""

import logging
from dataclasses import dataclass
from enum import StrEnum

from pedestrian_pipeline.warehouse import Warehouse

logger = logging.getLogger(__name__)


class Severity(StrEnum):
    ERROR = "error"
    WARNING = "warning"


@dataclass(frozen=True)
class Check:
    name: str
    severity: Severity
    sql: str
    detail: str


@dataclass(frozen=True)
class CheckResult:
    check: Check
    offending_rows: int

    @property
    def passed(self) -> bool:
        return self.offending_rows == 0


@dataclass(frozen=True)
class QualityReport:
    results: list[CheckResult]

    @property
    def failures(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed]

    @property
    def errors(self) -> list[CheckResult]:
        return [r for r in self.failures if r.check.severity is Severity.ERROR]

    @property
    def passed(self) -> bool:
        return not self.errors


CHECKS: tuple[Check, ...] = (
    Check(
        name="fact_table_not_empty",
        severity=Severity.ERROR,
        sql="SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM fact_pedestrian_count",
        detail="the fact table holds no rows",
    ),
    Check(
        name="every_reading_has_a_known_sensor",
        severity=Severity.ERROR,
        sql=(
            "SELECT count(*) FROM fact_pedestrian_count f "
            "LEFT JOIN dim_sensor d USING (location_id) WHERE d.location_id IS NULL"
        ),
        detail="readings reference a location_id missing from dim_sensor",
    ),
    Check(
        name="directions_sum_to_total",
        severity=Severity.ERROR,
        sql=(
            "SELECT count(*) FROM fact_pedestrian_count "
            "WHERE direction_1 + direction_2 <> total_of_directions"
        ),
        detail="direction counts disagree with the reported total",
    ),
    Check(
        name="counts_are_non_negative",
        severity=Severity.ERROR,
        sql=(
            "SELECT count(*) FROM fact_pedestrian_count "
            "WHERE least(direction_1, direction_2, total_of_directions) < 0"
        ),
        detail="negative pedestrian counts recorded",
    ),
    Check(
        name="local_hour_matches_local_datetime",
        severity=Severity.ERROR,
        sql=(
            "SELECT count(*) FROM fact_pedestrian_count "
            "WHERE local_hour <> extract('hour' FROM local_datetime)"
        ),
        detail="derived local_hour is inconsistent with local_datetime",
    ),
    Check(
        name="no_readings_from_the_future",
        severity=Severity.WARNING,
        sql=(
            "SELECT count(*) FROM fact_pedestrian_count "
            "WHERE sensing_datetime > now() + INTERVAL 1 HOUR"
        ),
        detail="readings are timestamped beyond the current time",
    ),
    Check(
        name="most_sensors_are_reporting",
        severity=Severity.WARNING,
        sql=(
            "SELECT CASE WHEN (SELECT count(DISTINCT location_id) FROM fact_pedestrian_count) "
            "< (SELECT count(*) FROM dim_sensor) * 0.5 THEN 1 ELSE 0 END"
        ),
        detail="fewer than half of known sensors have reported any reading",
    ),
)


def run_checks(warehouse: Warehouse, checks: tuple[Check, ...] = CHECKS) -> QualityReport:
    results: list[CheckResult] = []

    for check in checks:
        row = warehouse.connection.execute(check.sql).fetchone()
        offending = int(row[0]) if row and row[0] is not None else 0
        result = CheckResult(check=check, offending_rows=offending)
        results.append(result)

        if result.passed:
            logger.debug("quality check passed: %s", check.name)
        else:
            log = logger.error if check.severity is Severity.ERROR else logger.warning
            log("quality check failed: %s (%s rows) - %s", check.name, offending, check.detail)

    return QualityReport(results=results)
