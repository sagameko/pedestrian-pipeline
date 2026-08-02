from datetime import UTC, datetime

import pytest

from pedestrian_pipeline.models import SensorLocation
from pedestrian_pipeline.quality import Severity, run_checks
from pedestrian_pipeline.transform import build_dim_sensor, build_fact_counts
from tests.conftest import INGESTED_AT
from tests.test_transform import count


@pytest.fixture
def loaded(warehouse, raw_sensor):
    sensor = SensorLocation.model_validate(raw_sensor)
    warehouse.upsert_sensors(build_dim_sensor([sensor], INGESTED_AT))
    readings = [count(3, datetime(2026, 8, 2, 2, minute, tzinfo=UTC), 1, 2) for minute in range(5)]
    warehouse.upsert_counts(build_fact_counts(readings, INGESTED_AT).frame)
    return warehouse


def result_for(report, name):
    return next(r for r in report.results if r.check.name == name)


def test_a_healthy_load_passes_every_check(loaded):
    report = run_checks(loaded)

    assert report.passed
    assert report.failures == []


def test_an_empty_warehouse_fails(warehouse):
    report = run_checks(warehouse)

    assert not report.passed
    assert not result_for(report, "fact_table_not_empty").passed


def test_orphaned_readings_are_caught(loaded):
    loaded.connection.execute("DELETE FROM dim_sensor")

    report = run_checks(loaded)

    assert not report.passed
    assert not result_for(report, "every_reading_has_a_known_sensor").passed


def test_direction_arithmetic_is_asserted_at_the_warehouse(loaded):
    loaded.connection.execute("UPDATE fact_pedestrian_count SET total_of_directions = 999")

    report = run_checks(loaded)

    failed = result_for(report, "directions_sum_to_total")
    assert not failed.passed
    assert failed.check.severity is Severity.ERROR


def test_inconsistent_local_hour_is_caught(loaded):
    loaded.connection.execute("UPDATE fact_pedestrian_count SET local_hour = 23")

    report = run_checks(loaded)

    assert not result_for(report, "local_hour_matches_local_datetime").passed


def test_future_readings_only_warn(loaded):
    loaded.connection.execute(
        "UPDATE fact_pedestrian_count SET sensing_datetime = now() + INTERVAL 2 HOUR "
        "WHERE sensing_datetime = (SELECT max(sensing_datetime) FROM fact_pedestrian_count)"
    )

    report = run_checks(loaded)

    warning = result_for(report, "no_readings_from_the_future")
    assert not warning.passed
    assert warning.check.severity is Severity.WARNING
    assert report.passed
