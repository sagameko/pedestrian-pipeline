"""The warehouse must give identical answers on a Melbourne laptop and a UTC CI runner.

DuckDB resolves TIMESTAMPTZ against the session timezone, which it inherits from
the OS. These tests run under a forced non-Melbourne TZ, which is what caught the
original bug: the quality gate passed locally and failed in CI on the same data.
"""

import os
import time
from datetime import UTC, datetime

import pytest

from pedestrian_pipeline.config import MELBOURNE_TZ
from pedestrian_pipeline.models import SensorLocation
from pedestrian_pipeline.quality import run_checks
from pedestrian_pipeline.transform import build_dim_sensor, build_fact_counts
from tests.conftest import INGESTED_AT
from tests.test_transform import count


@pytest.fixture(params=["UTC", "America/New_York", MELBOURNE_TZ])
def host_timezone(request, monkeypatch):
    """Pretend the host clock is somewhere else for the duration of a test."""
    monkeypatch.setenv("TZ", request.param)
    time.tzset()
    yield request.param
    monkeypatch.delenv("TZ", raising=False)
    time.tzset()


def test_session_timezone_is_pinned_whatever_the_host_says(host_timezone, warehouse):
    setting = warehouse.connection.execute("SELECT current_setting('TimeZone')").fetchone()

    assert setting[0] == MELBOURNE_TZ


def test_local_hour_survives_a_round_trip_under_any_host_timezone(host_timezone, warehouse):
    # 02:55 UTC is 12:55 in Melbourne; a UTC session would extract 2, not 12.
    reading = count(3, datetime(2026, 8, 2, 2, 55, tzinfo=UTC), 1, 2)
    warehouse.upsert_counts(build_fact_counts([reading], INGESTED_AT).frame)

    row = warehouse.connection.execute(
        "SELECT local_hour, extract('hour' FROM local_datetime) FROM fact_pedestrian_count"
    ).fetchone()

    assert row[0] == 12
    assert row[1] == 12


def test_quality_gate_passes_under_any_host_timezone(host_timezone, warehouse, raw_sensor):
    warehouse.upsert_sensors(
        build_dim_sensor([SensorLocation.model_validate(raw_sensor)], INGESTED_AT)
    )
    readings = [count(3, datetime(2026, 8, 2, 2, m, tzinfo=UTC), 1, 2) for m in range(5)]
    warehouse.upsert_counts(build_fact_counts(readings, INGESTED_AT).frame)

    report = run_checks(warehouse)

    assert report.passed, [r.check.name for r in report.failures]


def test_the_environment_variable_actually_took_effect(host_timezone):
    """Guards the fixture itself — a no-op fixture would make these tests vacuous."""
    assert os.environ["TZ"] == host_timezone
