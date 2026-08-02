from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pedestrian_pipeline.config import Settings
from pedestrian_pipeline.warehouse import Warehouse

INGESTED_AT = datetime(2026, 8, 2, 3, 0, tzinfo=UTC)


@pytest.fixture
def settings() -> Settings:
    return Settings(
        database_path=Path(":memory:"),
        max_retries=2,
        backoff_base_seconds=0.001,
        request_timeout_seconds=5.0,
    )


@pytest.fixture
def warehouse() -> Iterator[Warehouse]:
    with Warehouse(Path(":memory:")) as wh:
        yield wh


@pytest.fixture
def raw_count() -> dict:
    return {
        "location_id": 3,
        "sensing_datetime": "2026-08-02T02:55:00+00:00",
        "sensing_date": "2026-08-02",
        "sensing_time": "12:55",
        "direction_1": 19,
        "direction_2": 34,
        "total_of_directions": 53,
    }


@pytest.fixture
def raw_sensor() -> dict:
    return {
        "location_id": 3,
        "sensor_description": "Melbourne Central",
        "sensor_name": "Swa295_T",
        "installation_date": "2009-03-25",
        "note": None,
        "location_type": "Outdoor",
        "status": "A",
        "direction_1": "North",
        "direction_2": "South",
        "latitude": -37.81101524,
        "longitude": 144.96429485,
        "location": {"lon": 144.96429485, "lat": -37.81101524},
    }
