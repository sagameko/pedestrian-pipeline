from datetime import UTC, datetime

from pedestrian_pipeline.models import PedestrianCount, SensorLocation
from pedestrian_pipeline.transform import (
    DIM_COLUMNS,
    FACT_COLUMNS,
    build_dim_sensor,
    build_fact_counts,
)
from tests.conftest import INGESTED_AT


def count(location_id: int, when: datetime, d1: int, d2: int) -> PedestrianCount:
    return PedestrianCount(
        location_id=location_id,
        sensing_datetime=when,
        direction_1=d1,
        direction_2=d2,
        total_of_directions=d1 + d2,
    )


def test_builds_the_expected_fact_columns(raw_count):
    result = build_fact_counts([PedestrianCount.model_validate(raw_count)], INGESTED_AT)

    assert result.frame.columns == FACT_COLUMNS
    assert result.frame.height == 1


def test_restated_readings_collapse_to_the_highest_total():
    when = datetime(2026, 8, 2, 2, 0, tzinfo=UTC)
    readings = [count(11, when, 2, 1), count(11, when, 10, 4)]

    result = build_fact_counts(readings, INGESTED_AT)

    assert result.frame.height == 1
    assert result.restated_keys == 1
    assert result.rows_discarded == 1
    assert result.frame["total_of_directions"].to_list() == [14]


def test_distinct_keys_are_never_collapsed():
    when = datetime(2026, 8, 2, 2, 0, tzinfo=UTC)
    readings = [count(11, when, 2, 1), count(12, when, 10, 4)]

    result = build_fact_counts(readings, INGESTED_AT)

    assert result.frame.height == 2
    assert result.restated_keys == 0


def test_local_time_uses_aest_in_winter():
    reading = count(3, datetime(2026, 8, 2, 2, 55, tzinfo=UTC), 1, 1)

    frame = build_fact_counts([reading], INGESTED_AT).frame

    assert str(frame["local_datetime"][0]) == "2026-08-02 12:55:00+10:00"
    assert frame["local_hour"][0] == 12


def test_local_time_uses_aedt_in_summer():
    """A hardcoded +10 offset would put this an hour out."""
    reading = count(3, datetime(2026, 1, 2, 2, 55, tzinfo=UTC), 1, 1)

    frame = build_fact_counts([reading], INGESTED_AT).frame

    assert str(frame["local_datetime"][0]) == "2026-01-02 13:55:00+11:00"
    assert frame["local_hour"][0] == 13


def test_empty_input_yields_an_empty_frame_with_the_right_schema():
    result = build_fact_counts([], INGESTED_AT)

    assert result.frame.is_empty()
    assert result.frame.columns == FACT_COLUMNS
    assert result.restated_keys == 0


def test_builds_the_expected_dim_columns(raw_sensor):
    frame = build_dim_sensor([SensorLocation.model_validate(raw_sensor)], INGESTED_AT)

    assert frame.columns == DIM_COLUMNS
    assert frame["direction_1_label"].to_list() == ["North"]


def test_duplicate_sensors_are_deduplicated(raw_sensor):
    sensor = SensorLocation.model_validate(raw_sensor)

    frame = build_dim_sensor([sensor, sensor], INGESTED_AT)

    assert frame.height == 1


def test_empty_sensors_yield_an_empty_dim_frame():
    frame = build_dim_sensor([], INGESTED_AT)

    assert frame.is_empty()
    assert frame.columns == DIM_COLUMNS
