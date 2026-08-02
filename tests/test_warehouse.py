from datetime import UTC, datetime

from pedestrian_pipeline.models import PedestrianCount, SensorLocation
from pedestrian_pipeline.transform import build_dim_sensor, build_fact_counts
from tests.conftest import INGESTED_AT
from tests.test_transform import count


def test_schema_is_created_on_connect(warehouse):
    assert warehouse.row_count("dim_sensor") == 0
    assert warehouse.row_count("fact_pedestrian_count") == 0


def test_counts_load(warehouse):
    frame = build_fact_counts([count(3, datetime(2026, 8, 2, 2, 0, tzinfo=UTC), 1, 2)], INGESTED_AT)

    result = warehouse.upsert_counts(frame.frame)

    assert result.rows_supplied == 1
    assert result.rows_after == 1


def test_reingesting_the_same_window_does_not_duplicate_rows(warehouse):
    readings = [
        count(3, datetime(2026, 8, 2, 2, 0, tzinfo=UTC), 1, 2),
        count(3, datetime(2026, 8, 2, 2, 1, tzinfo=UTC), 3, 4),
    ]
    frame = build_fact_counts(readings, INGESTED_AT).frame

    warehouse.upsert_counts(frame)
    warehouse.upsert_counts(frame)
    warehouse.upsert_counts(frame)

    assert warehouse.row_count("fact_pedestrian_count") == 2


def test_a_restated_reading_overwrites_the_stored_value(warehouse):
    when = datetime(2026, 8, 2, 2, 0, tzinfo=UTC)
    warehouse.upsert_counts(build_fact_counts([count(3, when, 1, 2)], INGESTED_AT).frame)
    warehouse.upsert_counts(build_fact_counts([count(3, when, 10, 20)], INGESTED_AT).frame)

    stored = warehouse.query("SELECT total_of_directions FROM fact_pedestrian_count")

    assert warehouse.row_count("fact_pedestrian_count") == 1
    assert stored["total_of_directions"].to_list() == [30]


def test_loading_an_empty_frame_is_a_no_op(warehouse):
    result = warehouse.upsert_counts(build_fact_counts([], INGESTED_AT).frame)

    assert result.rows_supplied == 0
    assert warehouse.row_count("fact_pedestrian_count") == 0


def test_sensors_upsert_by_location_id(warehouse, raw_sensor):
    sensor = SensorLocation.model_validate(raw_sensor)
    renamed = SensorLocation.model_validate(raw_sensor | {"sensor_description": "Renamed"})

    warehouse.upsert_sensors(build_dim_sensor([sensor], INGESTED_AT))
    warehouse.upsert_sensors(build_dim_sensor([renamed], INGESTED_AT))

    stored = warehouse.query("SELECT sensor_description FROM dim_sensor")

    assert warehouse.row_count("dim_sensor") == 1
    assert stored["sensor_description"].to_list() == ["Renamed"]


def test_timestamps_survive_a_round_trip(warehouse):
    when = datetime(2026, 8, 2, 2, 55, tzinfo=UTC)
    warehouse.upsert_counts(build_fact_counts([count(3, when, 1, 2)], INGESTED_AT).frame)

    stored = warehouse.query("SELECT sensing_datetime FROM fact_pedestrian_count")

    assert stored["sensing_datetime"][0] == when


def test_readings_are_stored_with_a_parsed_pydantic_model(warehouse, raw_count):
    reading = PedestrianCount.model_validate(raw_count)

    warehouse.upsert_counts(build_fact_counts([reading], INGESTED_AT).frame)

    assert warehouse.row_count("fact_pedestrian_count") == 1
