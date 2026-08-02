from datetime import UTC, date, datetime

from pedestrian_pipeline.models import (
    PedestrianCount,
    SensorLocation,
    parse_records,
)


def test_parses_a_well_formed_reading(raw_count):
    count = PedestrianCount.model_validate(raw_count)

    assert count.location_id == 3
    assert count.sensing_datetime == datetime(2026, 8, 2, 2, 55, tzinfo=UTC)
    assert count.total_of_directions == 53


def test_rejects_a_reading_whose_directions_do_not_sum(raw_count):
    broken = raw_count | {"total_of_directions": 99}

    valid, rejected = parse_records([broken], PedestrianCount)

    assert valid == []
    assert len(rejected) == 1
    assert "do not sum" in rejected[0].reason


def test_rejects_negative_counts(raw_count):
    broken = raw_count | {"direction_1": -1, "direction_2": 0, "total_of_directions": -1}

    valid, rejected = parse_records([broken], PedestrianCount)

    assert valid == []
    assert rejected[0].record is broken


def test_rejects_naive_timestamps(raw_count):
    broken = raw_count | {"sensing_datetime": "2026-08-02T02:55:00"}

    valid, rejected = parse_records([broken], PedestrianCount)

    assert valid == []


def test_one_bad_record_does_not_discard_the_good_ones(raw_count):
    good = raw_count
    bad = raw_count | {"location_id": 4, "total_of_directions": 1}

    valid, rejected = parse_records([good, bad, good], PedestrianCount)

    assert len(valid) == 2
    assert len(rejected) == 1


def test_sensor_direction_columns_are_renamed_to_labels(raw_sensor):
    sensor = SensorLocation.model_validate(raw_sensor)

    assert sensor.direction_1_label == "North"
    assert sensor.direction_2_label == "South"
    assert sensor.installation_date == date(2009, 3, 25)
    assert not hasattr(sensor, "location")


def test_sensor_tolerates_missing_optional_fields(raw_sensor):
    sparse = raw_sensor | {"direction_1": None, "direction_2": None, "installation_date": None}

    sensor = SensorLocation.model_validate(sparse)

    assert sensor.direction_1_label is None
    assert sensor.installation_date is None


def test_sensor_rejects_out_of_range_coordinates(raw_sensor):
    broken = raw_sensor | {"latitude": -999.0}

    valid, rejected = parse_records([broken], SensorLocation)

    assert valid == []
    assert "latitude" in rejected[0].reason
