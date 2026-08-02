"""The dashboard's pure chart builders, and the read-only warehouse it relies on."""

from datetime import UTC, datetime

import duckdb
import polars as pl
import pytest

from pedestrian_pipeline.dashboard import hourly_chart, ranked_bar
from pedestrian_pipeline.transform import build_fact_counts
from pedestrian_pipeline.warehouse import Warehouse
from tests.conftest import INGESTED_AT
from tests.test_transform import count


@pytest.fixture
def hourly_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "local_hour": [0, 1, 2],
            "pedestrians": [100, None, 300],
            "sensors_reporting": [10, None, 12],
            "avg_per_sensor_minute": [5.0, None, 7.5],
        }
    )


def test_hourly_chart_layers_points_over_the_area(hourly_frame):
    spec = hourly_chart(hourly_frame).to_dict()

    marks = [layer["mark"]["type"] for layer in spec["layer"]]
    assert marks == ["area", "point"]


def test_hourly_chart_pins_the_axis_to_a_full_day(hourly_frame):
    spec = hourly_chart(hourly_frame).to_dict()

    x = spec["layer"][0]["encoding"]["x"]
    assert x["scale"]["domain"] == [0, 23]
    assert x["scale"]["nice"] is False


def test_hourly_chart_plots_the_normalised_measure(hourly_frame):
    """Raw totals would make a partly-captured hour look like a traffic collapse."""
    spec = hourly_chart(hourly_frame).to_dict()

    assert spec["layer"][0]["encoding"]["y"]["field"] == "avg_per_sensor_minute"


def test_ranked_bar_sorts_by_value_descending():
    frame = pl.DataFrame({"sensor_description": ["a", "b"], "pedestrians": [1, 2]})

    spec = ranked_bar(frame, "pedestrians", "Pedestrians", 100).to_dict()

    assert spec["encoding"]["y"]["sort"] == "-x"
    assert spec["encoding"]["x"]["field"] == "pedestrians"


def test_read_only_warehouse_can_query_an_existing_file(tmp_path):
    database = tmp_path / "w.duckdb"
    with Warehouse(database) as writable:
        writable.upsert_counts(
            build_fact_counts(
                [count(3, datetime(2026, 8, 2, 2, 0, tzinfo=UTC), 1, 2)], INGESTED_AT
            ).frame
        )

    with Warehouse(database, read_only=True) as reader:
        assert reader.row_count("fact_pedestrian_count") == 1


def test_read_only_warehouse_refuses_writes(tmp_path):
    database = tmp_path / "w.duckdb"
    with Warehouse(database):
        pass

    with Warehouse(database, read_only=True) as reader, pytest.raises(duckdb.Error):
        reader.connection.execute("DELETE FROM fact_pedestrian_count")


def test_read_only_warehouse_does_not_create_a_missing_file(tmp_path):
    missing = tmp_path / "missing.duckdb"

    with pytest.raises(duckdb.Error), Warehouse(missing, read_only=True):
        pass

    assert not missing.exists()
