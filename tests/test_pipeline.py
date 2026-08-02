"""End-to-end run against a mocked API, plus the analytical queries."""

import httpx
import pytest
import respx

from pedestrian_pipeline.analytics import load_queries, run_report
from pedestrian_pipeline.ingestion.run import build_parser, ingest
from pedestrian_pipeline.sources import MelbourneOpenData


@pytest.fixture
def api(settings, raw_count, raw_sensor):
    readings = [
        raw_count | {"sensing_datetime": f"2026-08-02T02:{minute:02d}:00+00:00"}
        for minute in range(10)
    ]
    # A restated reading: same key as the first, higher total.
    readings.append(
        raw_count
        | {
            "sensing_datetime": "2026-08-02T02:00:00+00:00",
            "direction_1": 100,
            "direction_2": 100,
            "total_of_directions": 200,
        }
    )
    # An invalid reading that must be quarantined, not loaded.
    readings.append(raw_count | {"total_of_directions": 1})

    with respx.mock:
        respx.get(settings.dataset_export_url(settings.sensors_dataset)).mock(
            return_value=httpx.Response(200, json=[raw_sensor])
        )
        respx.get(settings.dataset_export_url(settings.counts_dataset)).mock(
            return_value=httpx.Response(200, json=readings)
        )
        yield


def test_full_run_loads_validates_and_passes_quality(settings, warehouse, api):
    with MelbourneOpenData(settings) as source:
        summary = ingest(settings, warehouse, source)

    assert summary.succeeded
    assert summary.sensors_loaded == 1
    assert summary.readings_fetched == 12
    assert summary.readings_loaded == 10
    assert summary.restated_keys == 1
    assert len(summary.rejected) == 1
    assert summary.fact_rows_total == 10


def test_the_restated_reading_wins(settings, warehouse, api):
    with MelbourneOpenData(settings) as source:
        ingest(settings, warehouse, source)

    stored = warehouse.query(
        "SELECT total_of_directions FROM fact_pedestrian_count "
        "WHERE sensing_datetime = '2026-08-02T02:00:00+00:00'"
    )

    assert stored["total_of_directions"].to_list() == [200]


def test_a_second_run_is_idempotent(settings, warehouse, api):
    with MelbourneOpenData(settings) as source:
        first = ingest(settings, warehouse, source)
        second = ingest(settings, warehouse, source)

    assert first.fact_rows_total == second.fact_rows_total


def test_summary_renders_without_error(settings, warehouse, api):
    with MelbourneOpenData(settings) as source:
        summary = ingest(settings, warehouse, source)

    rendered = summary.render()

    assert "readings loaded" in rendered
    assert "fact_table_not_empty" in rendered


def test_every_analytical_query_runs(settings, warehouse, api):
    with MelbourneOpenData(settings) as source:
        ingest(settings, warehouse, source)

    results = run_report(warehouse)

    assert set(results) == set(load_queries())
    assert all(frame.height > 0 for frame in results.values())


def test_unknown_query_names_are_rejected(warehouse):
    with pytest.raises(KeyError, match="nonexistent"):
        run_report(warehouse, ["nonexistent"])


def test_queries_file_parses_into_named_statements():
    queries = load_queries()

    assert "busiest_sensors" in queries
    assert queries["busiest_sensors"].rstrip().endswith(";")
    assert all(not sql.lstrip().startswith("-- name:") for sql in queries.values())


def test_cli_parses_arguments():
    args = build_parser().parse_args(["--log-level", "DEBUG"])

    assert args.log_level == "DEBUG"
    assert args.database is None
