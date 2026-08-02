import httpx
import pytest
import respx

from pedestrian_pipeline.analytics import main as report_main
from pedestrian_pipeline.config import Settings
from pedestrian_pipeline.ingestion.run import main as ingest_main


@pytest.fixture
def api(raw_count, raw_sensor):
    settings = Settings()
    with respx.mock:
        respx.get(settings.dataset_export_url(settings.sensors_dataset)).mock(
            return_value=httpx.Response(200, json=[raw_sensor])
        )
        respx.get(settings.dataset_export_url(settings.counts_dataset)).mock(
            return_value=httpx.Response(200, json=[raw_count])
        )
        yield


def test_ingest_cli_writes_a_warehouse_and_exits_zero(tmp_path, api, capsys):
    database = tmp_path / "pedestrian.duckdb"

    exit_code = ingest_main(["--database", str(database), "--log-level", "ERROR"])

    assert exit_code == 0
    assert database.exists()
    assert "ingestion summary" in capsys.readouterr().out


def test_ingest_cli_exits_non_zero_when_the_quality_gate_fails(tmp_path, capsys):
    settings = Settings()
    database = tmp_path / "pedestrian.duckdb"

    with respx.mock:
        respx.get(settings.dataset_export_url(settings.sensors_dataset)).mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(settings.dataset_export_url(settings.counts_dataset)).mock(
            return_value=httpx.Response(200, json=[])
        )
        exit_code = ingest_main(["--database", str(database), "--log-level", "ERROR"])

    assert exit_code == 1
    assert "ERROR" in capsys.readouterr().out


def test_report_cli_prints_query_results(tmp_path, api, capsys):
    database = tmp_path / "pedestrian.duckdb"
    ingest_main(["--database", str(database), "--log-level", "ERROR"])
    capsys.readouterr()

    exit_code = report_main(["busiest_sensors", "--database", str(database)])

    assert exit_code == 0
    assert "busiest_sensors" in capsys.readouterr().out


def test_report_cli_can_list_queries(capsys):
    exit_code = report_main(["--list"])

    assert exit_code == 0
    assert "hourly_profile" in capsys.readouterr().out


def test_report_cli_errors_when_no_warehouse_exists(tmp_path, capsys):
    exit_code = report_main(["--database", str(tmp_path / "missing.duckdb")])

    assert exit_code == 1
    assert "run `ingest` first" in capsys.readouterr().err
