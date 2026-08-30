import httpx
import pytest
import respx

from pedestrian_pipeline.sources import MelbourneOpenData, OpenDataError, QuotaExceededError

DATASET = "pedestrian-counting-system-past-hour-counts-per-minute"


def export_url(settings) -> str:
    return settings.dataset_export_url(DATASET)


@respx.mock
def test_fetches_a_dataset(settings, raw_count):
    respx.get(export_url(settings)).mock(return_value=httpx.Response(200, json=[raw_count]))

    with MelbourneOpenData(settings) as source:
        records = source.fetch_dataset(DATASET)

    assert records == [raw_count]


@respx.mock
def test_requests_the_export_endpoint_with_no_row_limit(settings):
    route = respx.get(export_url(settings)).mock(return_value=httpx.Response(200, json=[]))

    with MelbourneOpenData(settings) as source:
        source.fetch_dataset(DATASET)

    assert route.calls.last.request.url.params["limit"] == "-1"
    assert "/exports/json" in str(route.calls.last.request.url)


@respx.mock
def test_retries_then_succeeds_after_a_rate_limit(settings, raw_count):
    respx.get(export_url(settings)).mock(
        side_effect=[
            httpx.Response(429),
            httpx.Response(503),
            httpx.Response(200, json=[raw_count]),
        ]
    )

    with MelbourneOpenData(settings) as source:
        records = source.fetch_dataset(DATASET)

    assert records == [raw_count]


@respx.mock
def test_gives_up_after_exhausting_retries(settings):
    respx.get(export_url(settings)).mock(return_value=httpx.Response(503))

    with MelbourneOpenData(settings) as source, pytest.raises(OpenDataError, match="after 3"):
        source.fetch_dataset(DATASET)


@respx.mock
def test_stops_immediately_when_the_domain_quota_is_exhausted(settings):
    route = respx.get(export_url(settings)).mock(
        return_value=httpx.Response(
            429,
            json={
                "error": "Too many requests on the domain. Please contact the domain administrator.",
                "errorcode": 10002,
                "reset_time": "2026-09-01T00:00:00Z",
                "call_limit": 6000000,
                "limit_time_unit": "month",
            },
        )
    )

    with MelbourneOpenData(settings) as source, pytest.raises(QuotaExceededError) as exc_info:
        source.fetch_dataset(DATASET)

    assert route.call_count == 1
    assert exc_info.value.reset_time == "2026-09-01T00:00:00Z"


@respx.mock
def test_an_ordinary_rate_limit_still_retries(settings, raw_count):
    respx.get(export_url(settings)).mock(
        side_effect=[httpx.Response(429), httpx.Response(200, json=[raw_count])]
    )

    with MelbourneOpenData(settings) as source:
        assert source.fetch_dataset(DATASET) == [raw_count]


@respx.mock
def test_retries_transport_errors(settings, raw_count):
    respx.get(export_url(settings)).mock(
        side_effect=[httpx.ConnectTimeout("boom"), httpx.Response(200, json=[raw_count])]
    )

    with MelbourneOpenData(settings) as source:
        assert source.fetch_dataset(DATASET) == [raw_count]


@respx.mock
def test_client_errors_are_not_retried(settings):
    route = respx.get(export_url(settings)).mock(return_value=httpx.Response(404))

    with MelbourneOpenData(settings) as source, pytest.raises(OpenDataError, match="404"):
        source.fetch_dataset(DATASET)

    assert route.call_count == 1


@respx.mock
def test_rejects_a_non_array_payload(settings):
    respx.get(export_url(settings)).mock(return_value=httpx.Response(200, json={"results": []}))

    with MelbourneOpenData(settings) as source, pytest.raises(OpenDataError, match="JSON array"):
        source.fetch_dataset(DATASET)


@respx.mock
def test_rejects_malformed_json(settings):
    respx.get(export_url(settings)).mock(return_value=httpx.Response(200, content=b"not json"))

    with MelbourneOpenData(settings) as source, pytest.raises(OpenDataError, match="valid JSON"):
        source.fetch_dataset(DATASET)


@respx.mock
def test_an_injected_client_is_not_closed_by_the_source(settings):
    respx.get(export_url(settings)).mock(return_value=httpx.Response(200, json=[]))
    client = httpx.Client()

    with MelbourneOpenData(settings, client=client) as source:
        source.fetch_dataset(DATASET)

    assert not client.is_closed
    client.close()
