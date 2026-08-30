"""HTTP client for the Melbourne Open Data (Opendatasoft Explore v2.1) API."""

import logging
import time
from types import TracebackType
from typing import Any

import httpx

from pedestrian_pipeline.config import Settings

logger = logging.getLogger(__name__)

RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})


class OpenDataError(RuntimeError):
    """Raised when the API cannot be read after exhausting retries."""


class QuotaExceededError(OpenDataError):
    """Raised when the whole domain's monthly call quota is exhausted.

    Opendatasoft enforces this per-domain, not per-caller: every consumer of
    data.melbourne.vic.gov.au is rate limited until the quota resets, so
    retrying is pointless. Callers should treat this as "no data available
    right now" rather than a pipeline failure.
    """

    def __init__(self, url: str, reset_time: str | None) -> None:
        self.reset_time = reset_time
        suffix = f", resets {reset_time}" if reset_time else ""
        super().__init__(f"{url}: domain call quota exhausted{suffix}")


class MelbourneOpenData:
    """Reads whole datasets from the Explore v2.1 API.

    Deliberately uses the /exports/json endpoint rather than /records. The
    records endpoint enforces offset + limit <= 10,000, but the counts dataset
    holds ~25,000 rows, so paging through it silently truncates the result at
    roughly 39% with no error. The export endpoint streams the full dataset in
    one request and is the only complete read available.
    """

    def __init__(self, settings: Settings, client: httpx.Client | None = None) -> None:
        self._settings = settings
        self._client = client or httpx.Client(
            timeout=settings.request_timeout_seconds,
            headers={"Accept": "application/json"},
            follow_redirects=True,
        )
        self._owns_client = client is None

    def __enter__(self) -> "MelbourneOpenData":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def fetch_dataset(self, dataset: str) -> list[dict[str, Any]]:
        url = self._settings.dataset_export_url(dataset)
        response = self._get_with_retry(url, params={"limit": -1})

        try:
            payload = response.json()
        except ValueError as exc:
            raise OpenDataError(f"{dataset}: response was not valid JSON") from exc

        if not isinstance(payload, list):
            raise OpenDataError(f"{dataset}: expected a JSON array, got {type(payload).__name__}")

        logger.info("fetched %s records from %s", len(payload), dataset)
        self._log_rate_limit(response)
        return payload

    def _get_with_retry(self, url: str, params: dict[str, Any]) -> httpx.Response:
        last_error: Exception | None = None

        for attempt in range(self._settings.max_retries + 1):
            if attempt:
                delay = self._settings.backoff_base_seconds * (2 ** (attempt - 1))
                logger.warning("retrying %s in %.1fs (attempt %s)", url, delay, attempt + 1)
                time.sleep(delay)

            try:
                response = self._client.get(url, params=params)
            except httpx.HTTPError as exc:
                last_error = exc
                continue

            if response.status_code == 429 and (reset_time := self._quota_reset_time(response)):
                raise QuotaExceededError(url, reset_time)

            if response.status_code in RETRYABLE_STATUS:
                last_error = httpx.HTTPStatusError(
                    f"{response.status_code} from {url}",
                    request=response.request,
                    response=response,
                )
                continue

            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise OpenDataError(f"{url} returned {response.status_code}") from exc

            return response

        raise OpenDataError(
            f"{url} failed after {self._settings.max_retries + 1} attempts"
        ) from last_error

    @staticmethod
    def _quota_reset_time(response: httpx.Response) -> str | None:
        """Return the reset time if this 429 is a domain-wide quota exhaustion.

        Opendatasoft distinguishes this from an ordinary per-request rate
        limit via a dedicated error code in the JSON body, e.g.:
        {"error": "Too many requests on the domain...", "errorcode": 10002,
        "reset_time": "2026-09-01T00:00:00Z"}
        """
        try:
            payload = response.json()
        except ValueError:
            return None
        if not isinstance(payload, dict) or payload.get("errorcode") != 10002:
            return None
        reset_time = payload.get("reset_time")
        return reset_time if isinstance(reset_time, str) else "unknown"

    @staticmethod
    def _log_rate_limit(response: httpx.Response) -> None:
        remaining = response.headers.get("X-RateLimit-Remaining")
        limit = response.headers.get("X-RateLimit-Limit")
        if remaining and limit:
            logger.info("api quota: %s/%s remaining", remaining, limit)
