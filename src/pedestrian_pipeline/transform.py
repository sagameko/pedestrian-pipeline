"""Shape validated records into warehouse-ready frames."""

from dataclasses import dataclass
from datetime import datetime

import polars as pl

from pedestrian_pipeline.config import MELBOURNE_TZ
from pedestrian_pipeline.models import PedestrianCount, SensorLocation

COUNT_KEY = ["location_id", "sensing_datetime"]

FACT_COLUMNS = [
    "location_id",
    "sensing_datetime",
    "local_datetime",
    "local_date",
    "local_hour",
    "direction_1",
    "direction_2",
    "total_of_directions",
    "ingested_at",
]

DIM_COLUMNS = [
    "location_id",
    "sensor_description",
    "sensor_name",
    "status",
    "latitude",
    "longitude",
    "location_type",
    "installation_date",
    "direction_1_label",
    "direction_2_label",
    "note",
    "ingested_at",
]

_FACT_SCHEMA = {
    "location_id": pl.Int64,
    "sensing_datetime": pl.Datetime(time_unit="us", time_zone="UTC"),
    "direction_1": pl.Int64,
    "direction_2": pl.Int64,
    "total_of_directions": pl.Int64,
}


@dataclass(frozen=True)
class CountsTransform:
    frame: pl.DataFrame
    restated_keys: int
    rows_discarded: int


def build_fact_counts(counts: list[PedestrianCount], ingested_at: datetime) -> CountsTransform:
    """Resolve restated readings, then derive Melbourne-local time columns."""
    frame = pl.DataFrame([c.model_dump() for c in counts], schema=_FACT_SCHEMA)

    if frame.is_empty():
        return CountsTransform(frame=_empty_fact_frame(), restated_keys=0, rows_discarded=0)

    conflicting = frame.filter(pl.struct(COUNT_KEY).is_duplicated())
    restated_keys = conflicting.select(COUNT_KEY).unique().height

    resolved = frame.sort("total_of_directions", descending=True).unique(
        subset=COUNT_KEY, keep="first", maintain_order=True
    )

    enriched = (
        resolved.with_columns(
            pl.col("sensing_datetime").dt.convert_time_zone(MELBOURNE_TZ).alias("local_datetime")
        )
        .with_columns(
            pl.col("local_datetime").dt.date().alias("local_date"),
            pl.col("local_datetime").dt.hour().cast(pl.Int32).alias("local_hour"),
            pl.lit(ingested_at).cast(_FACT_SCHEMA["sensing_datetime"]).alias("ingested_at"),
        )
        .select(FACT_COLUMNS)
        .sort(COUNT_KEY)
    )

    return CountsTransform(
        frame=enriched,
        restated_keys=restated_keys,
        rows_discarded=frame.height - enriched.height,
    )


def build_dim_sensor(sensors: list[SensorLocation], ingested_at: datetime) -> pl.DataFrame:
    if not sensors:
        return _empty_dim_frame()

    return (
        pl.DataFrame([s.model_dump() for s in sensors])
        .with_columns(
            pl.lit(ingested_at).cast(_FACT_SCHEMA["sensing_datetime"]).alias("ingested_at")
        )
        .select(DIM_COLUMNS)
        .unique(subset=["location_id"], keep="first")
        .sort("location_id")
    )


def _empty_fact_frame() -> pl.DataFrame:
    tz = _FACT_SCHEMA["sensing_datetime"]
    melbourne = pl.Datetime(time_unit="us", time_zone=MELBOURNE_TZ)
    return pl.DataFrame(
        schema={
            "location_id": pl.Int64,
            "sensing_datetime": tz,
            "local_datetime": melbourne,
            "local_date": pl.Date,
            "local_hour": pl.Int32,
            "direction_1": pl.Int64,
            "direction_2": pl.Int64,
            "total_of_directions": pl.Int64,
            "ingested_at": tz,
        }
    )


def _empty_dim_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "location_id": pl.Int64,
            "sensor_description": pl.Utf8,
            "sensor_name": pl.Utf8,
            "status": pl.Utf8,
            "latitude": pl.Float64,
            "longitude": pl.Float64,
            "location_type": pl.Utf8,
            "installation_date": pl.Date,
            "direction_1_label": pl.Utf8,
            "direction_2_label": pl.Utf8,
            "note": pl.Utf8,
            "ingested_at": _FACT_SCHEMA["sensing_datetime"],
        }
    )
