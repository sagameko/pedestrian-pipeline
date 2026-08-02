"""Validated record schemas for the Melbourne Open Data pedestrian datasets."""

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError, model_validator


class PedestrianCount(BaseModel):
    """One per-minute reading from one sensor."""

    model_config = ConfigDict(frozen=True, extra="ignore")

    location_id: int = Field(ge=1)
    sensing_datetime: AwareDatetime
    direction_1: int = Field(ge=0)
    direction_2: int = Field(ge=0)
    total_of_directions: int = Field(ge=0)

    @model_validator(mode="after")
    def _directions_must_sum_to_total(self) -> "PedestrianCount":
        expected = self.direction_1 + self.direction_2
        if expected != self.total_of_directions:
            raise ValueError(
                f"direction counts do not sum to the reported total: "
                f"{self.direction_1} + {self.direction_2} != {self.total_of_directions}"
            )
        return self


class SensorLocation(BaseModel):
    """A physical sensor in the counting system."""

    model_config = ConfigDict(frozen=True, extra="ignore", populate_by_name=True)

    location_id: int = Field(ge=1)
    sensor_description: str
    sensor_name: str
    status: str
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    location_type: str | None = None
    installation_date: date | None = None
    note: str | None = None
    # Renamed on the way in: the counts dataset uses direction_1/direction_2 for
    # magnitudes, while this dataset uses the same names for compass labels.
    direction_1_label: str | None = Field(default=None, alias="direction_1")
    direction_2_label: str | None = Field(default=None, alias="direction_2")


@dataclass(frozen=True)
class RejectedRecord:
    record: dict[str, Any]
    reason: str


def parse_records[ModelT: BaseModel](
    raw_records: list[dict[str, Any]], model: type[ModelT]
) -> tuple[list[ModelT], list[RejectedRecord]]:
    """Validate records individually so one bad row cannot fail an entire run."""
    parsed: list[ModelT] = []
    rejected: list[RejectedRecord] = []

    for record in raw_records:
        try:
            parsed.append(model.model_validate(record))
        except ValidationError as exc:
            reasons = "; ".join(
                f"{'.'.join(str(p) for p in err['loc']) or '<record>'}: {err['msg']}"
                for err in exc.errors()
            )
            rejected.append(RejectedRecord(record=record, reason=reasons))

    return parsed, rejected


def utcnow() -> datetime:
    return datetime.now(UTC)
