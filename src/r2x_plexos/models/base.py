"""Base classes for PLEXOS data modeling."""

from typing import Any

from pydantic import Field
from pydantic.dataclasses import dataclass


@dataclass(frozen=True)
class PLEXOSRow:
    """Represents a result row from a PLEXOS database query."""

    value: Any = None
    period_type: str | None = None
    period_name: str | None = None
    timeslice_name: str | None = None
    timeslice_id: int | None = None
    band: int = Field(default=1, ge=1)
    units: str | None = None
    action: str | None = "="
    scenario_name: str | None = None
    datafile_name: str | None = None
    datafile_id: int | None = None
    column_name: str | None = None
    variable_name: str | None = None
    variable_id: int | None = None
    date_from: str | None = None
    date_to: str | None = None
    text: str | None = None
    text_class_name: str | None = None  # Type of text reference: "Data File", "Timeslice", "Variable"


@dataclass(frozen=True)
class PLEXOSPropertyKey:
    """Immutable key for property value lookups.

    This represents the full dimensionality of a PLEXOS property value.

    Notes
    -----
        frozen makes it hashable for use in dictionaries.
    """

    scenario: str | None = None
    band: int = 1
    timeslice: str | None = None
    date_from: str | None = None
    date_to: str | None = None
    period_type_id: int | None = None
    action: str | None = None
    variable: str | None = None
    text: str | None = None
