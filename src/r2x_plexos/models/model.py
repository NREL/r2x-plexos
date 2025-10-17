"""The following file contains Pydantic models for a PLEXOS Horizon class."""

from typing import Annotated

from pydantic import Field

from .component import PLEXOSConfiguration


class PLEXOSModel(PLEXOSConfiguration):
    """PLEXOS Model class."""

    @classmethod
    def example(cls) -> "PLEXOSModel":
        """Create an example PlexosHorizon."""
        return PLEXOSModel(
            name="PLEXOSModel",
        )


class PLEXOSHorizon(PLEXOSConfiguration):
    """PLEXOS Horizon class."""

    chrono_at_a_time: Annotated[
        float | int,
        Field(
            alias="Chrono At a Time",
            description="Number of steps in the chronological model",
            ge=1,
        ),
    ] = 1
    chrono_date_from: Annotated[
        float | int,
        Field(
            alias="Chrono Date From",
            description="Start date for the chronological model",
            ge=0,
        ),
    ] = 43831
    chrono_period_from: Annotated[
        float | int,
        Field(
            alias="Chrono Period From",
            description="Start interval for the chronological model",
            ge=1,
        ),
    ] = 1
    chrono_period_to: Annotated[
        float | int,
        Field(
            alias="Chrono Period To",
            description="End interval for the chronological model",
            ge=1,
        ),
    ] = 24
    chrono_step_count: Annotated[
        float | int,
        Field(
            alias="Chrono Step Count",
            description="Number of step types in each step of the chronological model",
            ge=1,
        ),
    ] = 1
    chrono_step_type: Annotated[
        int,
        Field(
            alias="Chrono Step Type",
            description="Chronological model step type",
            json_schema_extra={"enum": [-1, 0, 1, 2, 3]},
        ),
    ] = 2
    chronology: Annotated[
        int,
        Field(
            alias="Chronology",
            description="Type of chronology used",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    compression_factor: Annotated[
        float | int,
        Field(
            alias="Compression Factor",
            description="Number of intervals to output per interval simulated",
            ge=1,
        ),
    ] = 1
    date_from: Annotated[
        float | int,
        Field(
            alias="Date From",
            description="Start date of the planning horizon",
            ge=0,
        ),
    ] = 43831
    day_beginning: Annotated[
        float | int,
        Field(
            alias="Day Beginning",
            description="Start hour of the trading day",
            ge=0,
            le=23,
        ),
    ] = 0
    look_ahead_at_a_time: Annotated[
        float | int,
        Field(
            alias="Look-ahead At a Time",
            description="Number of step types in each step of the chronological model look-ahead",
            ge=1,
        ),
    ] = 1
    look_ahead_count: Annotated[
        float | int,
        Field(
            alias="Look-ahead Count",
            description="Number of additional look-ahead steps in the planning horizon",
            ge=0,
        ),
    ] = 0
    look_ahead_indicator: Annotated[
        int,
        Field(
            alias="Look-ahead Indicator",
            description="Flag if chronological model used a look-ahead",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    look_ahead_periods_per_day: Annotated[
        float | int,
        Field(
            alias="Look-ahead Periods per Day",
            description="Number of intervals in each trading day of the look-ahead",
            ge=1,
            le=86400,
        ),
    ] = 24
    look_ahead_type: Annotated[
        int,
        Field(
            alias="Look-ahead Type",
            description="Step type for look-ahead in chronological model",
            json_schema_extra={"enum": [0, 1, 2, 6]},
        ),
    ] = 1
    periods_per_day: Annotated[
        float | int,
        Field(
            alias="Periods per Day",
            description="Number of intervals in each trading day",
            ge=1,
            le=86400,
        ),
    ] = 24
    step_count: Annotated[
        float | int,
        Field(
            alias="Step Count",
            description="Number of steps in the planning horizon",
            ge=1,
        ),
    ] = 1
    step_type: Annotated[
        int,
        Field(
            alias="Step Type",
            description="Planning horizon step type",
            json_schema_extra={"enum": [0, 1, 3, 4]},
        ),
    ] = 1
    week_beginning: Annotated[
        float | int,
        Field(
            alias="Week Beginning",
            description="Start day for weekly constraints",
            ge=-1,
            le=7,
        ),
    ] = 0
    year_ending: Annotated[
        float | int,
        Field(
            alias="Year Ending",
            description="Last month of the fiscal year",
            ge=0,
            le=12,
        ),
    ] = 0

    @classmethod
    def example(cls) -> "PLEXOSHorizon":
        """Create an example PlexosHorizon."""
        return PLEXOSHorizon(
            name="ExampleHorizon",
            step_type=1,
            periods_per_day=24,
            step_count=1,
        )
