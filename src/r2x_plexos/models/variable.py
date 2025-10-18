"""The following file contains Pydantic models for a PLEXOS variable model."""

from typing import Annotated

from pydantic import Field

from .component import PLEXOSObject
from .property_specification import PLEXOSProperty


class PLEXOSVariable(PLEXOSObject):
    """Class that holds attributes about PLEXOS variables."""

    profile: Annotated[
        float | int | None,
        PLEXOSProperty,
        Field(alias="Profile", description="Sample profile of variable values"),
    ] = None
    sampling_method: Annotated[
        int,
        PLEXOSProperty(is_enum=True),
        Field(
            alias="Sampling Method",
            description="Sampling method applied to the variable",
            json_schema_extra={"enum": [0, 1, 2]},
        ),
    ] = 1
    abs_error_std_dev: Annotated[
        float | int,
        Field(
            alias="Abs Error Std Dev",
            description="Absolute value of standard deviation of errors",
        ),
    ] = 0
    arima_alpha: Annotated[
        float | int,
        Field(
            alias="ARIMA alpha",
            description="ARIMA autoregressive parameter",
        ),
    ] = 0
    arima_beta: Annotated[
        float | int,
        Field(
            alias="ARIMA beta",
            description="ARIMA moving-average parameter",
        ),
    ] = 0
    arima_d: Annotated[
        float | int,
        Field(
            alias="ARIMA d",
            description="ARIMA differencing parameter",
        ),
    ] = 0
    auto_correlation: Annotated[
        float | int,
        Field(
            alias="Auto Correlation",
            description="Correlation of error between time intervals",
        ),
    ] = 0
    compound_index: Annotated[
        float | int,
        Field(
            alias="Compound Index",
            description="Rate of escalation",
        ),
    ] = 0
    compound_index_day: Annotated[
        float | int,
        Field(
            alias="Compound Index Day",
            description="Rate of escalation per day",
        ),
    ] = 0
    compound_index_hour: Annotated[
        float | int,
        Field(
            alias="Compound Index Hour",
            description="Rate of escalation per hour",
        ),
    ] = 0
    compound_index_month: Annotated[
        float | int,
        Field(
            alias="Compound Index Month",
            description="Rate of escalation per month",
        ),
    ] = 0
    compound_index_week: Annotated[
        float | int,
        Field(
            alias="Compound Index Week",
            description="Rate of escalation per week",
        ),
    ] = 0
    compound_index_year: Annotated[
        float | int,
        Field(
            alias="Compound Index Year",
            description="Rate of escalation per year",
        ),
    ] = 0
    condition: Annotated[
        int,
        Field(
            alias="Condition",
            description="Conditional value type",
            json_schema_extra={"enum": [-2, -1, 0, 1, 2, 4]},
        ),
    ] = 4
    condition_logic: Annotated[
        int,
        Field(
            alias="Condition Logic",
            description="Logic used in combining conditional variables",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    distribution_type: Annotated[
        int,
        Field(
            alias="Distribution Type",
            description="Distribution type for error terms",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    error_std_dev: Annotated[
        float | int,
        Field(
            alias="Error Std Dev",
            description="Percentage standard deviation of errors",
        ),
    ] = 0
    formulate_value: Annotated[
        int,
        Field(
            alias="Formulate Value",
            description="Flag if the Value is formulated as a decision variable",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    garch_alpha: Annotated[
        float | int,
        Field(
            alias="GARCH alpha",
            description="Weight on the square of the return in GARCH(1,1)",
        ),
    ] = 0
    garch_beta: Annotated[
        float | int,
        Field(
            alias="GARCH beta",
            description="Weight on the variance in GARCH(1,1)",
        ),
    ] = 0
    garch_omega: Annotated[
        float | int,
        Field(
            alias="GARCH omega",
            description="Long-run weighted variance in GARCH(1,1)",
        ),
    ] = 0
    include_in_lt_plan: Annotated[
        int,
        Field(
            alias="Include in LT Plan",
            description="If the condition is allowed to be active in the LT Plan phase.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    include_in_mt_schedule: Annotated[
        int,
        Field(
            alias="Include in MT Schedule",
            description="If the condition is allowed to be active in the MT Schedule phase.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    include_in_pasa: Annotated[
        int,
        Field(
            alias="Include in PASA",
            description="If the condition is allowed to be active in the PASA phase.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    include_in_st_schedule: Annotated[
        int,
        Field(
            alias="Include in ST Schedule",
            description="If the condition is allowed to be active in the ST Schedule phase.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    jump_error_std_dev: Annotated[
        float | int,
        Field(
            alias="Jump Error Std Dev",
            description="Percentage standard deviation of jump magnitude errors",
        ),
    ] = 0
    jump_frequency: Annotated[
        float | int,
        Field(
            alias="Jump Frequency",
            description="Jump frequency in jump-diffusion model",
        ),
    ] = 0
    jump_magnitude: Annotated[
        float | int,
        Field(
            alias="Jump Magnitude",
            description="Jump magnitude in jump-diffusion model",
        ),
    ] = 0
    lookup_unit: Annotated[
        float | int,
        Field(
            alias="Lookup Unit",
            description="Unit of the y values in the lookup table",
        ),
    ] = 1
    lookup_x: Annotated[
        float | int,
        Field(
            alias="Lookup x",
            description="Lookup table for x-axis value",
        ),
    ] = 0
    lookup_y: Annotated[
        float | int,
        Field(
            alias="Lookup y",
            description="Lookup table for y-axis value",
        ),
    ] = 0
    max_value: Annotated[
        float | int,
        Field(
            alias="Max Value",
            description="Maximum allowed sample value",
        ),
    ] = 1e30
    max_value_std_dev: Annotated[
        float | int,
        Field(
            alias="Max Value Std Dev",
            description="Percentage standard deviation of maximum value",
        ),
    ] = 0
    mean_reversion: Annotated[
        float | int,
        Field(
            alias="Mean Reversion",
            description="Mean reversion parameter in differential equation",
        ),
    ] = 0
    min_value: Annotated[
        float | int,
        PLEXOSProperty,
        Field(
            alias="Min Value",
            description="Minimum allowed sample value",
        ),
    ] = -1e30
    min_value_std_dev: Annotated[
        float | int,
        Field(
            alias="Min Value Std Dev",
            description="Percentage standard deviation of minimum value",
        ),
    ] = 0
    probability: Annotated[
        float | int,
        Field(
            alias="Probability",
            description="Probability of exceedance (POE)",
        ),
    ] = 50
    profile_day: Annotated[
        float | int,
        Field(
            alias="Profile Day",
            description="Sample profile of variable values",
        ),
    ] = 0
    profile_hour: Annotated[
        float | int,
        Field(
            alias="Profile Hour",
            description="Sample profile of variable values",
        ),
    ] = 0
    profile_month: Annotated[
        float | int,
        Field(
            alias="Profile Month",
            description="Sample profile of variable values",
        ),
    ] = 0
    profile_week: Annotated[
        float | int,
        Field(
            alias="Profile Week",
            description="Sample profile of variable values",
        ),
    ] = 0
    profile_year: Annotated[
        float | int,
        Field(
            alias="Profile Year",
            description="Sample profile of variable values",
        ),
    ] = 0
    random_number_seed: Annotated[
        float | int,
        Field(
            alias="Random Number Seed",
            description="Random number seed assigned to the variable",
            ge=0,
            le=2147483647,
        ),
    ] = 0
    sampling: Annotated[
        int,
        Field(
            alias="Sampling",
            description="Flag if random sampling should occur in the period",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    sampling_frequency: Annotated[
        float | int,
        Field(
            alias="Sampling Frequency",
            description="Frequency of temporal sampling of period type [Sampling Period Type] where zero means no sampling.",
            ge=0,
        ),
    ] = 0
    sampling_period_type: Annotated[
        int,
        Field(
            alias="Sampling Period Type",
            description="Period type of temporal sampling where number of periods between samples is [Sampling Frequency].",
            json_schema_extra={"enum": [0, 1, 2, 3, 4, 6]},
        ),
    ] = 0
    step_hour_active_from: Annotated[
        float | int,
        Field(
            alias="Step Hour Active From",
            description="First hour of each step the Condition is allowed to be active",
            ge=1,
        ),
    ] = 1
    step_hours_active: Annotated[
        float | int,
        Field(
            alias="Step Hours Active",
            description="Number of hours the Condition is allowed to be active from the first active hour in the step",
        ),
    ] = -1

    @classmethod
    def example(cls) -> "PLEXOSVariable":
        """Create an example PLEXOSVariable."""
        return PLEXOSVariable(
            name="var-01",
        )
