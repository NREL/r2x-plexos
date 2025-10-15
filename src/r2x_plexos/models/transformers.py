"""The following file contains Pydantic models for a PLEXOS Storage model."""

from typing import Annotated

from pydantic import Field

from .component import PLEXOSObject


class PLEXOSTransformer(PLEXOSObject):
    """Transformer model for PLEXOS."""

    ac_fixed_shift_angle: Annotated[
        float | int,
        Field(
            alias="AC Fixed Shift Angle",
            description="The fixed phase shift angle between the two windings of a single-phase transformer",
        ),
    ] = 0
    ac_line_charging_susceptance: Annotated[
        float | int,
        Field(
            alias="AC Line Charging Susceptance",
            description="The line-charging susceptance of a transformer",
        ),
    ] = 0
    ac_tap_ratio: Annotated[
        float | int,
        Field(
            alias="AC Tap Ratio",
            description="The turns ratio of the primary winding of a transformer",
            gt=0,
        ),
    ] = 1
    contingency_limit_penalty: Annotated[
        float | int,
        Field(
            alias="Contingency Limit Penalty",
            description="Penalty for exceeding contingency flow limits (usd/MWh)",
        ),
    ] = -1
    enforce_limits: Annotated[
        int,
        Field(
            alias="Enforce Limits",
            description="If flow limits are enforced regardless of Transmission [Constraint Voltage Threshold].",
            json_schema_extra={"enum": [0, 1, 2, 3]},
        ),
    ] = 1
    fixed_loss: Annotated[
        float | int,
        Field(
            alias="Fixed Loss",
            description="Fixed loss on transformer",
        ),
    ] = 0
    forced_outage_rate: Annotated[
        float | int,
        Field(
            alias="Forced Outage Rate",
            description="Expected proportion of time the facility is unavailable due to forced outage",
            ge=0,
            le=100,
        ),
    ] = 0
    formulate_npl_upfront: Annotated[
        int,
        Field(
            alias="Formulate NPL Upfront",
            description="If integer conditions that control non-physical losses should be formulated upfront rather than checked iteratively",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    formulate_upfront: Annotated[
        int,
        Field(
            alias="Formulate Upfront",
            description="If constraints should all be formulated upfront rather than checked iteratively.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    limit_penalty: Annotated[
        float | int,
        Field(
            alias="Limit Penalty",
            description="Penalty for exceeding the flow limits on the Transformer. (usd/MWh)",
        ),
    ] = -1
    loss_allocation: Annotated[
        float | int,
        Field(
            alias="Loss Allocation",
            description="Proportion of transformer losses allocated to the receiving node",
            ge=0,
            le=1,
        ),
    ] = 0.5
    maintenance_frequency: Annotated[
        float | int,
        Field(
            alias="Maintenance Frequency",
            description="Frequency of maintenance outages in an annual timeframe",
            ge=0,
        ),
    ] = 0
    maintenance_rate: Annotated[
        float | int,
        Field(
            alias="Maintenance Rate",
            description="Expected proportion of time the facility is unavailable due to maintenance",
            ge=0,
            le=100,
        ),
    ] = 0
    max_loss_tranches: Annotated[
        float | int,
        Field(
            alias="Max Loss Tranches",
            description="Maximum number of tranches in piecewise linear loss function.",
            ge=2,
        ),
    ] = 2
    max_time_to_repair: Annotated[
        float | int,
        Field(
            alias="Max Time To Repair",
            description="Maximum time to repair (hr)",
            ge=0,
        ),
    ] = 0
    mean_time_to_repair: Annotated[
        float | int,
        Field(
            alias="Mean Time to Repair",
            description="Mean time to repair (hr)",
            ge=0,
        ),
    ] = 24
    min_time_to_repair: Annotated[
        float | int,
        Field(
            alias="Min Time To Repair",
            description="Minimum time to repair (hr)",
            ge=0,
        ),
    ] = 0
    must_report: Annotated[
        int,
        Field(
            alias="Must Report",
            description="If the Transformer must be reported regardless of Transmission [Report Voltage Threshold].",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    outage_max_rating: Annotated[
        float | int,
        Field(
            alias="Outage Max Rating",
            description="Transformer rating in the reference direction during outage",
        ),
    ] = 0
    outage_min_rating: Annotated[
        float | int,
        Field(
            alias="Outage Min Rating",
            description="Transformer rating in the counter-reference direction during outage",
        ),
    ] = 0
    overload_rating: Annotated[
        float | int,
        Field(
            alias="Overload Rating",
            description="Emergency rating in the reference direction",
            ge=0,
        ),
    ] = 0
    random_number_seed: Annotated[
        float | int,
        Field(
            alias="Random Number Seed",
            description="Random number seed assigned to the Line for the generation of outages",
            ge=0,
            le=2147483647,
        ),
    ] = 0
    rating: Annotated[
        float | int,
        Field(
            alias="Rating",
            description="Maximum MW rating",
            ge=0,
        ),
    ] = 0
    reactance: Annotated[
        float | int,
        Field(
            alias="Reactance",
            description="Together with any resistance this makes up the lines impedance",
        ),
    ] = 0
    repair_time_distribution: Annotated[
        int,
        Field(
            alias="Repair Time Distribution",
            description="Distribution used to generate repair times (Auto,Constant,Uniform,Triangular,Exponential,Weibull,Lognormal,SEV,LEV)",
            json_schema_extra={"enum": [-1, 0, 1, 2, 3, 4, 5, 6, 7]},
        ),
    ] = -1
    repair_time_scale: Annotated[
        float | int,
        Field(
            alias="Repair Time Scale",
            description="Repair time function scale parameter (for exponential,Weibull,lognormal,SEV,LEV)",
        ),
    ] = 0
    repair_time_shape: Annotated[
        float | int,
        Field(
            alias="Repair Time Shape",
            description="Repair time function shape parameter (for Weibull,lognormal)",
        ),
    ] = 0
    resistance: Annotated[
        float | int,
        Field(
            alias="Resistance",
            description="A measure of the transformer's opposition to the flow of electric charge (pu)",
        ),
    ] = 0
    screening_mode: Annotated[
        int,
        Field(
            alias="Screening Mode",
            description="The set of transformers that should be screened for post-contingency flow under screen contingencies",
            json_schema_extra={"enum": [0, 1, 2]},
        ),
    ] = 1
    susceptance: Annotated[
        float | int,
        Field(
            alias="Susceptance",
            description="The reciprocal of the reactance of a circuit and thus the imaginary part of its admittance (pu)",
        ),
    ] = 0
    units: Annotated[
        int,
        Field(
            alias="Units",
            description="Flag if transformer is in service",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 1
    units_out: Annotated[
        int,
        Field(
            alias="Units Out",
            description="Number of [Units] out of service",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    x: Annotated[
        float | int,
        Field(
            alias="x",
            description="Value to pass-through to solution",
        ),
    ] = 0
    y: Annotated[
        float | int,
        Field(
            alias="y",
            description="Value to pass-through to solution",
        ),
    ] = 0
    z: Annotated[
        float | int,
        Field(
            alias="z",
            description="Value to pass-through to solution",
        ),
    ] = 0

    @classmethod
    def example(cls) -> "PLEXOSTransformer":
        """Return a minimal example PlexosTransformer."""
        return PLEXOSTransformer(
            name="ExampleTransformer",
        )
