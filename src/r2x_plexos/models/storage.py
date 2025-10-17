"""The following file contains Pydantic models for a PLEXOS Storage model."""

from typing import Annotated

from pydantic import Field

from .component import PLEXOSObject
from .property_specification import PLEXOSProperty


class PLEXOSStorage(PLEXOSObject):
    """Class that holds attributes about PLEXOS storage."""

    balance_period: Annotated[
        int,
        Field(
            alias="Balance Period",
            description="Frequency of storage balance",
            json_schema_extra={"enum": [0, 1, 2, 3, 4, 5, 6]},
        ),
    ] = 0
    decomposition_bound_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/GWh"),
        Field(
            alias="Decomposition Bound Penalty",
            description="Penalty applied to violation of storage bounds when the decomposition implies possible violations.",
            ge=0,
        ),
    ] = 1000000
    decomposition_method: Annotated[
        int,
        Field(
            alias="Decomposition Method",
            description="Method used to pass the optimal storage trajectory from one simulation phase to the next.",
            json_schema_extra={"enum": [0, 1, 2]},
        ),
    ] = 1
    decomposition_penalty_a: Annotated[
        float | int,
        Field(
            alias="Decomposition Penalty a",
            description="Decomposition storage target penalty function 'a' term.",
        ),
    ] = 0.0489
    decomposition_penalty_b: Annotated[
        float | int,
        Field(
            alias="Decomposition Penalty b",
            description="Decomposition storage target penalty function 'b' term.",
        ),
    ] = 0.6931
    decomposition_penalty_c: Annotated[
        float | int,
        Field(
            alias="Decomposition Penalty c",
            description="Decomposition storage target penalty function 'c' term.",
        ),
    ] = 0
    decomposition_penalty_x: Annotated[
        float | int,
        Field(
            alias="Decomposition Penalty x",
            description="Decomposition storage target penalty function 'x' term.",
        ),
    ] = 1.1
    downstream_efficiency: Annotated[
        float | int,
        PLEXOSProperty(units="MW/MW"),
        Field(
            alias="Downstream Efficiency",
            description="Aggregate efficiency of generation down the river chain",
            ge=0,
        ),
    ] = 1
    end_effects_method: Annotated[
        int,
        PLEXOSProperty(is_enum=True),
        Field(
            alias="End Effects Method",
            description="Method used to handle end-of-period storage.",
            json_schema_extra={"enum": [0, 1, 2]},
        ),
    ] = 0
    energy_value: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Energy Value",
            description="Incremental price of energy generated from storage",
        ),
    ] = 10000
    energy_value_point: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Energy Value Point",
            description="Energy associated with [Energy Value] in multiple bands",
            ge=0,
        ),
    ] = 1e30
    enforce_bounds: Annotated[
        int,
        Field(
            alias="Enforce Bounds",
            description="If the storage bounds are enforced.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    high_ref_area: Annotated[
        float | int,
        PLEXOSProperty(units="m²"),
        Field(
            alias="High Ref Area",
            description="Area of surface at high reference level",
            ge=0,
        ),
    ] = 0
    high_ref_level: Annotated[
        float | int,
        PLEXOSProperty(units="m"),
        Field(
            alias="High Ref Level",
            description="High reference level for volume calculation",
            ge=0,
        ),
    ] = 0
    initial_level: Annotated[
        float | int,
        PLEXOSProperty(units="m"),
        Field(
            alias="Initial Level",
            description="Initial level",
            ge=0,
        ),
    ] = 0
    initial_volume: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Initial Volume",
            description="Storage volume at the start of the period",
            ge=0,
        ),
    ] = 0
    internal_volume_scalar: Annotated[
        float | int,
        Field(
            alias="Internal Volume Scalar",
            description="Storage volume scaling factor used internal to the mathematical program.",
            gt=0,
        ),
    ] = 1000
    loss_rate: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Loss Rate",
            description="Rate of loss due to evaporation, leakage, etc",
        ),
    ] = 0
    low_ref_area: Annotated[
        float | int,
        PLEXOSProperty(units="m²"),
        Field(
            alias="Low Ref Area",
            description="Area of surface at low reference level",
            ge=0,
        ),
    ] = 0
    low_ref_level: Annotated[
        float | int,
        PLEXOSProperty(units="m"),
        Field(
            alias="Low Ref Level",
            description="Low reference level for volume calculation",
            ge=0,
        ),
    ] = 0
    max_generator_release: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Generator Release",
            description="Maximum rate of release for generation from the storage",
            ge=0,
        ),
    ] = 1e30
    max_level: Annotated[
        float | int,
        PLEXOSProperty(units="m"),
        Field(
            alias="Max Level",
            description="Maximum level",
            ge=0,
        ),
    ] = 1e30
    max_ramp: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Ramp",
            description="Maximum rate of change in storage",
            ge=0,
        ),
    ] = 1e30
    max_ramp_day: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Ramp Day",
            description="Maximum of change in storage across each day.",
            ge=0,
        ),
    ] = 1e30
    max_ramp_hour: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Ramp Hour",
            description="Maximum change in storage across each hour.",
            ge=0,
        ),
    ] = 1e30
    max_ramp_month: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Ramp Month",
            description="Maximum of change in storage across each month.",
            ge=0,
        ),
    ] = 1e30
    max_ramp_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Max Ramp Penalty",
            description="Penalty for violating the [Max Ramp Day/Week/Month/Year] constraint.",
        ),
    ] = -1
    max_ramp_week: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Ramp Week",
            description="Maximum of change in storage across each week.",
            ge=0,
        ),
    ] = 1e30
    max_ramp_year: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Ramp Year",
            description="Maximum of change in storage across each year.",
            ge=0,
        ),
    ] = 1e30
    max_release: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Release",
            description="Maximum rate of release from the storage",
            ge=0,
        ),
    ] = 1e30
    max_release_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Max Release Penalty",
            description="Penalty for violation of maximum rate of release constraints",
        ),
    ] = -1
    max_spill: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Spill",
            description='Maximum allowable spill from the storage to "the sea"',
            ge=0,
        ),
    ] = 1e30
    max_volume: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Volume",
            description="Maximum volume",
            ge=0,
        ),
    ] = 1e30
    max_volume_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Max Volume Penalty",
            description="Penalty for violatiog the Max Volume constraint",
        ),
    ] = -1
    min_level: Annotated[
        float | int,
        PLEXOSProperty(units="m"),
        Field(
            alias="Min Level",
            description="Minimum level",
            ge=0,
        ),
    ] = 0
    min_release: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Min Release",
            description="Minimum rate of release from the storage",
        ),
    ] = 0
    min_release_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Min Release Penalty",
            description="Penalty for violation of minimum rate of release constraints",
        ),
    ] = -1
    min_volume: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Min Volume",
            description="Minimum volume",
            ge=0,
        ),
    ] = 0
    min_volume_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Min Volume Penalty",
            description="Penalty for violatiog the Min Volume constraint",
        ),
    ] = -1
    model: Annotated[
        int,
        Field(
            alias="Model",
            description="Model used to define and model storage volumes (used to override the file-level Hydro Model setting).",
            json_schema_extra={"enum": [0, 1, 2, 3]},
        ),
    ] = 0
    natural_inflow: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Natural Inflow",
            description="Rate of natural inflow",
        ),
    ] = 0
    natural_inflow_incr: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Natural Inflow Incr",
            description="Increment to [Natural Inflow]",
        ),
    ] = 0
    natural_inflow_scalar: Annotated[
        float | int,
        Field(
            alias="Natural Inflow Scalar",
            description="Multiplier on [Natural Inflow]",
        ),
    ] = 1
    non_physical_inflow_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Non-physical Inflow Penalty",
            description="Penalty applied to non-physical inflow to the storage. A value of -1 means none are allowed.",
        ),
    ] = -1
    non_physical_spill_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Non-physical Spill Penalty",
            description="Penalty applied to non-physical spill from the storage. A value of -1 means none are allowed.",
        ),
    ] = -1
    recycle_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/GWh"),
        Field(
            alias="Recycle Penalty",
            description="Penalty for violating the recycling constraint.",
        ),
    ] = -1
    rolling_planning_bonus: Annotated[
        float | int,
        PLEXOSProperty(units="usd/GWh"),
        Field(
            alias="Rolling Planning Bonus",
            description="Bonus for storage contents at the end of the look-ahead",
        ),
    ] = 0
    spill_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/GWh"),
        Field(
            alias="Spill Penalty",
            description='Penalty applied to spill from the storage to "the sea" in the last period of each simulation step.',
            ge=0,
        ),
    ] = 0.0001
    target: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Target",
            description="storage target",
            ge=0,
        ),
    ] = 0
    target_custom: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Target Custom",
            description="end of horizon storage target",
            ge=0,
        ),
    ] = 0
    target_day: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Target Day",
            description="end of day storage target",
            ge=0,
        ),
    ] = 0
    target_hour: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Target Hour",
            description="end of hour storage target",
            ge=0,
        ),
    ] = 0
    target_level: Annotated[
        float | int,
        PLEXOSProperty(units="m"),
        Field(
            alias="Target Level",
            description="storage target",
            ge=0,
        ),
    ] = 0
    target_level_day: Annotated[
        float | int,
        PLEXOSProperty(units="m"),
        Field(
            alias="Target Level Day",
            description="end of day storage target",
            ge=0,
        ),
    ] = 0
    target_level_hour: Annotated[
        float | int,
        PLEXOSProperty(units="m"),
        Field(
            alias="Target Level Hour",
            description="end of hour storage target",
            ge=0,
        ),
    ] = 0
    target_level_month: Annotated[
        float | int,
        PLEXOSProperty(units="m"),
        Field(
            alias="Target Level Month",
            description="end of month storage target",
            ge=0,
        ),
    ] = 0
    target_level_week: Annotated[
        float | int,
        PLEXOSProperty(units="m"),
        Field(
            alias="Target Level Week",
            description="end of week storage target",
            ge=0,
        ),
    ] = 0
    target_level_year: Annotated[
        float | int,
        PLEXOSProperty(units="m"),
        Field(
            alias="Target Level Year",
            description="end of year storage target",
            ge=0,
        ),
    ] = 0
    target_month: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Target Month",
            description="end of month storage target",
            ge=0,
        ),
    ] = 0
    target_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/GWh"),
        Field(
            alias="Target Penalty",
            description="Penalty for violating the target.",
        ),
    ] = -1
    target_week: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Target Week",
            description="end of weekly storage target",
            ge=0,
        ),
    ] = 0
    target_year: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Target Year",
            description="end of year storage target",
            ge=0,
        ),
    ] = 0
    trajectory_lower_bound_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Trajectory Lower Bound Penalty",
            description="Price for running the storage below the stochastic optimal storage trajectory",
        ),
    ] = -1
    trajectory_non_anticipativity: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Trajectory Non-anticipativity",
            description="Price for violating non-anticipativity constraints in scenario-wise decomposition mode",
        ),
    ] = -1
    trajectory_non_anticipativity_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Trajectory Non-anticipativity Time",
            description="Window of time over which to enforce non-anticipativity constraints in scenario-wise decomposition",
            ge=0,
        ),
    ] = 0
    trajectory_non_anticipativity_volume: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Trajectory Non-anticipativity Volume",
            description="Volume of violation of non-anticipativity constraints in band",
            ge=0,
        ),
    ] = 1e30
    trajectory_upper_bound_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Trajectory Upper Bound Penalty",
            description="Price for running the storage above the stochastic optimal storage trajectory",
        ),
    ] = -1
    units: Annotated[
        float | int,
        Field(
            alias="Units",
            description="Number of units of the storage",
            ge=0,
        ),
    ] = 1
    water_value: Annotated[
        float | int,
        PLEXOSProperty(units="usd/GWh"),
        Field(
            alias="Water Value",
            description="Incremental price of water released from storage",
        ),
    ] = 10000
    water_value_point: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Water Value Point",
            description="Volume associated with [Water Value] in multiple bands",
            ge=0,
        ),
    ] = 1e30
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

    # Storage Constraints Input Properties
    capacity_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Capacity Coefficient",
            description="Coefficient of energy storage capacity.",
        ),
    ] = 0
    end_level_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="m"),
        Field(
            alias="End Level Coefficient",
            description="Coefficient of storage end level.",
        ),
    ] = 0
    end_volume_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="End Volume Coefficient",
            description="Coefficient of storage end volume.",
        ),
    ] = 0
    generator_release_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Generator Release Coefficient",
            description="Coefficient of generator release.",
        ),
    ] = 0
    hours_full_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Hours Full Coefficient",
            description="Coefficient of the number of hours the storage is full.",
        ),
    ] = 0
    inflow_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Inflow Coefficient",
            description="Coefficient of inflow.",
        ),
    ] = 0
    initial_volume_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Initial Volume Coefficient",
            description="Coefficient of storage initial volume.",
        ),
    ] = 0
    loss_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Loss Coefficient",
            description="Coefficient of the loss from the storage.",
        ),
    ] = 0
    natural_inflow_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Natural Inflow Coefficient",
            description="Coefficient of natural inflow",
        ),
    ] = 0
    ramp_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Ramp Coefficient",
            description="Coefficient of change in storage end volume.",
        ),
    ] = 0
    release_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Release Coefficient",
            description="Coefficient of release.",
        ),
    ] = 0
    spill_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Spill Coefficient",
            description="Coefficient of spill.",
        ),
    ] = 0

    # Storage Objectives Input Properties
    objective_end_level_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="m"),
        Field(
            alias="End Level Coefficient",
            description="Coefficient of storage end level",
        ),
    ] = 0
    objective_end_volume_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="End Volume Coefficient",
            description="Coefficient of storage end volume",
        ),
    ] = 0
    objective_generator_release_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Generator Release Coefficient",
            description="Coefficient of generator release",
        ),
    ] = 0
    objective_hours_full_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Hours Full Coefficient",
            description="Coefficient of the number of hours the storage is full",
        ),
    ] = 0
    objective_inflow_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Inflow Coefficient",
            description="Coefficient of inflow",
        ),
    ] = 0
    objective_loss_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Loss Coefficient",
            description="Coefficient of the loss from the storage",
        ),
    ] = 0
    objective_natural_inflow_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Natural Inflow Coefficient",
            description="Coefficient of natural inflow",
        ),
    ] = 0
    objective_ramp_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Ramp Coefficient",
            description="Coefficient of change in storage end volume",
        ),
    ] = 0
    objective_release_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Release Coefficient",
            description="Coefficient of release",
        ),
    ] = 0
    objective_spill_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Spill Coefficient",
            description="Coefficient of spill",
        ),
    ] = 0

    # Storage Conditions Input Properties
    end_potential_energy_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="End Potential Energy Coefficient",
            description="Coefficient of the end potential energy in storage in the condition equation",
        ),
    ] = 0
    condition_end_volume_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="End Volume Coefficient",
            description="Coefficient of the end volume in storage in the condition equation",
        ),
    ] = 0
    condition_inflow_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Inflow Coefficient",
            description="Coefficient of inflow in the condition equation",
        ),
    ] = 0
    initial_potential_energy_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Initial Potential Energy Coefficient",
            description="Coefficient of the initial potential energy in storage in the condition equation",
        ),
    ] = 0
    condition_initial_volume_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Initial Volume Coefficient",
            description="Coefficient of the initial volume in storage in the condition equation",
        ),
    ] = 0
    condition_release_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Release Coefficient",
            description="Coefficient of release in the condition equation",
        ),
    ] = 0
    condition_spill_coefficient: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Spill Coefficient",
            description="Coefficient of spill in the condition equation",
        ),
    ] = 0

    @classmethod
    def example(cls) -> "PLEXOSStorage":
        """Create an example PLEXOSStorage."""
        return PLEXOSStorage(
            name="ExampleStorage",
            initial_volume=100.0,
        )
