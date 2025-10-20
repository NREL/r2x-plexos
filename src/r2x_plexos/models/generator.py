"""The following file contains Pydantic models for a PLEXOS Generator model."""

from typing import Annotated

from pydantic import Field

from .component import PLEXOSObject
from .property_specification import PLEXOSProperty


class PLEXOSGenerator(PLEXOSObject):
    """Class that holds attributes about PLEXOS generators."""

    expansion_economy_units: Annotated[
        float | int,
        Field(
            alias="Expansion Economy Units",
            description="Minimum number of units required for the expansion economy (band)",
            ge=0,
        ),
    ] = 0
    expansion_optimality: Annotated[
        int,
        PLEXOSProperty,
        Field(
            alias="Expansion Optimality",
            description="Expansion planning integerization scheme.",
            json_schema_extra={"enum": [0, 2]},
        ),
    ] = 2
    firm_capacity: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Firm Capacity",
            description="Contribution of the generator to capacity reserves",
        ),
    ] = 0
    firm_capacity_unit_count: Annotated[
        float | int,
        PLEXOSProperty,
        Field(
            alias="Firm Capacity Unit Count",
            description="The total number of units installed in band corresponding to the same band of [Firm Capacity]",
        ),
    ] = 0
    fixed_charge: Annotated[
        float | int,
        PLEXOSProperty(units="usd/kW/yr"),
        Field(
            alias="Fixed Charge",
            description="Generic annual fixed charge",
        ),
    ] = 0
    fixed_load: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Fixed Load",
            description="Fixed load",
        ),
    ] = 0
    fixed_load_global: Annotated[
        int,
        Field(
            alias="Fixed Load Global",
            description="If [Fixed Load] applies across all units or unit-by-unit",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    fixed_load_method: Annotated[
        int,
        Field(
            alias="Fixed Load Method",
            description="Method of interpreting zero values of the [Fixed Load] property.",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 1
    fixed_load_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Fixed Load Penalty",
            description="Penalty for violation of [Fixed Load].",
        ),
    ] = -1
    fixed_pump_load: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Fixed Pump Load",
            description="Fixed pump load",
        ),
    ] = 0
    fixed_pump_load_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Fixed Pump Load Penalty",
            description="Penalty for violation of [Fixed Pump Load].",
        ),
    ] = -1
    fom_charge: Annotated[
        float | int,
        PLEXOSProperty(units="usd/kW/yr"),
        Field(
            alias="FO&M Charge",
            description="Annual fixed operation and maintenance charge",
        ),
    ] = 0
    build_cost: Annotated[
        float | int,
        PLEXOSProperty(units="usd/kW"),
        Field(
            alias="Build Cost",
            description="Cost of building a generator unit",
        ),
    ] = 0
    economic_life: Annotated[
        float | int,
        PLEXOSProperty(units="yr"),
        Field(
            alias="Economic Life",
            description="Economic life of a generator unit (period over which fixed costs are recovered)",
            ge=0,
        ),
    ] = 30
    forced_outage: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            validation_alias="forced_outage",
            alias="Forced Outage",
            description="Capacity lost to forced outage",
            ge=0,
        ),
    ] = 0
    forced_outage_rate: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            validation_alias="forced_outage_rate",
            alias="Forced Outage Rate",
            description="Expected proportion of time the facility is unavailable due to forced outage",
            ge=0,
            le=100,
        ),
    ] = 0
    forced_outage_rate_denominator: Annotated[
        int,
        Field(
            alias="Forced Outage Rate Denominator",
            description="Denominator for Forced Outage Rate calculations",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    formulate_non_convex: Annotated[
        int,
        Field(
            alias="Formulate Non-convex",
            description="Controls when integers are used to enforce clearing of marginal efficiency tranches in order.",
            json_schema_extra={"enum": [0, 1, 2]},
        ),
    ] = 1
    formulate_risk: Annotated[
        int,
        Field(
            alias="Formulate Risk",
            description="If constraint should be formulated to bound net profit risk",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    fuel_mix_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/GJ"),
        Field(
            alias="Fuel Mix Penalty",
            description="Penalty applied to violations of fuel mixing constraints",
            ge=0,
        ),
    ] = 0
    fuel_price: Annotated[
        float | int,
        PLEXOSProperty(units="usd/GJ"),
        Field(
            alias="Fuel Price",
            description="Fuel price (when not using Fuels collection)",
        ),
    ] = 0
    generating_units: Annotated[
        float | int,
        Field(
            alias="Generating Units",
            description="Number of generating units",
            ge=0,
        ),
    ] = 1e30
    generation_credit: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Generation Credit",
            description="Credit received for generation",
        ),
    ] = 0
    generation_credit_start_year: Annotated[
        float | int,
        Field(
            alias="Generation Credit Start Year",
            description="Year to start applying Generation Credits",
            ge=0,
        ),
    ] = 0
    generation_non_anticipativity: Annotated[
        float | int,
        PLEXOSProperty(units="usd"),
        Field(
            alias="Generation Non-anticipativity",
            description="Price for violating non-anticipativity constraints in scenario-wise decomposition mode",
        ),
    ] = 0
    generation_non_anticipativity_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Generation Non-anticipativity Time",
            description="Window of time over which to enforce non-anticipativity constraints in scenario-wise decomposition",
            ge=0,
        ),
    ] = 0
    generation_transition_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Generation Transition Time",
            description="Minimum time between operating in generation mode and pump mode",
            ge=0,
        ),
    ] = 0
    head_effects_method: Annotated[
        int,
        Field(
            alias="Head Effects Method",
            description="Method used to account for the effects of storage head on efficiency",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    heat_injection_charge: Annotated[
        float | int,
        PLEXOSProperty(units="usd/GJ"),
        Field(
            alias="Heat Injection Charge",
            description="Incremental cost of injecting heat into the storage",
        ),
    ] = 0
    heat_load: Annotated[
        float | int,
        PLEXOSProperty(units="GJ"),
        Field(
            alias="Heat Load",
            description="Waste heat that must be extracted for exogenous loads (CCGT)",
        ),
    ] = 0
    heat_loss: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Heat Loss",
            description="Rate at which heat is lost from storage",
        ),
    ] = 0
    heat_rate: Annotated[
        float | int,
        PLEXOSProperty(units="GJ/MWh"),
        Field(
            alias="Heat Rate",
            description="Average heat rate (total fuel divided by total generation)",
        ),
    ] = 0
    heat_rate_base: Annotated[
        float | int,
        PLEXOSProperty(units="GJ/h"),
        Field(
            alias="Heat Rate Base",
            description="Constant term in fuel use function (no-load cost)",
        ),
    ] = 0
    heat_rate_incr: Annotated[
        float | int,
        PLEXOSProperty(units="GJ/MWh"),
        Field(
            alias="Heat Rate Incr",
            description="First-order polynomial term in unit fuel use function (marginal heat rate)",
        ),
    ] = 0
    heat_rate_incr2: Annotated[
        float | int,
        PLEXOSProperty(units="GJ/MWh^2"),
        Field(
            alias="Heat Rate Incr2",
            description="Second-order polynomial term in unit fuel use function",
        ),
    ] = 0
    heat_rate_incr3: Annotated[
        float | int,
        PLEXOSProperty(units="GJ/MWh^3"),
        Field(
            alias="Heat Rate Incr3",
            description="Third-order polynomial term in unit fuel use function",
        ),
    ] = 0
    heat_withdrawal_charge: Annotated[
        float | int,
        PLEXOSProperty(units="usd/GJ"),
        Field(
            alias="Heat Withdrawal Charge",
            description="Incremental cost of withdrawing heat from the storage",
        ),
    ] = 0
    hint_units_built: Annotated[
        float | int,
        Field(
            alias="Hint Units Built",
            description="Capacity expansion solution to be passed to the optimizer as a hint or initial solution",
            ge=0,
        ),
    ] = 0
    hint_units_retired: Annotated[
        float | int,
        Field(
            alias="Hint Units Retired",
            description="Capacity expansion solution to be passed to the optimizer as a hint or initial solution",
            ge=0,
        ),
    ] = 0
    hydro_efficiency_optimality: Annotated[
        int,
        Field(
            alias="Hydro Efficiency Optimality",
            description="Controls when integers are used to enforce multi-band hydro efficiency functions to dispatch in order",
            json_schema_extra={"enum": [-1, 0, 2]},
        ),
    ] = -1
    hydro_power_curve: Annotated[
        int,
        Field(
            alias="Hydro Power Curve",
            description="Hydro power curve representation",
            json_schema_extra={"enum": [0, 1, 2]},
        ),
    ] = 0
    include_in_capacity_payments: Annotated[
        int,
        Field(
            alias="Include in Capacity Payments",
            description="If this generator receives capacity payments.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    include_in_rounded_relaxation_merit_order: Annotated[
        int,
        Field(
            alias="Include in Rounded Relaxation Merit Order",
            description="Flag if Generator is included in the Region merit order for Rounded Relaxation unit commitment.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    include_in_uplift: Annotated[
        int,
        Field(
            alias="Include in Uplift",
            description="If this generator's costs are included in the uplift calculations.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    incremental_heat_rate: Annotated[
        float | int,
        PLEXOSProperty(units="GJ/GJ"),
        Field(
            alias="Incremental Heat Rate",
            description="Incremental heat rate for conversion of fuel to electricity",
        ),
    ] = 0
    inertia_constant: Annotated[
        float | int,
        PLEXOSProperty(units="s"),
        Field(
            alias="Inertia Constant",
            description="Stored energy per unit of capacity",
            ge=0,
        ),
    ] = 0
    initial_age: Annotated[
        float | int,
        PLEXOSProperty(units="yr"),
        Field(
            alias="Initial Age",
            description="Average age of units at the start of the simulation horizon",
        ),
    ] = 0
    initial_generation: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Initial Generation",
            description="Generation at time zero",
            ge=0,
        ),
    ] = 0
    initial_hours_down: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Initial Hours Down",
            description="Hours the unit has been down for at time zero",
            ge=0,
        ),
    ] = 1e30
    initial_hours_pumping: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Initial Hours Pumping",
            description="Hours the unit has been pumping for at time zero",
            ge=0,
        ),
    ] = 1e30
    initial_hours_up: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Initial Hours Up",
            description="Hours the unit has been up for at time zero",
            ge=0,
        ),
    ] = 0
    initial_operating_hours: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Initial Operating Hours",
            description="Hours the unit has been operating since the last forced outage",
            ge=0,
        ),
    ] = 0
    initial_units_generating: Annotated[
        float | int,
        Field(
            alias="Initial Units Generating",
            description="Number of units generating at time zero",
            ge=0,
        ),
    ] = 0
    initial_units_pumping: Annotated[
        float | int,
        Field(
            alias="Initial Units Pumping",
            description="Number of units pumping at time zero",
            ge=0,
        ),
    ] = 0
    integerization_horizon: Annotated[
        float | int,
        PLEXOSProperty(units="yr"),
        Field(
            alias="Integerization Horizon",
            description="Number of years over which the expansion decisions are integerized",
            ge=-1,
        ),
    ] = -1
    internal_volume_scalar: Annotated[
        float | int,
        Field(
            alias="Internal Volume Scalar",
            description="Storage volume scaling factor used internal to the mathematical program.",
            gt=0,
        ),
    ] = 1
    investment_tax_credit: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Investment Tax Credit",
            description="Percentage of the annualized build cost to apply to investment credit",
        ),
    ] = 0
    is_strategic: Annotated[
        int,
        Field(
            alias="Is Strategic",
            description="If the generator's capacity acts strategically",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    last_start_state: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Last Start State",
            description="Number of hours the unit had been down before the last start",
            ge=0,
        ),
    ] = 0
    lccr_apply_discounting: Annotated[
        int,
        PLEXOSProperty(is_enum=True),
        Field(
            alias="LCCR Apply Discounting",
            description="If the discount rate should be applied to the LCCR",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    lead_time: Annotated[
        float | int,
        PLEXOSProperty(units="yr"),
        Field(
            alias="Lead Time",
            description="Number of years after which the expansion project can begin",
            ge=0,
        ),
    ] = 0
    levelized_capital_carrying_rate: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Levelized Capital Carrying Rate",
            description="Levelized capital carrying rate",
            ge=0,
        ),
    ] = 0
    load_following_factor: Annotated[
        float | int,
        PLEXOSProperty,
        Field(
            alias="Load Following Factor",
            description="Regression factor for proportional load following",
        ),
    ] = 1
    load_following_profile: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Load Following Profile",
            description="Profile to follow for proportional load following",
        ),
    ] = 0
    load_point: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Load Point",
            description="Load point for use with multi-point heat rate.",
            ge=0,
        ),
    ] = 0
    load_subtracter: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Load Subtracter",
            description="Generation subtracted from the System load duration curve prior to slicing.",
        ),
    ] = 0
    load_subtracter_global: Annotated[
        int,
        Field(
            alias="Load Subtracter Global",
            description="If [Load Subtracter] applies across all units or unit-by-unit",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    maintenance_frequency: Annotated[
        float | int,
        Field(
            alias="Maintenance Frequency",
            description="Frequency of maintenance outages in an annual time frame",
            ge=0,
        ),
    ] = 0
    maintenance_rate: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Maintenance Rate",
            description="Expected proportion of time the facility is unavailable due to maintenance",
            ge=0,
            le=100,
        ),
    ] = 0
    marginal_loss_factor: Annotated[
        float | int,
        Field(
            alias="Marginal Loss Factor",
            description="Transmission marginal loss factor (MLF or TLF)",
            ge=0,
        ),
    ] = 1
    mark_up: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Mark-up",
            description="Mark-up above marginal cost",
        ),
    ] = 0
    mark_up_point: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Mark-up Point",
            description="Load point for use with multi-point [Mark-up] or [Bid-Cost Mark-up].",
        ),
    ] = 0
    max_boiler_heat: Annotated[
        float | int,
        PLEXOSProperty(units="GJ"),
        Field(
            alias="Max Boiler Heat",
            description="Maximum heat production from ancillary boiler",
        ),
    ] = 0
    max_build_events: Annotated[
        float | int,
        Field(
            alias="Max Build Events",
            description="Maximum number of distinct build events allowed over the planning horizon",
            ge=0,
        ),
    ] = 1e30
    max_capacity: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Capacity",
            description="Maximum generating capacity of each unit",
            ge=0,
        ),
    ] = 0
    max_capacity_factor: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Capacity Factor",
            description="Maximum capacity factor (energy constraint)",
            ge=0,
            le=100,
        ),
    ] = 100
    max_capacity_factor_day: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Capacity Factor Day",
            description="Maximum capacity factor in day",
            ge=0,
            le=100,
        ),
    ] = 100
    max_capacity_factor_hour: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Capacity Factor Hour",
            description="Maximum capacity factor in hour",
            ge=0,
            le=100,
        ),
    ] = 100
    max_capacity_factor_month: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Capacity Factor Month",
            description="Maximum capacity factor in month",
            ge=0,
            le=100,
        ),
    ] = 100
    max_capacity_factor_week: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Capacity Factor Week",
            description="Maximum capacity factor in week",
            ge=0,
            le=100,
        ),
    ] = 100
    max_capacity_factor_year: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Capacity Factor Year",
            description="Maximum capacity factor in year",
            ge=0,
            le=100,
        ),
    ] = 100
    max_down_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Max Down Time",
            description="Maximum number of hours a unit can be off after being shut down",
            ge=0,
        ),
    ] = 0
    max_down_time_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/h"),
        Field(
            alias="Max Down Time Penalty",
            description="Penalty for violation of max down time",
        ),
    ] = -1
    max_energy: Annotated[
        float | int,
        PLEXOSProperty(units="MWh"),
        Field(
            alias="Max Energy",
            description="Maximum energy",
            ge=0,
        ),
    ] = 1e30
    max_energy_day: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Energy Day",
            description="Maximum energy in day",
            ge=0,
        ),
    ] = 1e30
    max_energy_hour: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Energy Hour",
            description="Maximum energy in hour",
            ge=0,
        ),
    ] = 1e30
    max_energy_month: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Energy Month",
            description="Maximum energy in month",
            ge=0,
        ),
    ] = 1e30
    max_energy_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/GWh"),
        Field(
            alias="Max Energy Penalty",
            description="Penalty applied to violations of [Max Energy] and [Max Capacity Factor] constraints.",
        ),
    ] = -1
    max_energy_week: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Energy Week",
            description="Maximum energy in week",
            ge=0,
        ),
    ] = 1e30
    max_energy_year: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Energy Year",
            description="Maximum energy in year",
            ge=0,
        ),
    ] = 1e30
    max_generation_curtailed: Annotated[
        float | int,
        PLEXOSProperty(units="MWh"),
        Field(
            alias="Max Generation Curtailed",
            description="Maximum generation curtailed",
            ge=0,
        ),
    ] = 1e30
    max_generation_curtailed_day: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Generation Curtailed Day",
            description="Maximum generation curtailed in day",
            ge=0,
        ),
    ] = 1e30
    max_generation_curtailed_hour: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Generation Curtailed Hour",
            description="Maximum generation curtailed in hour",
            ge=0,
        ),
    ] = 1e30
    max_generation_curtailed_month: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Generation Curtailed Month",
            description="Maximum generation curtailed in month",
            ge=0,
        ),
    ] = 1e30
    max_generation_curtailed_week: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Generation Curtailed Week",
            description="Maximum generation curtailed in week",
            ge=0,
        ),
    ] = 1e30
    max_generation_curtailed_year: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Generation Curtailed Year",
            description="Maximum generation curtailed in year",
            ge=0,
        ),
    ] = 1e30
    max_generation_curtailment_factor: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Generation Curtailment Factor",
            description="Maximum proportion of generation curtailed",
            ge=0,
        ),
    ] = 100
    max_generation_curtailment_factor_day: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Generation Curtailment Factor Day",
            description="Maximum proportion of generation curtailed in day",
            ge=0,
        ),
    ] = 100
    max_generation_curtailment_factor_hour: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Generation Curtailment Factor Hour",
            description="Maximum proportion of generation curtailed in hour",
            ge=0,
        ),
    ] = 100
    max_generation_curtailment_factor_month: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Generation Curtailment Factor Month",
            description="Maximum proportion of generation curtailed in month",
            ge=0,
        ),
    ] = 100
    max_generation_curtailment_factor_week: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Generation Curtailment Factor Week",
            description="Maximum proportion of generation curtailed in week",
            ge=0,
        ),
    ] = 100
    max_generation_curtailment_factor_year: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Generation Curtailment Factor Year",
            description="Maximum proportion of generation curtailed in year",
            ge=0,
        ),
    ] = 100
    max_heat: Annotated[
        int | float,
        PLEXOSProperty(units="GJ"),
        Field(
            alias="Max Heat",
            description="Maximum heat allowed in storage",
        ),
    ] = 1e30
    max_heat_injection: Annotated[
        int | float,
        PLEXOSProperty(units="GJ"),
        Field(
            alias="Max Heat Injection",
            description="Maximum amount of heat that can be injected into the storage",
            ge=0,
        ),
    ] = 1e30
    max_heat_injection_day: Annotated[
        float,
        PLEXOSProperty(units="TJ"),
        Field(
            alias="Max Heat Injection Day",
            description="Maximum amount of heat that can be injected into the storage in a day",
            ge=0,
        ),
    ] = 1e30
    max_heat_injection_hour: Annotated[
        int | float,
        PLEXOSProperty(units="GJ"),
        Field(
            alias="Max Heat Injection Hour",
            description="Maximum amount of heat that can be injected into the storage in a hour",
            ge=0,
        ),
    ] = 1e30
    max_heat_injection_month: Annotated[
        int | float,
        PLEXOSProperty(units="TJ"),
        Field(
            alias="Max Heat Injection Month",
            description="Maximum amount of heat that can be injected into the storage in a month",
            ge=0,
        ),
    ] = 1e30
    max_heat_injection_week: Annotated[
        int | float,
        PLEXOSProperty(units="TJ"),
        Field(
            alias="Max Heat Injection Week",
            description="Maximum amount of heat that can be injected into the storage in a week",
            ge=0,
        ),
    ] = 1e30
    max_heat_injection_year: Annotated[
        int | float,
        PLEXOSProperty(units="TJ"),
        Field(
            alias="Max Heat Injection Year",
            description="Maximum amount of heat that can be injected into the storage in a year",
            ge=0,
        ),
    ] = 1e30
    max_heat_penalty: Annotated[
        int | float,
        PLEXOSProperty(units="usd/GJ"),
        Field(
            alias="Max Heat Penalty",
            description="Adds a penalty to the max heat constraint to allow for relaxation",
            ge=-1,
        ),
    ] = -1
    max_heat_rate_tranches: Annotated[
        int | float,
        Field(
            alias="Max Heat Rate Tranches",
            description="Maximum number of tranches in the fuel function piecewise linear approximation.",
            ge=0,
            le=100,
        ),
    ] = 0
    max_heat_withdrawal: Annotated[
        int | float,
        PLEXOSProperty(units="GJ"),
        Field(
            alias="Max Heat Withdrawal",
            description="Maximum amount of heat that can be withdrawn from the storage",
            ge=0,
        ),
    ] = 1e30
    max_heat_withdrawal_day: Annotated[
        int | float,
        PLEXOSProperty(units="TJ"),
        Field(
            alias="Max Heat Withdrawal Day",
            description="Maximum amount of heat that can be withdrawn from the storage in a day",
            ge=0,
        ),
    ] = 1e30
    max_heat_withdrawal_hour: Annotated[
        int | float,
        PLEXOSProperty(units="GJ"),
        Field(
            alias="Max Heat Withdrawal Hour",
            description="Maximum amount of heat that can be withdrawn from the storage in a hour",
            ge=0,
        ),
    ] = 1e30
    max_heat_withdrawal_month: Annotated[
        int | float,
        PLEXOSProperty(units="TJ"),
        Field(
            alias="Max Heat Withdrawal Month",
            description="Maximum amount of heat that can be withdrawn from the storage in a month",
            ge=0,
        ),
    ] = 1e30
    max_heat_withdrawal_week: Annotated[
        int | float,
        PLEXOSProperty(units="TJ"),
        Field(
            alias="Max Heat Withdrawal Week",
            description="Maximum amount of heat that can be withdrawn from the storage in a week",
            ge=0,
        ),
    ] = 1e30
    max_heat_withdrawal_year: Annotated[
        int | float,
        PLEXOSProperty(units="TJ"),
        Field(
            alias="Max Heat Withdrawal Year",
            description="Maximum amount of heat that can be withdrawn from the storage in a year",
            ge=0,
        ),
    ] = 1e30
    max_load: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Load",
            description="Maximum level of unit load (unit may provide spinning reserve with remainder of spare capacity)",
            ge=0,
        ),
    ] = 0
    max_load_penalty: Annotated[
        int | float,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Max Load Penalty",
            description="Penalty for violation of [Max Load].",
            ge=-1,
        ),
    ] = -1
    max_pump_up_time: Annotated[
        int | float,
        PLEXOSProperty(units="h"),
        Field(
            alias="Max Pump Up Time",
            description="Maximum number of hours a unit can be run in pump mode after being started",
            ge=0,
        ),
    ] = 0
    max_ramp_down: Annotated[
        float | int,
        PLEXOSProperty(units="MW/min"),
        Field(
            alias="Max Ramp Down",
            description="Maximum ramp down rate that applies at the given load point",
            ge=0,
        ),
    ] = 1e30
    max_ramp_down_penalty: Annotated[
        int | float,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Max Ramp Down Penalty",
            description="Penalty for violating [Max Ramp Down] constraint.",
        ),
    ] = -1
    max_ramp_up: Annotated[
        float | int,
        PLEXOSProperty(units="MW/min"),
        Field(
            alias="Max Ramp Up",
            description="Maximum ramp up rate that applies at the given load point",
            ge=0,
        ),
    ] = 1e30
    max_ramp_up_penalty: Annotated[
        int | float,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Max Ramp Up Penalty",
            description="Penalty for violating [Max Ramp Up] constraint.",
        ),
    ] = -1
    max_release: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Release",
            description="Maximum rate of release from each unit",
            ge=0,
        ),
    ] = 1e30
    max_starts: Annotated[
        float | int,
        Field(
            alias="Max Starts",
            description="Maximum number of starts allowed",
            ge=0,
        ),
    ] = 1e30
    max_starts_day: Annotated[
        int | float,
        Field(
            alias="Max Starts Day",
            description="Maximum number of starts allowed in a day",
            ge=0,
        ),
    ] = 1e30
    max_starts_hour: Annotated[
        int | float,
        Field(
            alias="Max Starts Hour",
            description="Maximum number of starts allowed in a hour",
            ge=0,
        ),
    ] = 1e30
    max_starts_month: Annotated[
        int | float,
        Field(
            alias="Max Starts Month",
            description="Maximum number of starts allowed in a month",
            ge=0,
        ),
    ] = 1e30
    max_starts_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd"),
        Field(
            alias="Max Starts Penalty",
            description="Penalty applied to violations of [Max Starts] constraints.",
        ),
    ] = -1
    max_starts_week: Annotated[
        int | float,
        Field(
            alias="Max Starts Week",
            description="Maximum number of starts allowed in a week",
            ge=0,
        ),
    ] = 1e30
    max_starts_year: Annotated[
        int | float,
        Field(
            alias="Max Starts Year",
            description="Maximum number of starts allowed in a year",
            ge=0,
        ),
    ] = 1e30
    max_time_to_repair: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Max Time To Repair",
            description="Maximum time to repair",
            ge=0,
        ),
    ] = 0
    max_total_maintenance_percent: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Total Maintenance Percent",
            description="Maximum percent of total maintenance scheduled within the time frame",
            ge=0,
            le=100,
        ),
    ] = 100
    max_units_built: Annotated[
        float | int,
        PLEXOSProperty,
        Field(
            alias="Max Units Built",
            description="Maximum number of units automatically constructed in aggregate over the planning horizon",
            ge=0,
        ),
    ] = 0
    max_units_built_in_year: Annotated[
        int | float,
        PLEXOSProperty,
        Field(
            alias="Max Units Built in Year",
            description="Maximum number of units automatically constructed in any single year of the planning horizon",
            ge=0,
        ),
    ] = 1e30
    max_units_pumping: Annotated[
        int | float,
        Field(
            alias="Max Units Pumping",
            description="Maximum number of units allowed to be running in pump mode.",
            ge=0,
        ),
    ] = 1e30
    max_units_retired: Annotated[
        int | float,
        PLEXOSProperty,
        Field(
            alias="Max Units Retired",
            description="Maximum number of units automatically retired in aggregate over the planning horizon",
            ge=0,
        ),
    ] = 0
    max_units_retired_in_year: Annotated[
        int | float,
        Field(
            alias="Max Units Retired in Year",
            description="Maximum number of units automatically retired in any single year of the planning horizon",
            ge=0,
        ),
    ] = 1e30
    max_up_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Max Up Time",
            description="Maximum number of hours a unit can be run after being started",
            ge=0,
        ),
    ] = 0
    max_up_time_penalty: Annotated[
        int | float,
        PLEXOSProperty(units="usd/h"),
        Field(
            alias="Max Up Time Penalty",
            description="Penalty for violation of max up time",
            ge=-1,
        ),
    ] = -1
    mean_time_to_repair: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Mean Time to Repair",
            description="Mean time to repair",
            ge=0,
        ),
    ] = 24
    min_capacity_factor: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Min Capacity Factor",
            description="Minimum capacity factor",
            ge=0,
            le=100,
        ),
    ] = 0
    min_capacity_factor_day: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Min Capacity Factor Day",
            description="Minimum capacity factor in day",
            ge=0,
            le=100,
        ),
    ] = 0
    min_capacity_factor_hour: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Min Capacity Factor Hour",
            description="Minimum capacity factor in hour",
            ge=0,
            le=100,
        ),
    ] = 0
    min_capacity_factor_month: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Min Capacity Factor Month",
            description="Minimum capacity factor in month",
            ge=0,
            le=100,
        ),
    ] = 0
    min_capacity_factor_week: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Min Capacity Factor Week",
            description="Minimum capacity factor in week",
            ge=0,
            le=100,
        ),
    ] = 0
    min_capacity_factor_year: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Min Capacity Factor Year",
            description="Minimum capacity factor in year",
            ge=0,
            le=100,
        ),
    ] = 0
    min_down_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Min Down Time",
            description="Minimum number of hours a unit must be off after being shut down",
            ge=0,
        ),
    ] = 0
    min_down_time_mode: Annotated[
        int,
        Field(
            alias="Min Down Time Mode",
            description="Controls how [Min Down Time] is applied after outages.",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 1
    min_down_time_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/h"),
        Field(
            alias="Min Down Time Penalty",
            description="Penalty for violation of min down time",
            ge=-1,
        ),
    ] = -1
    min_energy: Annotated[
        float | int,
        PLEXOSProperty(units="MWh"),
        Field(
            alias="Min Energy",
            description="Minimum energy",
            ge=0,
        ),
    ] = 0
    min_energy_day: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Min Energy Day",
            description="Minimum energy in day",
            ge=0,
        ),
    ] = 0
    min_energy_hour: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Min Energy Hour",
            description="Minimum energy in hour",
            ge=0,
        ),
    ] = 0
    min_energy_month: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Min Energy Month",
            description="Minimum energy in month",
            ge=0,
        ),
    ] = 0
    min_energy_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/GWh"),
        Field(
            alias="Min Energy Penalty",
            description="Penalty applied to violations of [Min Energy] and [Min Capacity Factor] constraints.",
        ),
    ] = 10000000
    min_energy_week: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Min Energy Week",
            description="Minimum energy in week",
            ge=0,
        ),
    ] = 0
    min_energy_year: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Min Energy Year",
            description="Minimum energy in year",
            ge=0,
        ),
    ] = 0
    min_heat: Annotated[
        float | int,
        PLEXOSProperty(units="GJ"),
        Field(
            alias="Min Heat",
            description="Minimum heat allowed in storage",
        ),
    ] = 0
    min_heat_injection: Annotated[
        float | int,
        PLEXOSProperty(units="GJ"),
        Field(
            alias="Min Heat Injection",
            description="Amount of heat that must be injected into the storage",
            ge=0,
        ),
    ] = 0
    min_heat_injection_day: Annotated[
        float | int,
        PLEXOSProperty(units="TJ"),
        Field(
            alias="Min Heat Injection Day",
            description="Amount of heat that must be injected into the storage each day",
            ge=0,
        ),
    ] = 0
    min_heat_injection_hour: Annotated[
        float | int,
        PLEXOSProperty(units="GJ"),
        Field(
            alias="Min Heat Injection Hour",
            description="Amount of heat that must be injected into the storage each hour",
            ge=0,
        ),
    ] = 0
    min_heat_injection_month: Annotated[
        float | int,
        PLEXOSProperty(units="TJ"),
        Field(
            alias="Min Heat Injection Month",
            description="Amount of heat that must be injected into the storage each month",
            ge=0,
        ),
    ] = 0
    min_heat_injection_week: Annotated[
        float | int,
        PLEXOSProperty(units="TJ"),
        Field(
            alias="Min Heat Injection Week",
            description="Amount of heat that must be injected into the storage each week",
            ge=0,
        ),
    ] = 0
    min_heat_injection_year: Annotated[
        float | int,
        PLEXOSProperty(units="TJ"),
        Field(
            alias="Min Heat Injection Year",
            description="Amount of heat that must be injected into the storage each year",
            ge=0,
        ),
    ] = 0
    min_heat_withdrawal: Annotated[
        float | int,
        PLEXOSProperty(units="GJ"),
        Field(
            alias="Min Heat Withdrawal",
            description="Amount of heat that must be withdrawn from storage",
            ge=0,
        ),
    ] = 0
    min_heat_withdrawal_day: Annotated[
        float | int,
        PLEXOSProperty(units="TJ"),
        Field(
            alias="Min Heat Withdrawal Day",
            description="Amount of heat that must be withdrawn from storage each day",
            ge=0,
        ),
    ] = 0
    min_heat_withdrawal_hour: Annotated[
        float | int,
        PLEXOSProperty(units="GJ"),
        Field(
            alias="Min Heat Withdrawal Hour",
            description="Amount of heat that must be withdrawn from storage each hour",
            ge=0,
        ),
    ] = 0
    min_heat_withdrawal_month: Annotated[
        float | int,
        PLEXOSProperty(units="TJ"),
        Field(
            alias="Min Heat Withdrawal Month",
            description="Amount of heat that must be withdrawn from storage each month",
            ge=0,
        ),
    ] = 0
    min_heat_withdrawal_week: Annotated[
        float | int,
        PLEXOSProperty(units="TJ"),
        Field(
            alias="Min Heat Withdrawal Week",
            description="Amount of heat that must be withdrawn from storage each week",
            ge=0,
        ),
    ] = 0
    min_heat_withdrawal_year: Annotated[
        float | int,
        PLEXOSProperty(units="TJ"),
        Field(
            alias="Min Heat Withdrawal Year",
            description="Amount of heat that must be withdrawn from storage each year",
            ge=0,
        ),
    ] = 0
    min_load: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Min Load",
            description="Minimum level of station load (must run/run of river)",
            ge=0,
        ),
    ] = 0
    min_load_global: Annotated[
        int,
        Field(
            alias="Min Load Global",
            description="If [Min Load] applies across all units or unit-by-unit",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    min_load_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Min Load Penalty",
            description="Penalty for violation of [Min Load].",
        ),
    ] = -1
    min_pump_down_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Min Pump Down Time",
            description="Minimum number of hours a unit must be off after being shut down from pump mode",
            ge=0,
        ),
    ] = 0
    min_pump_load: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Min Pump Load",
            description="Minimum unit load while pumping",
            ge=0,
        ),
    ] = 0
    min_pump_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Min Pump Time",
            description="Minimum number of hours a unit must be run in pump mode after being started",
            ge=0,
        ),
    ] = 0
    min_stable_factor: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Min Stable Factor",
            description="Minimum stable generation level as a proportion of [Max Capacity]",
            ge=0,
        ),
    ] = 0
    min_stable_level: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Min Stable Level",
            description="Minimum stable generation level",
            ge=0,
        ),
    ] = 0
    min_stable_level_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Min Stable Level Penalty",
            description="Penalty applied to violation of min stable level.",
        ),
    ] = -1
    min_time_between_maintenance: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Min Time Between Maintenance",
            description="Minimum time between maintenance events",
            ge=0,
        ),
    ] = 0
    min_time_to_repair: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Min Time To Repair",
            description="Minimum time to repair",
            ge=0,
        ),
    ] = 0
    min_total_maintenance_percent: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Min Total Maintenance Percent",
            description="Minimum percent of total maintenance scheduled within the time frame",
            ge=0,
            le=100,
        ),
    ] = 0
    min_units_built: Annotated[
        float | int,
        PLEXOSProperty,
        Field(
            alias="Min Units Built",
            description="Minimum number of units automatically constructed in aggregate over the planning horizon",
            ge=0,
        ),
    ] = 0
    min_units_built_in_year: Annotated[
        float | int,
        PLEXOSProperty,
        Field(
            alias="Min Units Built in Year",
            description="Minimum number of units automatically constructed in any single year of the planning horizon",
            ge=0,
        ),
    ] = 0
    min_units_retired: Annotated[
        float | int,
        PLEXOSProperty,
        Field(
            alias="Min Units Retired",
            description="Minimum number of units automatically retired in aggregate over the planning horizon",
            ge=0,
        ),
    ] = 0
    min_units_retired_in_year: Annotated[
        float | int,
        PLEXOSProperty,
        Field(
            alias="Min Units Retired in Year",
            description="Minimum number of units automatically retired in any single year of the planning horizon",
            ge=0,
        ),
    ] = 0
    min_up_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Min Up Time",
            description="Minimum number of hours a unit must be run after being started",
            ge=0,
        ),
    ] = 0
    min_up_time_by_cooling_state: Annotated[
        int,
        Field(
            alias="Min Up Time by Cooling State",
            description="Determines whether cooling states are considered for Min Up Time",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    min_up_time_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/h"),
        Field(
            alias="Min Up Time Penalty",
            description="Penalty for violation of min up time",
            ge=-1,
        ),
    ] = -1
    model_capital_cost_recovery: Annotated[
        int,
        Field(
            alias="Model Capital Cost Recovery",
            description="Indicates if capital cost recovery feature is modeled for expansion planning",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    must_pump_units: Annotated[
        float | int,
        Field(
            alias="Must Pump Units",
            description="Number of pump units that must be running in pump mode",
            ge=0,
        ),
    ] = 0
    must_report: Annotated[
        int,
        Field(
            alias="Must Report",
            description="If the generator must be reported even if it is out-of-service",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    must_run_sync_cond_units: Annotated[
        float | int,
        Field(
            alias="Must-run Sync Cond Units",
            description="Number of must-run synchronous condenser units",
            ge=0,
        ),
    ] = 0
    must_run_units: Annotated[
        float | int,
        PLEXOSProperty,
        Field(
            alias="Must-Run Units",
            description="Number of must-run units",
            ge=0,
        ),
    ] = 0
    natural_inflow: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Natural Inflow",
            description="Natural inflow to the generator (controllable and uncontrolled)",
        ),
    ] = 0
    offer_base: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Offer Base",
            description="Base dispatch point for incr/decr style offer",
        ),
    ] = 0
    offer_no_load_cost: Annotated[
        float | int,
        PLEXOSProperty(units="usd/h"),
        Field(
            alias="Offer No Load Cost",
            description="Fixed dispatch cost component of generator offer.",
        ),
    ] = 0

    offer_price: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Offer Price",
            description="Price of energy in band",
        ),
    ] = 10000
    offer_price_incr: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Offer Price Incr",
            description="Adder applied to the [Offer Price] property",
        ),
    ] = 0
    offer_price_scalar: Annotated[
        float | int,
        Field(
            alias="Offer Price Scalar",
            description="Scalar applied to the [Offer Price] property",
        ),
    ] = 1
    offer_quantity: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Offer Quantity",
            description="Quantity offered in band",
        ),
    ] = 0
    offer_quantity_format: Annotated[
        int,
        Field(
            alias="Offer Quantity Format",
            description="Format for [Offer Quantity] and [Offer Price]",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    offer_quantity_scalar: Annotated[
        float | int,
        Field(
            alias="Offer Quantity Scalar",
            description="Scalar applied to the [Offer Quantity] property",
        ),
    ] = 1
    offers_must_clear_in_order: Annotated[
        int,
        Field(
            alias="Offers Must Clear in Order",
            description="Flag to control ordering of clearing of user-defined generator offers.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    one_time_cost: Annotated[
        float | int,
        PLEXOSProperty(units="usd"),
        Field(
            alias="One-time Cost",
            description="One-time cost associated with the project",
        ),
    ] = 0
    opening_heat: Annotated[
        float | int,
        PLEXOSProperty(units="GJ"),
        Field(
            alias="Opening Heat",
            description="Initial heat in the storage",
            ge=0,
        ),
    ] = 0
    outage_factor: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Outage Factor",
            description="Unit outage rating based on the unit capacity",
            ge=0,
            le=100,
        ),
    ] = 100
    outage_pump_load: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Outage Pump Load",
            description="Load drawn by a unit in pumping mode",
        ),
    ] = 1e30
    outage_rating: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Outage Rating",
            description="Unit rating during outage",
        ),
    ] = 0
    power_degradation: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Power Degradation",
            description="Annual degradation of power with age",
        ),
    ] = 0
    power_to_heat_ratio: Annotated[
        float | int,
        Field(
            alias="Power to Heat Ratio",
            description="Ratio of heat production to electric production",
        ),
    ] = 0
    price_following: Annotated[
        float | int,
        PLEXOSProperty,
        Field(
            alias="Price Following",
            description="Proportion of energy optimized, where the remainder is proportional load following",
        ),
    ] = 1
    price_setting: Annotated[
        int,
        Field(
            alias="Price Setting",
            description="Flag if the generator can set price",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    production_decomposition_method: Annotated[
        int,
        Field(
            alias="Production Decomposition Method",
            description="Method used to decompose generator production from MT to ST Schedule",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    project_start_date: Annotated[
        float | int,
        PLEXOSProperty,
        Field(
            alias="Project Start Date",
            description="Start date of generation project, for expansion planning.",
            ge=0,
        ),
    ] = 36526
    pump_bid_base: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Pump Bid Base",
            description="Base pump load for balancing bid",
        ),
    ] = 0
    pump_bid_price: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Pump Bid Price",
            description="Bid price of pump load in band",
        ),
    ] = -10000
    pump_bid_price_incr: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Pump Bid Price Incr",
            description="Adder applied to the [Pump Bid Price] property",
        ),
    ] = 0
    pump_bid_price_scalar: Annotated[
        float | int,
        Field(
            alias="Pump Bid Price Scalar",
            description="Scalar applied to the [Pump Bid Price] property",
        ),
    ] = 1
    pump_bid_quantity: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Pump Bid Quantity",
            description="Pump load bid quantity in band",
        ),
    ] = 0
    pump_bid_quantity_scalar: Annotated[
        float | int,
        Field(
            alias="Pump Bid Quantity Scalar",
            description="Scalar applied to the [Pump Bid Quantity] property",
        ),
    ] = 1
    pump_efficiency: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Pump Efficiency",
            description="Efficiency of pumping",
            ge=0,
        ),
    ] = 70
    pump_load: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Pump Load",
            description="Load drawn by a unit in pumping mode",
            ge=0,
        ),
    ] = 0
    pump_load_factor: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Pump Load Factor",
            description="A multiplier (percentage) on the pump load",
            ge=0,
        ),
    ] = 100
    pump_load_non_anticipativity: Annotated[
        float | int,
        PLEXOSProperty(units="usd"),
        Field(
            alias="Pump Load Non-anticipativity",
            description="Price for violating non-anticipativity constraints in scenario-wise decomposition mode",
        ),
    ] = 0
    pump_load_non_anticipativity_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Pump Load Non-anticipativity Time",
            description="Window of time over which to enforce non-anticipativity constraints in scenario-wise decomposition",
            ge=0,
        ),
    ] = 0
    pump_load_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Pump Load Penalty",
            description="Penalty for violation of Pump Load.",
            ge=-1,
        ),
    ] = -1
    pump_transition_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Pump Transition Time",
            description="Minimum time between operating in pump mode and generation mode",
            ge=0,
        ),
    ] = 0
    pump_unit_commitment_non_anticipativity: Annotated[
        float | int,
        PLEXOSProperty(units="usd"),
        Field(
            alias="Pump Unit Commitment Non-anticipativity",
            description="Price for violating non-anticipativity constraints in scenario-wise decomposition mode",
        ),
    ] = 0
    pump_unit_commitment_non_anticipativity_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Pump Unit Commitment Non-anticipativity Time",
            description="Window of time over which to enforce non-anticipativity constraints in scenario-wise decomposition",
            ge=0,
        ),
    ] = 0
    pump_units: Annotated[
        float | int,
        Field(
            alias="Pump Units",
            description="Number of pump units",
            ge=0,
        ),
    ] = 1e30
    pump_uos_charge: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Pump UoS Charge",
            description="Use of system charge for pump load",
        ),
    ] = 0
    ramp_down_charge: Annotated[
        int | float,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Ramp Down Charge",
            description="Charge applied to ramping down",
        ),
    ] = 0
    ramp_down_point: Annotated[
        int | float,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Ramp Down Point",
            description="Load point for use with multi-band Max Ramp Down constraints",
            ge=0,
        ),
    ] = 1e30
    ramp_up_charge: Annotated[
        int | float,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Ramp Up Charge",
            description="Charge applied to ramping up",
        ),
    ] = 0
    ramp_up_point: Annotated[
        int | float,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Ramp Up Point",
            description="Load point for use with multi-band Max Ramp Up constraints",
            ge=0,
        ),
    ] = 1e30
    random_number_seed: Annotated[
        int,
        Field(
            alias="Random Number Seed",
            description="Random number seed assigned to the generator for the generation of outages",
            ge=0,
            le=2147483647,
        ),
    ] = 0
    rating: Annotated[
        int | float,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Rating",
            description="Rated capacity of units",
        ),
    ] = 0
    rating_factor: Annotated[
        int | float,
        PLEXOSProperty(units="%"),
        Field(
            alias="Rating Factor",
            description="Maximum dispatchable capacity of each unit expressed as a percentage of [Max Capacity]",
            ge=0,
        ),
    ] = 0
    rating_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Rating Penalty",
            description="Penalty for violation of Rating",
        ),
    ] = -1
    recycle_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/GJ"),
        Field(
            alias="Recycle Penalty",
            description="Penalty for violating the recycling constraint.",
        ),
    ] = -1
    reference_generation: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Reference Generation",
            description="Generation level for generation slack PTDF calculation",
        ),
    ] = 0
    regulation_point: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Regulation Point",
            description="Start point of regulation reserve range",
            ge=0,
        ),
    ] = 0
    regulation_range: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Regulation Range",
            description="Length of regulation reserve range (must be paired with Regulation Point)",
            ge=0,
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
    reserve_share: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Reserve Share",
            description="Proportion of maximum capacity that must be set aside for reserves",
        ),
    ] = 0
    reserves_vom_charge: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Reserves VO&M Charge",
            description="Variable O&M cost associated with providing spinning reserve",
        ),
    ] = 0
    retire_non_anticipativity: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Retire Non-anticipativity",
            description="Price for violating non-anticipativity constraints in scenario-wise decomposition mode",
        ),
    ] = -1
    retirement_cost: Annotated[
        float | int,
        PLEXOSProperty(units="usd"),
        Field(
            alias="Retirement Cost",
            description="Cost of retiring a unit",
        ),
    ] = 0
    risk_level: Annotated[
        float | int,
        Field(
            alias="Risk Level",
            description="Risk level for risk-constrained optimization",
            ge=0,
            le=1,
        ),
    ] = 0.1
    rolling_planning_bonus: Annotated[
        float | int,
        PLEXOSProperty(units="usd"),
        Field(
            alias="Rolling Planning Bonus",
            description="Bonus for remaining online at the end of the look-ahead",
            ge=0,
        ),
    ] = 0
    rough_running_point: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Rough Running Point",
            description="Start point of rough running range",
            ge=0,
        ),
    ] = 0
    rough_running_range: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Rough Running Range",
            description="Length of rough running range (must be paired with Rough Running Point)",
            ge=0,
        ),
    ] = 0
    rounding_up_threshold: Annotated[
        float | int,
        Field(
            alias="Rounding Up Threshold",
            description="Threshold at which non-integers are rounded up.",
            ge=0,
            le=1,
        ),
    ] = 0
    run_down_rate: Annotated[
        float | int,
        PLEXOSProperty(units="MW/min"),
        Field(
            alias="Run Down Rate",
            description="Ramp rate that applies while running the unit down from [Min Stable Level] to zero",
            ge=0,
        ),
    ] = 1e30
    run_up_rate: Annotated[
        float | int,
        PLEXOSProperty(units="MW/min"),
        Field(
            alias="Run Up Rate",
            description="Ramp rate that applies while running the unit up from zero to [Min Stable Level].",
            ge=0,
        ),
    ] = 1e30
    running_cost: Annotated[
        float | int,
        PLEXOSProperty(units="usd/h"),
        Field(
            alias="Running Cost",
            description="Fixed cost of running a generating unit when on-line",
        ),
    ] = 0
    shutdown_cost: Annotated[
        float | int,
        PLEXOSProperty(units="usd"),
        Field(
            alias="Shutdown Cost",
            description="Cost of shutting down a unit",
            ge=0,
        ),
    ] = 0
    shutdown_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd"),
        Field(
            alias="Shutdown Penalty",
            description="Penalty applied to shutting down a unit",
        ),
    ] = 0
    shutdown_profile: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Shutdown Profile",
            description="Detailed regime for running the unit down from [Min Stable Level] when [Run Down Rate] is non-constant.",
        ),
    ] = 0
    simultaneous_increment_and_decrement: Annotated[
        int,
        Field(
            alias="Simultaneous Increment and Decrement",
            description="Degenerate increment and decrement offers and bids can be cleared simultaneously",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    simultaneous_pump_and_generation: Annotated[
        int,
        Field(
            alias="Simultaneous Pump and Generation",
            description="Pumped storage can pump and generate simultaneously",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    start_cost: Annotated[
        float | int,
        PLEXOSProperty(units="usd"),
        Field(
            alias="Start Cost",
            description="Cost of starting a unit",
            ge=0,
        ),
    ] = 0
    start_cost_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Start Cost Time",
            description="Incremental cooling time over which the corresponding Start Cost applies",
            ge=0,
        ),
    ] = 0
    start_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd"),
        Field(
            alias="Start Penalty",
            description="Penalty applied to starting a unit",
        ),
    ] = 0
    start_profile: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Start Profile",
            description="Detailed regime for running the unit up from zero to [Min Stable Level] when [Run Up Rate] is non-constant.",
        ),
    ] = 1e30
    start_profile_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Start Profile Penalty",
            description="Penalty for violation of [Start Profile].",
        ),
    ] = -1
    start_profile_range: Annotated[
        int,
        Field(
            alias="Start Profile Range",
            description="Maximum range for [Start Profile]",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    strategic_load_rating: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Strategic Load Rating",
            description="Pumping unit/Anti-generation rating for application in RSI capacity calculations.",
        ),
    ] = 0
    strategic_rating: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Strategic Rating",
            description="Generating unit rating for application in RSI capacity calculations",
        ),
    ] = 0
    strategic_reference_price: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Strategic Reference Price",
            description="Sent-out marginal generation reference price for RSI markup application.",
        ),
    ] = 0
    sync_cond_load: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Sync Cond Load",
            description="Load drawn by a unit in synchronous condenser mode",
            ge=0,
        ),
    ] = 0
    sync_cond_units: Annotated[
        float | int,
        Field(
            alias="Sync Cond Units",
            description="Maximum number of synchronous condenser units",
            ge=0,
        ),
    ] = 0
    sync_cond_vom_charge: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Sync Cond VO&M Charge",
            description="Variable O&M cost associated with running a unit in synchronous condenser mode",
        ),
    ] = 0
    technical_life: Annotated[
        float | int,
        PLEXOSProperty(units="yr"),
        Field(
            alias="Technical Life",
            description="Technical lifetime of the unit",
            ge=0,
        ),
    ] = 1e30
    tie_break_group: Annotated[
        float | int,
        Field(
            alias="Tie Break Group",
            description="TieBreak group that the generator belongs to",
            ge=0,
        ),
    ] = 0
    transition_type: Annotated[
        int,
        Field(
            alias="Transition Type",
            description="If the generator can transition between a single generator or to a group of generators.",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    turbine_blade_radius: Annotated[
        float | int,
        PLEXOSProperty(units="m"),
        Field(
            alias="Turbine Blade Radius",
            description="Turbine Blade Radius",
        ),
    ] = 0
    unit_commitment_aggregation: Annotated[
        int,
        Field(
            alias="Unit Commitment Aggregation",
            description="Flag if the generator is aggregated into an equivalent single unit for the purpose of unit commitment",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    unit_commitment_non_anticipativity: Annotated[
        float | int,
        PLEXOSProperty(units="usd"),
        Field(
            alias="Unit Commitment Non-anticipativity",
            description="Price for violating non-anticipativity constraints in scenario-wise decomposition mode",
        ),
    ] = 0
    unit_commitment_non_anticipativity_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Unit Commitment Non-anticipativity Time",
            description="Window of time over which to enforce non-anticipativity constraints in scenario-wise decomposition",
            ge=0,
        ),
    ] = 0
    unit_commitment_optimality: Annotated[
        int,
        Field(
            alias="Unit Commitment Optimality",
            description="Unit commitment integerization scheme for the generator.",
            json_schema_extra={"enum": [-1, 0, 1, 2, 3]},
        ),
    ] = -1
    unit_commitment_period: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Unit Commitment Period",
            description="Period between unit commitment (on/off) decisions.",
            ge=0,
        ),
    ] = 0
    units: Annotated[
        float | int,
        PLEXOSProperty(units=""),
        Field(
            alias="Units",
            description="Number of installed units",
            ge=0,
        ),
    ] = 0
    units_out: Annotated[
        float | int,
        PLEXOSProperty,
        Field(
            alias="Units Out",
            description="Number of units out of service",
            ge=0,
        ),
    ] = 0
    uos_charge: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="UoS Charge",
            description="Use of system charge for generation",
        ),
    ] = 0
    vom_charge: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="VO&M Charge",
            description="Variable operation and maintenance charge",
        ),
    ] = 0
    wacc: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="WACC",
            description="Weighted average cost of capital",
            ge=0,
        ),
    ] = 10

    @classmethod
    def example(cls) -> "PLEXOSGenerator":
        """Create an example generator."""
        return PLEXOSGenerator(
            name="gen-01",
            object_id=1,
            max_capacity=100,
            forced_outage_rate=0,
            maintenance_rate=0,
            mean_time_to_repair=24,
            pump_load=0,
            vom_charge=0,
        )
