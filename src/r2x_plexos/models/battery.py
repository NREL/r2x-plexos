"""The following file contains Pydantic models for a PLEXOS Battery model."""

from typing import Annotated

from pydantic import Field

from .component import PLEXOSObject
from .property_specification import PLEXOSProperty


class PLEXOSBattery(PLEXOSObject):
    """PLEXOS battery class."""

    initial_soc: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Initial SoC",
            description="Initial state of charge of the battery",
            ge=0,
        ),
    ] = 0
    charge_efficiency: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Charge Efficiency",
            description="Charge efficiency",
            ge=0,
        ),
    ] = 70
    discharge_efficiency: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Discharge Efficiency",
            description="Discharge efficiency",
            ge=0,
        ),
    ] = 100
    capacity: Annotated[
        float | int,
        PLEXOSProperty(units="MWh"),
        Field(
            alias="Capacity",
            description="Capacity of the battery",
            ge=0,
        ),
    ] = 0
    max_power: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Power",
            description="Output power",
            ge=0,
        ),
    ] = 0
    units: Annotated[
        float | int,
        PLEXOSProperty(),
        Field(
            alias="Units",
            description="Number of units of the storage",
            ge=0,
        ),
    ] = 0
    aux_base: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Aux Base",
            description="Auxiliary use per unit committed",
            ge=0,
        ),
    ] = 0
    aux_fixed: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Aux Fixed",
            description="Fixed auxiliary usage per installed unit",
            ge=0,
        ),
    ] = 0
    aux_incr: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Aux Incr",
            description="Auxiliary use per unit of generation and load",
            ge=0,
            le=100,
        ),
    ] = 0
    bid_base: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Bid Base",
            description="Base load for balancing bid",
        ),
    ] = 0
    bid_price: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Bid Price",
            description="Bid price of load in band",
        ),
    ] = -10000
    bid_price_incr: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Bid Price Incr",
            description="Adder applied to the [Bid Price] property",
        ),
    ] = 0
    bid_price_scalar: Annotated[
        float | int,
        Field(
            alias="Bid Price Scalar",
            description="Scalar applied to the [Bid Price] property",
        ),
    ] = 1
    bid_quantity: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Bid Quantity",
            description="load bid quantity in band",
        ),
    ] = 0
    bid_quantity_scalar: Annotated[
        float | int,
        Field(
            alias="Bid Quantity Scalar",
            description="Scalar applied to the [Bid Quantity] property",
        ),
    ] = 1
    bid_cost_mark_up: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Bid-Cost Mark-up",
            description="Percentage mark-up applied to generator offer prices = (P - C) / C",
        ),
    ] = 0
    build_cost: Annotated[
        float | int,
        PLEXOSProperty(units="usd/kW"),
        Field(
            alias="Build Cost",
            description="Cost of building a BESS unit",
        ),
    ] = 0
    build_cost_multiplier: Annotated[
        int,
        Field(
            alias="Build Cost Multiplier",
            description="Sets the unit for the input Build Cost",
            json_schema_extra={"enum": [0, 1, 2]},
        ),
    ] = 1
    build_non_anticipativity: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Build Non-anticipativity",
            description="Price for violating non-anticipativity constraints in scenario-wise decomposition mode",
        ),
    ] = -1
    capacity_degradation: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Capacity Degradation",
            description="Degradation in capacity with age in number of cycles",
        ),
    ] = 0
    capacity_price: Annotated[
        float | int,
        PLEXOSProperty(units="usd/kW/yr"),
        Field(
            alias="Capacity Price",
            description="Price received by the battery for capacity",
        ),
    ] = 0
    charge_transition_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Charge Transition Time",
            description="Minimum time between operating in charge mode and discharge mode",
            ge=0,
        ),
    ] = 0
    charging_vom_charge: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Charging VO&M Charge",
            description="Variable operation and maintenance charge for charging",
        ),
    ] = 0
    declining_depreciation_balance: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Declining Depreciation Balance",
            description="Balance applied to declining depreciation method",
        ),
    ] = 0
    decomposition_bound_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd"),
        Field(
            alias="Decomposition Bound Penalty",
            description="Penalty applied to violation of state-of-charge bounds when the decomposition implies possible violations",
            ge=0,
        ),
    ] = 1000000
    decomposition_method: Annotated[
        int,
        PLEXOSProperty(is_enum=True),
        Field(
            alias="Decomposition Method",
            description="Method used to pass the optimal state-of-charge from one simulation phase to the next",
            json_schema_extra={"enum": [0, 1, 2]},
        ),
    ] = 1
    decomposition_penalty_a: Annotated[
        float | int,
        Field(
            alias="Decomposition Penalty a",
            description="Decomposition state-of-charge target penalty function 'a' term",
        ),
    ] = 0.0489
    decomposition_penalty_b: Annotated[
        float | int,
        Field(
            alias="Decomposition Penalty b",
            description="Decomposition state-of-charge target penalty function 'b' term",
        ),
    ] = 0.6931
    decomposition_penalty_c: Annotated[
        float | int,
        Field(
            alias="Decomposition Penalty c",
            description="Decomposition state-of-charge target penalty function 'c' term",
        ),
    ] = 0
    decomposition_penalty_x: Annotated[
        float | int,
        Field(
            alias="Decomposition Penalty x",
            description="Decomposition state-of-charge target penalty function 'x' term",
        ),
    ] = 1.1
    discharge_transition_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Discharge Transition Time",
            description="Minimum time between operating in discharge mode and charge mode",
            ge=0,
        ),
    ] = 0
    discharging_vom_charge: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Discharging VO&M Charge",
            description="Variable operation and maintenance charge for discharging",
        ),
    ] = 0
    duration: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Duration",
            description="Battery duration used to determine the MWh capacity",
            ge=0,
        ),
    ] = 0
    economic_life: Annotated[
        float | int,
        PLEXOSProperty(units="yr"),
        Field(
            alias="Economic Life",
            description="Economic life of a BESS unit (period over which fixed costs are recovered)",
            ge=0,
        ),
    ] = 30
    effective_forced_outage_rate: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Effective Forced Outage Rate",
            description="Effective forced outage rate for use in calculation of reliability indices",
            ge=0,
            le=100,
        ),
    ] = 0
    end_effects_method: Annotated[
        int,
        PLEXOSProperty(is_enum=True),
        Field(
            alias="End Effects Method",
            description="Method used to value or constrain end-of-period energy.",
            json_schema_extra={"enum": [1, 2]},
        ),
    ] = 2
    energy_target: Annotated[
        float | int,
        PLEXOSProperty(units="MWh"),
        Field(
            alias="Energy Target",
            description="Battery stored energy target",
            ge=0,
        ),
    ] = 0
    energy_target_day: Annotated[
        float | int,
        PLEXOSProperty(units="MWh"),
        Field(
            alias="Energy Target Day",
            description="end of day battery stored energy target",
            ge=0,
        ),
    ] = 0
    energy_target_hour: Annotated[
        float | int,
        PLEXOSProperty(units="MWh"),
        Field(
            alias="Energy Target Hour",
            description="end of hour battery stored energy target",
            ge=0,
        ),
    ] = 0
    energy_target_month: Annotated[
        float | int,
        PLEXOSProperty(units="MWh"),
        Field(
            alias="Energy Target Month",
            description="end of month battery stored energy target",
            ge=0,
        ),
    ] = 0
    energy_target_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Energy Target Penalty",
            description="Penalty for violating the battery stored energy target.",
        ),
    ] = -1
    energy_target_week: Annotated[
        float | int,
        PLEXOSProperty(units="MWh"),
        Field(
            alias="Energy Target Week",
            description="end of week battery stored energy target",
            ge=0,
        ),
    ] = 0
    energy_target_year: Annotated[
        float | int,
        PLEXOSProperty(units="MWh"),
        Field(
            alias="Energy Target Year",
            description="end of year battery stored energy target",
            ge=0,
        ),
    ] = 0
    expansion_economy_cost: Annotated[
        float | int,
        PLEXOSProperty(units="usd/kW"),
        Field(
            alias="Expansion Economy Cost",
            description="Cost of building a unit at economy of scale (band)",
        ),
    ] = 0
    expansion_economy_units: Annotated[
        float | int,
        Field(
            alias="Expansion Economy PLEXOSProperty",
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
            description="Contribution of the battery to capacity reserves",
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
    fom_charge: Annotated[
        float | int,
        PLEXOSProperty(units="usd/kW/yr"),
        Field(
            alias="FO&M Charge",
            description="Fixed operations and maintenance charge",
        ),
    ] = 0
    forced_outage_rate: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Forced Outage Rate",
            description="Expected proportion of time the facility is unavailable due to forced outage",
            ge=0,
            le=100,
        ),
    ] = 0
    hint_units_built: Annotated[
        float | int,
        Field(
            alias="Hint PLEXOSProperty Built",
            description="Capacity expansion solution to be passed to the optimizer as a hint or initial solution",
            ge=0,
        ),
    ] = 0
    hint_units_retired: Annotated[
        float | int,
        Field(
            alias="Hint PLEXOSProperty Retired",
            description="Capacity expansion solution to be passed to the optimizer as a hint or initial solution",
            ge=0,
        ),
    ] = 0
    inertia_constant: Annotated[
        float | int,
        PLEXOSProperty(units="s"),
        Field(
            alias="Inertia Constant",
            description="Stored energy per unit of power",
            ge=0,
        ),
    ] = 0
    initial_age: Annotated[
        float | int,
        PLEXOSProperty(units="cycles"),
        Field(
            alias="Initial Age",
            description="Age of the battery in number of cycles at the start of the simulation horizon",
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
    is_strategic: Annotated[
        int,
        Field(
            alias="Is Strategic",
            description="If the battery acts strategically",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    lccr_apply_discounting: Annotated[
        int,
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
            description="Battery marginal loss factor (MLF or TLF)",
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
    max_build_events: Annotated[
        float | int,
        Field(
            alias="Max Build Events",
            description="Maximum number of distinct build events allowed over the planning horizon",
            ge=0,
        ),
    ] = 1e30
    max_charge_up_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Max Charge Up Time",
            description="Maximum number of hours a unit can be run in charge mode after being started",
            ge=0,
        ),
    ] = 0
    max_cycles: Annotated[
        float | int,
        PLEXOSProperty(units="cycles"),
        Field(
            alias="Max Cycles",
            description="Number of cycles allowed each interval",
        ),
    ] = 1e30
    max_cycles_day: Annotated[
        float | int,
        PLEXOSProperty(units="cycles"),
        Field(
            alias="Max Cycles Day",
            description="Number of cycles allowed each day",
        ),
    ] = 1e30
    max_cycles_hour: Annotated[
        float | int,
        PLEXOSProperty(units="cycles"),
        Field(
            alias="Max Cycles Hour",
            description="Number of cycles allowed each hour",
        ),
    ] = 1e30
    max_cycles_month: Annotated[
        float | int,
        PLEXOSProperty(units="cycles"),
        Field(
            alias="Max Cycles Month",
            description="Number of cycles allowed each month",
        ),
    ] = 1e30
    max_cycles_week: Annotated[
        float | int,
        PLEXOSProperty(units="cycles"),
        Field(
            alias="Max Cycles Week",
            description="Number of cycles allowed each week",
        ),
    ] = 1e30
    max_cycles_year: Annotated[
        float | int,
        PLEXOSProperty(units="cycles"),
        Field(
            alias="Max Cycles Year",
            description="Number of cycles allowed each year",
        ),
    ] = 1e30
    max_down_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Max Down Time",
            description="Maximum number of hours a battery can have zero discharge after discharging stops",
            ge=0,
        ),
    ] = 0
    max_energy_charging: Annotated[
        float | int,
        PLEXOSProperty(units="MWh"),
        Field(
            alias="Max Energy Charging",
            description="Max Energy Discharging",
            ge=0,
        ),
    ] = 1e30
    max_energy_charging_day: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Energy Charging Day",
            description="Max Energy Discharging Day",
            ge=0,
        ),
    ] = 1e30
    max_energy_charging_hour: Annotated[
        float | int,
        PLEXOSProperty(units="MWh"),
        Field(
            alias="Max Energy Charging Hour",
            description="Max Energy Discharging Hour",
            ge=0,
        ),
    ] = 1e30
    max_energy_charging_month: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Energy Charging Month",
            description="Max Energy Discharging Month",
            ge=0,
        ),
    ] = 1e30
    max_energy_charging_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/GWh"),
        Field(
            alias="Max Energy Charging Penalty",
            description="Max Energy Discharging Penalty",
        ),
    ] = -1
    max_energy_charging_week: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Energy Charging Week",
            description="Max Energy Discharging Week",
            ge=0,
        ),
    ] = 1e30
    max_energy_charging_year: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Energy Charging Year",
            description="Max Energy Discharging Year",
            ge=0,
        ),
    ] = 1e30
    max_energy_discharging: Annotated[
        float | int,
        PLEXOSProperty(units="MWh"),
        Field(
            alias="Max Energy Discharging",
            description="Max Energy Discharging",
            ge=0,
        ),
    ] = 1e30
    max_energy_discharging_day: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Energy Discharging Day",
            description="Max Energy Discharging Day",
            ge=0,
        ),
    ] = 1e30
    max_energy_discharging_hour: Annotated[
        float | int,
        PLEXOSProperty(units="MWh"),
        Field(
            alias="Max Energy Discharging Hour",
            description="Max Energy Discharging Hour",
            ge=0,
        ),
    ] = 1e30
    max_energy_discharging_month: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Energy Discharging Month",
            description="Max Energy Discharging Month",
            ge=0,
        ),
    ] = 1e30
    max_energy_discharging_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/GWh"),
        Field(
            alias="Max Energy Discharging Penalty",
            description="Max Energy Discharging Penalty",
        ),
    ] = -1
    max_energy_discharging_week: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Energy Discharging Week",
            description="Max Energy Discharging Week",
            ge=0,
        ),
    ] = 1e30
    max_energy_discharging_year: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Energy Discharging Year",
            description="Max Energy Discharging Year",
            ge=0,
        ),
    ] = 1e30
    max_load: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Load",
            description="Power at full charge including inverter losses",
        ),
    ] = 0
    max_ramp_down: Annotated[
        float | int,
        PLEXOSProperty(units="MW/min"),
        Field(
            alias="Max Ramp Down",
            description="Maximum ramp down rate",
            ge=0,
        ),
    ] = 1e30
    max_ramp_down_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Max Ramp Down Penalty",
            description="Penalty for violating Max Ramp Down constraint.",
        ),
    ] = -1
    max_ramp_up: Annotated[
        float | int,
        PLEXOSProperty(units="MW/min"),
        Field(
            alias="Max Ramp Up",
            description="Maximum ramp up rate",
            ge=0,
        ),
    ] = 1e30
    max_ramp_up_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Max Ramp Up Penalty",
            description="Penalty for violating [Max Ramp Up] constraint.",
        ),
    ] = -1
    max_soc: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max SoC",
            description="Allowable maximum State of Charge",
            ge=0,
        ),
    ] = 100
    max_time_to_repair: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Max Time To Repair",
            description="Maximum time to repair",
            ge=0,
        ),
    ] = 0
    max_units_built: Annotated[
        float | int,
        PLEXOSProperty,
        Field(
            alias="Max PLEXOSProperty Built",
            description="Maximum number of BESS units that can be built",
            ge=0,
        ),
    ] = 0
    max_units_built_in_year: Annotated[
        float | int,
        Field(
            alias="Max PLEXOSProperty Built in Year",
            description="Maximum number of BESS units that can be built in a year",
            ge=0,
        ),
    ] = 1e30
    max_units_retired: Annotated[
        float | int,
        Field(
            alias="Max PLEXOSProperty Retired",
            description="Maximum number of units allowed to be retired in aggregate over the planning horizon",
            ge=0,
        ),
    ] = 0
    max_units_retired_in_year: Annotated[
        float | int,
        Field(
            alias="Max PLEXOSProperty Retired in Year",
            description="Maximum number of units allowed to be retired in any single year of the planning horizon",
            ge=0,
        ),
    ] = 1e30
    max_up_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Max Up Time",
            description="Maximum number of hours a battery can discharge after discharging starts",
            ge=0,
        ),
    ] = 0
    mean_time_to_repair: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Mean Time to Repair",
            description="Mean time to repair",
            ge=0,
        ),
    ] = 24
    min_charge_down_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Min Charge Down Time",
            description="Minimum number of hours a unit must refrain from charging after being shut down from charge mode",
            ge=0,
        ),
    ] = 0
    min_charge_level: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Min Charge Level",
            description="Minimum unit charge level when charging",
            ge=0,
        ),
    ] = 0
    min_charge_up_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Min Charge Up Time",
            description="Minimum number of hours a unit must be run in charge mode after being started",
            ge=0,
        ),
    ] = 0
    min_discharge_level: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Min Discharge Level",
            description="Minimum discharge level when discharging",
            ge=0,
        ),
    ] = 0
    min_down_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Min Down Time",
            description="Minimum number of hours a battery must have zero discharge after discharging stops",
            ge=0,
        ),
    ] = 0
    min_energy_charging: Annotated[
        float | int,
        PLEXOSProperty(units="MWh"),
        Field(
            alias="Min Energy Charging",
            description="Min Energy Discharging",
            ge=0,
        ),
    ] = 0
    min_energy_charging_day: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Min Energy Charging Day",
            description="Min Energy Discharging Day",
            ge=0,
        ),
    ] = 0
    min_energy_charging_hour: Annotated[
        float | int,
        PLEXOSProperty(units="MWh"),
        Field(
            alias="Min Energy Charging Hour",
            description="Min Energy Discharging Hour",
            ge=0,
        ),
    ] = 0
    min_energy_charging_month: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Min Energy Charging Month",
            description="Min Energy Discharging Month",
            ge=0,
        ),
    ] = 0
    min_energy_charging_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/GWh"),
        Field(
            alias="Min Energy Charging Penalty",
            description="Min Energy Discharging Penalty",
        ),
    ] = 10000000
    min_energy_charging_week: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Min Energy Charging Week",
            description="Min Energy Discharging Week",
            ge=0,
        ),
    ] = 0
    min_energy_charging_year: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Min Energy Charging Year",
            description="Min Energy Discharging Year",
            ge=0,
        ),
    ] = 0
    min_energy_discharging: Annotated[
        float | int,
        PLEXOSProperty(units="MWh"),
        Field(
            alias="Min Energy Discharging",
            description="Min Energy Discharging",
            ge=0,
        ),
    ] = 0
    min_energy_discharging_day: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Min Energy Discharging Day",
            description="Min Energy Discharging Day",
            ge=0,
        ),
    ] = 0
    min_energy_discharging_hour: Annotated[
        float | int,
        PLEXOSProperty(units="MWh"),
        Field(
            alias="Min Energy Discharging Hour",
            description="Min Energy Discharging Hour",
            ge=0,
        ),
    ] = 0
    min_energy_discharging_month: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Min Energy Discharging Month",
            description="Min Energy Discharging Month",
            ge=0,
        ),
    ] = 0
    min_energy_discharging_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/GWh"),
        Field(
            alias="Min Energy Discharging Penalty",
            description="Min Energy Discharging Penalty",
        ),
    ] = 10000000
    min_energy_discharging_week: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Min Energy Discharging Week",
            description="Min Energy Discharging Week",
            ge=0,
        ),
    ] = 0
    min_energy_discharging_year: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Min Energy Discharging Year",
            description="Min Energy Discharging Year",
            ge=0,
        ),
    ] = 0
    min_soc: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Min SoC",
            description="Allowable minimum state of charge",
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
    min_units_built: Annotated[
        float | int,
        Field(
            alias="Min PLEXOSProperty Built",
            description="Minimum number of units automatically constructed in aggregate over the planning horizon",
            ge=0,
        ),
    ] = 0
    min_units_built_in_year: Annotated[
        float | int,
        Field(
            alias="Min PLEXOSProperty Built in Year",
            description="Minimum number of units allowed to be constructed in any single year of the planning horizon",
            ge=0,
        ),
    ] = 0
    min_units_retired: Annotated[
        float | int,
        Field(
            alias="Min PLEXOSProperty Retired",
            description="Minimum number of units automatically retired in aggregate over the planning horizon",
            ge=0,
        ),
    ] = 0
    min_units_retired_in_year: Annotated[
        float | int,
        Field(
            alias="Min PLEXOSProperty Retired in Year",
            description="Minimum number of units allowed to be retired in any single year of the planning horizon",
            ge=0,
        ),
    ] = 0
    min_up_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Min Up Time",
            description="Minimum number of hours a battery must discharge after discharging starts",
            ge=0,
        ),
    ] = 0
    model_capital_cost_recovery: Annotated[
        int,
        Field(
            alias="Model Capital Cost Recovery",
            description="Indicates if the capital cost recovery feature is modeled for expansion planning",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    non_anticipativity: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Non-anticipativity",
            description="Price for violating non-anticipativity constraints in scenario-wise decomposition mode",
        ),
    ] = -1
    non_anticipativity_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Non-anticipativity Time",
            description="Window of time over which to enforce non-anticipativity constraints in scenario-wise decomposition",
            ge=0,
        ),
    ] = 0
    non_physical_charge_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Non-physical Charge Penalty",
            description="Penalty applied to non-physical charging of the battery. A value of -1 means none is allowed.",
        ),
    ] = -1
    non_physical_discharge_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Non-physical Discharge Penalty",
            description="Penalty applied to non-physical discharging of the battery. A value of -1 means none is allowed.",
        ),
    ] = -1
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
    offer_quantity_scalar: Annotated[
        float | int,
        Field(
            alias="Offer Quantity Scalar",
            description="Scalar applied to the [Offer Quantity] property",
        ),
    ] = 1
    outage_factor: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Outage Factor",
            description="Battery outage rating based on max power",
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
            description="Degradation of battery power with cycles",
        ),
    ] = 0
    price_setting: Annotated[
        int,
        Field(
            alias="Price Setting",
            description="Flag if the battery can set price",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    project_start_date: Annotated[
        float | int,
        PLEXOSProperty,
        Field(
            alias="Project Start Date",
            description="First date at which a BESS unit can be built",
            ge=0,
        ),
    ] = 36526
    random_number_seed: Annotated[
        float | int,
        Field(
            alias="Random Number Seed",
            description="Random number seed assigned to the generator for the generation of outages",
            ge=0,
            le=2147483647,
        ),
    ] = 0
    recharge_timeframe: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Recharge Timeframe",
            description="Maximum hours to recharge after discharge",
            ge=0,
        ),
    ] = 1e30
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
            description="Cost of retiring a BESS unit",
        ),
    ] = 0
    self_discharge_rate: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Self Discharge Rate",
            description="Percentage of stored energy lost per hour due to self-discharge.",
        ),
    ] = 0
    simultaneous_charge_and_discharge: Annotated[
        int,
        Field(
            alias="Simultaneous Charge and Discharge",
            description="Battery can charge and discharge simultaneously",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    simultaneous_increment_and_decrement: Annotated[
        int,
        Field(
            alias="Simultaneous Increment and Decrement",
            description="Degenerate increment and decrement offers and bids can be cleared simultaneously",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    strategic_load_rating: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Strategic Load Rating",
            description="Pumping unit rating for application in RSI capacity calculations.",
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
    technical_life: Annotated[
        float | int,
        PLEXOSProperty(units="yr"),
        Field(
            alias="Technical Life",
            description="Technical lifetime of a BESS unit",
            ge=0,
        ),
    ] = 1e30
    unit_commitment_optimality: Annotated[
        int,
        Field(
            alias="Unit Commitment Optimality",
            description="Unit commitment integerization scheme for the battery.",
            json_schema_extra={"enum": [-1, 0, 1, 2, 3]},
        ),
    ] = -1
    units_out: Annotated[
        float | int,
        Field(
            alias="PLEXOSProperty Out",
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
    def example(cls) -> "PLEXOSBattery":
        """Create an example PLEXOSBattery."""
        return PLEXOSBattery(
            name="ExampleBattery",
            initial_soc=100,
            charge_efficiency=95,
            discharge_efficiency=95,
            capacity=4,  # 4 hour duration
            max_power=1,  # 1MW Battery
            units=1,
        )
