"""The following file contains Pydantic models for a PLEXOS Line model."""

from typing import Annotated

from pydantic import Field

from .component import PLEXOSObject
from .property_specification import PLEXOSProperty


class PLEXOSLine(PLEXOSObject):
    """Class that holds attributes about PLEXOS Line objects."""

    ac_line_charging_susceptance: Annotated[
        float | int,
        PLEXOSProperty(units="pu"),
        Field(
            alias="AC Line Charging Susceptance",
            description="The line-charging susceptance of a transmission line",
        ),
    ] = 0
    build_cost: Annotated[
        float | int,
        PLEXOSProperty(units="usd"),
        Field(
            alias="Build Cost",
            description="Cost of building the line",
        ),
    ] = 0
    build_non_anticipativity: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Build Non-anticipativity",
            description="Price for violating non-anticipativity constraints in scenario-wise decomposition mode",
        ),
    ] = -1
    circuits: Annotated[
        float | int,
        Field(
            alias="Circuits",
            description="Number of circuits in the notional interconnector for the purposes of outage modelling",
            ge=1,
        ),
    ] = 1
    commission_date: Annotated[
        float | int,
        Field(
            alias="Commission Date",
            description="Date the line was commissioned for use with [Technical Life]",
            ge=0,
        ),
    ] = 1
    contingency_limit_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Contingency Limit Penalty",
            description="Penalty for exceeding contingency flow limits",
        ),
    ] = -1
    debt_charge: Annotated[
        float | int,
        PLEXOSProperty(units="usd"),
        Field(
            alias="Debt Charge",
            description="Annual debt charge",
        ),
    ] = 0
    economic_life: Annotated[
        float | int,
        PLEXOSProperty(units="yr"),
        Field(
            alias="Economic Life",
            description="Economic life of the line (period over which fixed costs are recovered).",
            ge=0,
        ),
    ] = 30
    enforce_limits: Annotated[
        int,
        Field(
            alias="Enforce Limits",
            description="Controls when flow limits are enforced with regard to Transmission [Constraint Voltage Threshold].",
            json_schema_extra={"enum": [0, 1, 2, 3]},
        ),
    ] = 1
    equity_charge: Annotated[
        float | int,
        PLEXOSProperty(units="usd"),
        Field(
            alias="Equity Charge",
            description="Annual required return on equity",
        ),
    ] = 0
    expansion_optimality: Annotated[
        int,
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
            description="Net capacity reserves exported",
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
    fixed_flow: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Fixed Flow",
            description="Fixed flow on line",
        ),
    ] = 0
    fixed_flow_method: Annotated[
        int,
        Field(
            alias="Fixed Flow Method",
            description="Method of interpreting zero values of the [Fixed Flow] property.",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 1
    fixed_flow_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Fixed Flow Penalty",
            description="Penalty for violation of [Fixed Flow].",
        ),
    ] = -1
    fixed_loss: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Fixed Loss",
            description="Fixed loss on line",
        ),
    ] = 0
    flow_non_anticipativity: Annotated[
        float | int,
        PLEXOSProperty(units="usd"),
        Field(
            alias="Flow Non-anticipativity",
            description="Price for violating non-anticipativity constraints in scenario-wise decomposition mode",
        ),
    ] = 0
    flow_non_anticipativity_time: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Flow Non-anticipativity Time",
            description="Window of time over which to enforce non-anticipativity constraints in scenario-wise decomposition",
            ge=0,
        ),
    ] = 0
    fom_charge: Annotated[
        float | int,
        PLEXOSProperty(units="usd"),
        Field(
            alias="FO&M Charge",
            description="Annual fixed operation and maintenance charge",
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
    integerization_horizon: Annotated[
        float | int,
        PLEXOSProperty(units="yr"),
        Field(
            alias="Integerization Horizon",
            description="Number of years over which the expansion decisions are integerized",
            ge=-1,
        ),
    ] = -1
    lead_time: Annotated[
        float | int,
        PLEXOSProperty(units="yr"),
        Field(
            alias="Lead Time",
            description="Number of years after which the expansion project can begin",
            ge=0,
        ),
    ] = 0
    limit_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Limit Penalty",
            description="Penalty for exceeding the flow limits on the line",
        ),
    ] = -1
    loss_allocation: Annotated[
        float | int,
        Field(
            alias="Loss Allocation",
            description="Proportion of line losses allocated to the receiving node",
            ge=0,
            le=1,
        ),
    ] = 0.5
    loss_base: Annotated[
        float | int,
        Field(
            alias="Loss Base",
            description="Interconnector loss function constant parameter for reference direction flows",
        ),
    ] = 0
    loss_base_back: Annotated[
        float | int,
        Field(
            alias="Loss Base Back",
            description="Interconnector loss function constant parameter for counter-reference direction flows",
        ),
    ] = 0
    loss_incr: Annotated[
        float | int,
        Field(
            alias="Loss Incr",
            description="Interconnector loss function linear parameter for reference direction flows",
        ),
    ] = 0
    loss_incr_back: Annotated[
        float | int,
        Field(
            alias="Loss Incr Back",
            description="Interconnector loss function linear parameter for counter-reference direction flows",
        ),
    ] = 0
    loss_incr2: Annotated[
        float | int,
        Field(
            alias="Loss Incr2",
            description="Interconnector loss function quadratic parameter for reference direction flows",
        ),
    ] = 0
    loss_incr2_back: Annotated[
        float | int,
        Field(
            alias="Loss Incr2 Back",
            description="Interconnector loss function quadratic parameter for counter-reference direction flows",
        ),
    ] = 0
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
            description="Transmission marginal loss factor (MLF or TLF) for exports",
        ),
    ] = 1
    marginal_loss_factor_back: Annotated[
        float | int,
        Field(
            alias="Marginal Loss Factor Back",
            description="Transmission marginal loss factor (MLF or TLF) for imports",
        ),
    ] = 1
    max_capacity_reserves: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Capacity Reserves",
            description="Maximum amount of capacity reserves supplied to the receiving Region/Zone.",
        ),
    ] = 1e30
    max_flow: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Flow",
            description="Maximum flow",
        ),
    ] = 1e30
    max_loss_tranches: Annotated[
        float | int,
        Field(
            alias="Max Loss Tranches",
            description="Maximum number of tranches in piecewise linear loss function.",
            ge=2,
        ),
    ] = 2
    max_ramp_down: Annotated[
        float | int,
        PLEXOSProperty(units="MW/min"),
        Field(
            alias="Max Ramp Down",
            description="Maximum ramp down rate",
            ge=0,
        ),
    ] = 1e30
    max_ramp_up: Annotated[
        float | int,
        PLEXOSProperty(units="MW/min"),
        Field(
            alias="Max Ramp Up",
            description="Maximum ramp up rate",
            ge=0,
        ),
    ] = 1e30
    max_rating: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Rating",
            description="Rated maximum (overrides Max Flow)",
        ),
    ] = 1e30
    max_time_to_repair: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Max Time To Repair",
            description="Maximum time to repair (hr)",
            ge=0,
        ),
    ] = 0
    max_units_built: Annotated[
        int,
        PLEXOSProperty(is_enum=True),
        Field(
            alias="Max Units Built",
            description="Maximum number of units automatically constructed in aggregate over the planning horizon",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    max_units_built_in_year: Annotated[
        int,
        Field(
            alias="Max Units Built in Year",
            description="Maximum number of units automatically constructed in any single year of the planning horizon",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 1
    max_units_retired: Annotated[
        int,
        Field(
            alias="Max Units Retired",
            description="Maximum number of units automatically retired in aggregate over the planning horizon",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    max_units_retired_in_year: Annotated[
        int,
        Field(
            alias="Max Units Retired in Year",
            description="Maximum number of units automatically retired in any single year of the planning horizon",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 1
    mean_time_to_repair: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Mean Time to Repair",
            description="Mean time to repair",
            ge=0,
        ),
    ] = 24
    min_capacity_reserves: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Min Capacity Reserves",
            description="Minimum amount of capacity reserves supplied to the receiving Region/Zone.",
        ),
    ] = -1e30
    min_flow: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Min Flow",
            description="Minimum flow",
        ),
    ] = -1e30
    min_rating: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Min Rating",
            description="Rated minimum (overrides Min Flow)",
        ),
    ] = -1e30
    min_time_to_repair: Annotated[
        float | int,
        PLEXOSProperty(units="h"),
        Field(
            alias="Min Time To Repair",
            description="Minimum time to repair (hr)",
            ge=0,
        ),
    ] = 0
    min_units_built: Annotated[
        int,
        Field(
            alias="Min Units Built",
            description="Minimum number of lines automatically constructed",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    min_units_built_in_year: Annotated[
        int,
        Field(
            alias="Min Units Built in Year",
            description="Minimum number of units automatically constructed in any single year of the planning horizon",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    min_units_retired: Annotated[
        int,
        Field(
            alias="Min Units Retired",
            description="Minimum number of lines automatically retired",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    min_units_retired_in_year: Annotated[
        int,
        Field(
            alias="Min Units Retired in Year",
            description="Minimum number of units automatically retired in any single year of the planning horizon",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    must_report: Annotated[
        int,
        Field(
            alias="Must Report",
            description="If the Line must be reported regardless of Transmission [Report Voltage Threshold].",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    offer_base: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Offer Base",
            description="Base dispatch point for balancing offer",
        ),
    ] = 0
    offer_price: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Offer Price",
            description="Price offered in band for reference direction flows",
        ),
    ] = 10000
    offer_price_back: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Offer Price Back",
            description="Price offered in band for counter-reference direction flows",
        ),
    ] = 10000
    offer_quantity: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Offer Quantity",
            description="Quantity offered in band for reference direction flows",
        ),
    ] = 0
    offer_quantity_back: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Offer Quantity Back",
            description="Quantity offered in band for counter-reference direction flows",
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
    outage_max_rating: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Outage Max Rating",
            description="Line rating in the reference direction during outage",
        ),
    ] = 0
    outage_min_rating: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Outage Min Rating",
            description="Line rating in the counter-reference direction during outage",
        ),
    ] = 0
    overload_max_rating: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Overload Max Rating",
            description="Emergency line rating in the reference direction",
        ),
    ] = 0
    overload_min_rating: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Overload Min Rating",
            description="Emergency line rating in the counter-reference direction",
        ),
    ] = 0
    price_setting: Annotated[
        int,
        Field(
            alias="Price Setting",
            description="Flag if the Line can transfer price across the network",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    project_start_date: Annotated[
        float | int,
        Field(
            alias="Project Start Date",
            description="Start date of transmission project, for expansion planning.",
            ge=0,
        ),
    ] = 36526
    ramp_down_point: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Ramp Down Point",
            description="Flow for use with multi-band Max Ramp Down constraints",
        ),
    ] = 1e30
    ramp_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Ramp Penalty",
            description="Penalty for changes in flow on the line",
        ),
    ] = 0
    ramp_up_point: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Ramp Up Point",
            description="Flow for use with multi-band Max Ramp Up constraints",
        ),
    ] = 1e30
    random_number_seed: Annotated[
        float | int,
        Field(
            alias="Random Number Seed",
            description="Random number seed assigned to the Line for the generation of outages",
            ge=0,
            le=2147483647,
        ),
    ] = 0
    reactance: Annotated[
        float | int,
        PLEXOSProperty(units="pu"),
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
        PLEXOSProperty(units="pu"),
        Field(
            alias="Resistance",
            description="A measure of the line's opposition to the flow of electric charge",
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
            description="Cost of retiring the line",
        ),
    ] = 0
    screening_mode: Annotated[
        int,
        Field(
            alias="Screening Mode",
            description="The set of lines that should be screened for post-contingency flow under screen contingencies",
            json_schema_extra={"enum": [0, 1, 2]},
        ),
    ] = 1
    susceptance: Annotated[
        float | int,
        PLEXOSProperty(units="pu"),
        Field(
            alias="Susceptance",
            description="The reciprocal of the reactance of a circuit and thus the imaginary part of its admittance",
        ),
    ] = 0
    technical_life: Annotated[
        float | int,
        PLEXOSProperty(units="yr"),
        Field(
            alias="Technical Life",
            description="Technical lifetime of the line",
            ge=0,
        ),
    ] = 1e30
    type: Annotated[
        int,
        Field(
            alias="Type",
            description="Line expansion type",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    units: Annotated[
        int,
        PLEXOSProperty(is_enum=True),
        Field(
            alias="Units",
            description="Flag if the line is in service (0,1)",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 1
    units_out: Annotated[
        float | int,
        Field(
            alias="Units Out",
            description="Number of units (circuits) out of service",
            ge=0,
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
    wheeling_charge: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Wheeling Charge",
            description="Wheeling charge for reference direction flows",
        ),
    ] = 0
    wheeling_charge_back: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Wheeling Charge Back",
            description="Wheeling charge for counter-reference direction flows",
        ),
    ] = 0
    flow_coefficient: Annotated[
        float | int,
        PLEXOSProperty,
        Field(
            alias="Flow Coefficient",
            description="Flow coefficient",
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
    voltage: Annotated[
        float | int,
        PLEXOSProperty(units="kV"),
        Field(
            alias="Voltage",
            description="Voltage of the line",
        ),
    ] = 0

    @classmethod
    def example(cls) -> "PLEXOSLine":
        """Create an example PLEXOSLine."""
        return PLEXOSLine(
            name="Example Line",
            max_flow=100,
            resistance=0.1,
            reactance=0.1,
        )
