"""The following file contains Pydantic models for a PLEXOS Region model."""

from typing import Annotated

from pydantic import Field

from .component import PLEXOSTopology
from .property_specification import PLEXOSProperty


class PLEXOSRegion(PLEXOSTopology):
    """Class that holds attributes about PLEXOS Regions."""

    aggregate_transmission: Annotated[
        int,
        Field(
            alias="Aggregate Transmission",
            description="If transmission should be aggregated to the region level",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    capacity_excess_price: Annotated[
        float | int,
        PLEXOSProperty(units="usd/kW/yr"),
        Field(
            alias="Capacity Excess Price",
            description="Penalty for an excess of capacity reserves",
            ge=0,
        ),
    ] = 0
    capacity_expansion_group: Annotated[
        float | int,
        Field(
            alias="Capacity Expansion Group",
            description="The capacity expansion group that the region belongs to for LT decomposition.",
            ge=0,
        ),
    ] = 0
    capacity_price_cap: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Capacity Price Cap",
            description="Cap on the capacity price",
        ),
    ] = 1e30
    capacity_price_floor: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Capacity Price Floor",
            description="Floor on the capacity price",
        ),
    ] = -1e30
    capacity_shortage_price: Annotated[
        float | int,
        PLEXOSProperty(units="usd/kW/yr"),
        Field(
            alias="Capacity Shortage Price",
            description="Penalty for a shortage of capacity reserves",
            ge=0,
        ),
    ] = 0
    constraint_payments_compatibility: Annotated[
        int,
        Field(
            alias="Constraint Payments Compatibility",
            description="Constraint payments compatibility (match to market being modeled)",
            json_schema_extra={"enum": [1, 2]},
        ),
    ] = 1
    constraint_payments_enabled: Annotated[
        int,
        Field(
            alias="Constraint Payments Enabled",
            description="Constraint payments compatibility (match to market being modelled)",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    decomposition_group: Annotated[
        int,
        Field(
            alias="Decomposition Group",
            description="The decomposition group that the region belongs to.",
            ge=0,
        ),
    ] = 0
    dsp_bid_price: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="DSP Bid Price",
            description="Bid price for demand-side participation",
        ),
    ] = 10000
    dsp_bid_quantity: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="DSP Bid Quantity",
            description="Bid quantity for demand-side participation",
        ),
    ] = 0
    elasticity: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh**2"),
        Field(
            alias="Elasticity",
            description="Price elasticity of demand",
            lt=0,
        ),
    ] = -0.2
    enforce_transmission_limits_on_lines_in_interfaces: Annotated[
        float | int,
        Field(
            alias="Enforce Transmission Limits On Lines In Interfaces",
            description="If lines in interfaces should have their limits enforced regardless of voltage in this region.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    firm_capacity_incr: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Firm Capacity Incr",
            description="Firm Capacity not explicitly modeled that should be included in reserve margin calculations",
        ),
    ] = 0
    firm_capacity_values: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Firm Capacity Values",
            description="Firm capacity values corresponding to the Capacity Points of connected Firm Capacity Groups",
        ),
    ] = 0
    fixed_cost_scalar: Annotated[
        float | int,
        Field(
            alias="Fixed Cost Scalar",
            description="This parameter is a percentage to represent the amount of fixed cost considered in the recovery algorithm (but PLEXOS will still report the full amount of fixed costs on each generator/line)",
            ge=0,
        ),
    ] = 1
    fixed_generation: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Fixed Generation",
            description="Fixed (or embedded) generation",
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
    formulate_load: Annotated[
        int,
        Field(
            alias="Formulate Load",
            description="Flag if the Load is formulated as a decision variable",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    generator_settlement_model: Annotated[
        int,
        Field(
            alias="Generator Settlement Model",
            description="Model used to determine price paid to generators.",
            json_schema_extra={"enum": [0, 1, 2, 3, 4, 5, 6, 7]},
        ),
    ] = 0
    include_in_kron_reduction: Annotated[
        int,
        Field(
            alias="Include in Kron Reduction",
            description="A flag indicating if the selected region should be included in the Kron-reduction algorithm",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    include_in_marginal_unit: Annotated[
        int,
        Field(
            alias="Include in Marginal Unit",
            description="Flag if the region is included in the Region Marginal Unit Diagnostic",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    include_in_region_supply: Annotated[
        int,
        Field(
            alias="Include in Region Supply",
            description="Flag if the region is included in the Region Supply Diagnostic",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    include_in_uplift: Annotated[
        int,
        Field(
            alias="Include in Uplift",
            description="If uplift is allowed in the period",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    internal_voll: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Internal VoLL",
            description="Region specific value of lost load",
            ge=0,
        ),
    ] = 0
    internal_voll_level: Annotated[
        float | int,
        PLEXOSProperty(units="MWh"),
        Field(
            alias="Internal VoLL Level",
            description="Level of unserved energy VoLL applies to",
        ),
    ] = 0
    is_strategic: Annotated[
        int,
        Field(
            alias="Is Strategic",
            description="If the Region's Generators are included in Competition modelling.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    load: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Load",
            description="Load",
        ),
    ] = 0
    load_includes_losses: Annotated[
        int,
        Field(
            alias="Load Includes Losses",
            description="Flag if input load includes transmission losses",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    load_metering_point: Annotated[
        int,
        Field(
            alias="Load Metering Point",
            description="Metering point for input loads in the region",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    load_scalar: Annotated[
        float | int,
        Field(
            alias="Load Scalar",
            description="Scale factor for raw load figures (to convert as required to nodal load)",
        ),
    ] = 1
    load_settlement_model: Annotated[
        int,
        Field(
            alias="Load Settlement Model",
            description="Model used to determine price paid by loads.",
            json_schema_extra={"enum": [0, 1, 2, 3, 4, 5, 6, 7]},
        ),
    ] = 2
    lolp_target: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="LOLP Target",
            description="Loss of Load Probability target for this region",
            ge=0,
            le=100,
        ),
    ] = 0
    maintenance_factor: Annotated[
        float | int,
        Field(
            alias="Maintenance Factor",
            description="Maintenance factor",
            ge=0,
        ),
    ] = 1
    max_capacity_reserve_margin: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Capacity Reserve Margin",
            description="Maximum capacity reserve margin for capacity planning",
        ),
    ] = 1e30
    max_capacity_reserves: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Capacity Reserves",
            description="Maximum capacity reserves allowed",
        ),
    ] = 1e30
    max_dump_energy: Annotated[
        float | int,
        PLEXOSProperty(units="MWh"),
        Field(
            alias="Max Dump Energy",
            description="Maximum dump energy",
            ge=0,
        ),
    ] = 1e30
    max_dump_energy_day: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Dump Energy Day",
            description="Maximum dump energy in day",
            ge=0,
        ),
    ] = 1e30
    max_dump_energy_factor: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Dump Energy Factor",
            description="Maximum proportion of energy dumped",
            ge=0,
        ),
    ] = 100
    max_dump_energy_factor_day: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Dump Energy Factor Day",
            description="Maximum proportion of energy dumped in day",
            ge=0,
        ),
    ] = 100
    max_dump_energy_factor_hour: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Dump Energy Factor Hour",
            description="Maximum proportion of energy dumped in hour",
            ge=0,
        ),
    ] = 100
    max_dump_energy_factor_month: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Dump Energy Factor Month",
            description="Maximum proportion of energy dumped in month",
            ge=0,
        ),
    ] = 100
    max_dump_energy_factor_week: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Dump Energy Factor Week",
            description="Maximum proportion of energy dumped in week",
            ge=0,
        ),
    ] = 100
    max_dump_energy_factor_year: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Dump Energy Factor Year",
            description="Maximum proportion of energy dumped in year",
            ge=0,
        ),
    ] = 100
    max_dump_energy_hour: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Dump Energy Hour",
            description="Maximum dump energy in hour",
            ge=0,
        ),
    ] = 1e30
    max_dump_energy_month: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Dump Energy Month",
            description="Maximum dump energy in month",
            ge=0,
        ),
    ] = 1e30
    max_dump_energy_week: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Dump Energy Week",
            description="Maximum dump energy in week",
            ge=0,
        ),
    ] = 1e30
    max_dump_energy_year: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Dump Energy Year",
            description="Maximum dump energy in year",
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
    max_maintenance: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Maintenance",
            description="Maximum generation capacity allowed to be scheduled on maintenance",
            ge=0,
        ),
    ] = 1e30
    max_unserved_energy: Annotated[
        float | int,
        PLEXOSProperty(units="MWh"),
        Field(
            alias="Max Unserved Energy",
            description="Maximum unserved energy",
            ge=0,
        ),
    ] = 1e30
    max_unserved_energy_day: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Unserved Energy Day",
            description="Maximum unserved energy in day",
            ge=0,
        ),
    ] = 1e30
    max_unserved_energy_factor: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Unserved Energy Factor",
            description="Maximum proportion of energy unserved",
            ge=0,
        ),
    ] = 100
    max_unserved_energy_factor_day: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Unserved Energy Factor Day",
            description="Maximum proportion of energy unserved in day",
            ge=0,
        ),
    ] = 100
    max_unserved_energy_factor_hour: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Unserved Energy Factor Hour",
            description="Maximum proportion of energy unserved in hour",
            ge=0,
        ),
    ] = 100
    max_unserved_energy_factor_month: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Unserved Energy Factor Month",
            description="Maximum proportion of energy unserved in month",
            ge=0,
        ),
    ] = 100
    max_unserved_energy_factor_week: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Unserved Energy Factor Week",
            description="Maximum proportion of energy unserved in week",
            ge=0,
        ),
    ] = 100
    max_unserved_energy_factor_year: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Unserved Energy Factor Year",
            description="Maximum proportion of energy unserved in year",
            ge=0,
        ),
    ] = 100
    max_unserved_energy_hour: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Unserved Energy Hour",
            description="Maximum unserved energy in hour",
            ge=0,
        ),
    ] = 1e30
    max_unserved_energy_month: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Unserved Energy Month",
            description="Maximum unserved energy in month",
            ge=0,
        ),
    ] = 1e30
    max_unserved_energy_week: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Unserved Energy Week",
            description="Maximum unserved energy in week",
            ge=0,
        ),
    ] = 1e30
    max_unserved_energy_year: Annotated[
        float | int,
        PLEXOSProperty(units="GWh"),
        Field(
            alias="Max Unserved Energy Year",
            description="Maximum unserved energy in year",
            ge=0,
        ),
    ] = 1e30
    min_capacity_reserve_margin: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Min Capacity Reserve Margin",
            description="Minimum capacity reserve margin for capacity planning",
        ),
    ] = -1e30
    min_capacity_reserves: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Min Capacity Reserves",
            description="Minimum capacity reserves allowed",
        ),
    ] = -1e30
    min_native_capacity_reserve_margin: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Min Native Capacity Reserve Margin",
            description="Minimum capacity reserve margin supplied only by sources in the Region",
        ),
    ] = -1e30
    min_native_capacity_reserves: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Min Native Capacity Reserves",
            description="Minimum capacity reserves supplied only by sources in the Region",
        ),
    ] = -1e30
    mlf_adjusts_bid_price: Annotated[
        int,
        Field(
            alias="MLF Adjusts Bid Price",
            description="If Purchaser [Marginal Loss Factor] adjusts [Bid Price].",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    mlf_adjusts_no_load_cost: Annotated[
        int,
        Field(
            alias="MLF Adjusts No Load Cost",
            description="If Generator [Marginal Loss Factor] adjusts [Offer No Load Cost].",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    mlf_adjusts_offer_price: Annotated[
        int,
        Field(
            alias="MLF Adjusts Offer Price",
            description="If Generator [Marginal Loss Factor] adjusts [Offer Price].",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    mlf_adjusts_start_cost: Annotated[
        int,
        Field(
            alias="MLF Adjusts Start Cost",
            description="If Generator [Marginal Loss Factor] adjusts [Start Cost].",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    peak_period: Annotated[
        int,
        Field(
            alias="Peak Period",
            description="Indicates periods that include the peak load",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    pool_type: Annotated[
        int,
        Field(
            alias="Pool Type",
            description="Gross or Net Pool for settlement of Financial Contracts in the Region.",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    price: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Price",
            description="Price",
        ),
    ] = 0
    price_cap: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Price Cap",
            description="Cap on generator offer prices",
        ),
    ] = 1e30
    price_floor: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Price Floor",
            description="Floor on generator offer prices",
        ),
    ] = -1e30
    price_of_dump_energy: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Price of Dump Energy",
            description="Price of dump energy per MWh (1=hard constraint)",
        ),
    ] = -1000
    reference_load: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Reference Load",
            description="Reference load for distributed load slack model",
        ),
    ] = 1
    report_marginal_resources: Annotated[
        int,
        Field(
            alias="Report Marginal Resources",
            description="If marginal resources are reported for the region in the solution",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    report_objects_in_region: Annotated[
        int,
        Field(
            alias="Report Objects in Region",
            description="If objects in the Region such as Nodes, Lines, Generators, etc should be reported.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    seasonal_reserve_constraint_active: Annotated[
        int,
        Field(
            alias="Seasonal Reserve Constraint Active",
            description="Specifies when a seasonal capacity reserve is active",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    solution_detail: Annotated[
        int,
        Field(
            alias="Solution Detail",
            description="Solution detail to be used for the region",
            json_schema_extra={"enum": [1, 2, 3]},
        ),
    ] = 1
    transmission_clustering_level: Annotated[
        float | int,
        Field(
            alias="Transmission Clustering Level",
            description="Cluster nodes until this number of equivalent nodes remain (-1 means no clustering)",
        ),
    ] = -1
    transmission_clustering_tolerance: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Transmission Clustering Tolerance",
            description="Cluster nodes until this level of accuracy is reached (100% means no clustering)",
            ge=0,
            le=100,
        ),
    ] = 100
    transmission_constraint_voltage_threshold: Annotated[
        float | int,
        PLEXOSProperty(units="kV"),
        Field(
            alias="Transmission Constraint Voltage Threshold",
            description="Voltage level at which thermal limits are modeled in this region.",
            ge=0,
        ),
    ] = 0
    transmission_constraints_enabled: Annotated[
        int,
        Field(
            alias="Transmission Constraints Enabled",
            description="If transmission line constraints are enabled in this region.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    transmission_interface_constraints_enabled: Annotated[
        int,
        Field(
            alias="Transmission Interface Constraints Enabled",
            description="If interface constraints are enabled in this region.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    transmission_report_enabled: Annotated[
        int,
        Field(
            alias="Transmission Report Enabled",
            description="If transmission reporting is enabled in this region.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    transmission_report_injection_and_load_nodes: Annotated[
        int,
        Field(
            alias="Transmission Report Injection and Load Nodes",
            description="If all injection and load buses (nodes) are reported on (regardless of voltage) in this region.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    transmission_report_lines_in_interfaces: Annotated[
        int,
        Field(
            alias="Transmission Report Lines In Interfaces",
            description="If all flows on lines selected interfaces are reported in this region.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    transmission_report_voltage_threshold: Annotated[
        float | int,
        PLEXOSProperty(units="kV"),
        Field(
            alias="Transmission Report Voltage Threshold",
            description="Voltage level at which transmission reporting begins in this region.",
            ge=0,
        ),
    ] = 0
    uniform_pricing_pumped_storage_price_setting: Annotated[
        int,
        Field(
            alias="Uniform Pricing Pumped Storage Price Setting",
            description="If pumped storage can set the SMP",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    uniform_pricing_relax_ancillary_services: Annotated[
        int,
        Field(
            alias="Uniform Pricing Relax Ancillary Services",
            description="If ancillary service requirements are relaxed in calculating SMP",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    uniform_pricing_relax_generator_constraints: Annotated[
        int,
        Field(
            alias="Uniform Pricing Relax Generator Constraints",
            description="If generator non-technical constraints are relaxed in calculating SMP",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    uniform_pricing_relax_generic_constraints: Annotated[
        int,
        Field(
            alias="Uniform Pricing Relax Generic Constraints",
            description="If other generic constraints are relaxed in calculating SMP",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    uniform_pricing_relax_transmission_limits: Annotated[
        int,
        Field(
            alias="Uniform Pricing Relax Transmission Limits",
            description="If transmission limits are relaxed in calculating SMP",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    units: Annotated[
        int,
        PLEXOSProperty(is_enum=True),
        Field(
            alias="Units",
            description="Flag if the Region is included in the simulation.",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 1
    unserved_energy_method: Annotated[
        int,
        PLEXOSProperty(is_enum=True),
        Field(
            alias="Unserved Energy Method",
            description="Unserved Energy Method to be used for the region",
            json_schema_extra={"enum": [0, 1, 2, 3]},
        ),
    ] = 0
    uplift_alpha: Annotated[
        float | int,
        Field(
            alias="Uplift Alpha",
            description="Alpha parameter for SEM-style uplift payment (weight on total generation revenues)",
        ),
    ] = 1
    uplift_beta: Annotated[
        float | int,
        Field(
            alias="Uplift Beta",
            description="Beta parameter for SEM-style uplift payment (weight on squared deviations from shadow price)",
        ),
    ] = 0
    uplift_compatibility: Annotated[
        int,
        Field(
            alias="Uplift Compatibility",
            description="Uplift calculation compatibility (match to market being modelled)",
            json_schema_extra={"enum": [1, 2, 3]},
        ),
    ] = 1
    uplift_cost_basis: Annotated[
        int,
        Field(
            alias="Uplift Cost Basis",
            description="Basis for calculating generation cost for uplift calculations (cost-based or bid-base)",
            json_schema_extra={"enum": [1, 2]},
        ),
    ] = 1
    uplift_delta: Annotated[
        float | int,
        Field(
            alias="Uplift Delta",
            description="Delta parameter for SEM-style uplift payment (proportion of shadow total system revenues after uplift)",
        ),
    ] = 5
    uplift_detect_active_min_stable_level_constraints: Annotated[
        int,
        Field(
            alias="Uplift Detect Active Min Stable Level Constraints",
            description="If the uplift calculation should exclude units running at min stable level",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    uplift_detect_active_ramp_constraints: Annotated[
        int,
        Field(
            alias="Uplift Detect Active Ramp Constraints",
            description="If the uplift calculation should exclude generators on ramp limits",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    uplift_enabled: Annotated[
        int,
        Field(
            alias="Uplift Enabled",
            description="If uplift is added to market prices",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    uplift_include_no_load_cost: Annotated[
        int,
        Field(
            alias="Uplift Include No-Load Cost",
            description="If the uplift calculation should include recovery of no-load costs",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    uplift_include_start_cost: Annotated[
        int,
        Field(
            alias="Uplift Include Start Cost",
            description="If the uplift calculation should include recovery of start costs",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    voll: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="VoLL",
            description="Value of lost load (VoLL)",
        ),
    ] = 10000
    wheeling_charge: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Wheeling Charge",
            description="Wheeling charge on exports from the region",
        ),
    ] = 0
    wheeling_method: Annotated[
        int,
        Field(
            alias="Wheeling Method",
            description="Export wheeling charge method",
            json_schema_extra={"enum": [1, 2]},
        ),
    ] = 1
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
    def example(cls) -> "PLEXOSRegion":
        """Create an example PLEXOSRegion."""
        return PLEXOSRegion(
            name="ExampleRegion",
            object_id=1,
        )
