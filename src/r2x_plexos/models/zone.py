"""The following file contains Pydantic models for a PLEXOS zone model."""

from typing import Annotated

from pydantic import Field

from .component import PLEXOSTopology
from .property_specification import PLEXOSProperty


class PLEXOSZone(PLEXOSTopology):
    """Class that holds all attributes of a PLEXOS zone."""

    capacity_excess_price: Annotated[
        float | int,
        PLEXOSProperty(units="usd/kW/yr"),
        Field(
            alias="Capacity Excess Price",
            description="Penalty for an excess of capacity reserves",
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
    # Load attributes
    formulate_load: Annotated[
        int,
        Field(
            alias="Formulate Load",
            description="Flag if the Load is formulated as a decision variable",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    load: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Load",
            description="Load",
        ),
    ] = 0
    load_participation_factor: Annotated[
        float | int,
        Field(
            alias="Load Participation Factor",
            description="Proportion of region load that occurs in the zone",
            ge=-1,
            le=1,
        ),
    ] = 0
    load_scalar: Annotated[
        float | int,
        Field(
            alias="Load Scalar",
            description="Scale factor for input [Load]",
        ),
    ] = 1
    load_settlement_model: Annotated[
        int,
        Field(
            alias="Load Settlement Model",
            description="Model used to determine energy prices reported in the zone.",
            json_schema_extra={"enum": [0, 1, 2]},
        ),
    ] = 0
    lolp_target: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="LOLP Target",
            description="Loss of Load Probability target for this zone",
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
    # Capacity reserve margin attributes
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
    # Dump energy constraints
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
            description="Maximum dump energy in month in GWh",
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
    # Generation curtailment constraints
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
    # Unserved energy constraints
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
    # Minimum capacity constraints
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
            description="Minimum capacity reserve margin supplied only by sources in the Zone",
        ),
    ] = -1e30
    min_native_capacity_reserves: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Min Native Capacity Reserves",
            description="Minimum capacity reserves supplied only by sources in the Zone",
        ),
    ] = -1e30
    # Other zone attributes
    peak_period: Annotated[
        int,
        Field(
            alias="Peak Period",
            description="Indicates periods that include the peak load",
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
    units: Annotated[
        int,
        Field(
            alias="Units",
            description="Flag if the Zone is in service",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 1
    wheeling_charge: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Wheeling Charge",
            description="Wheeling charge on exports from the zone",
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
    # Pass-through values
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
    def example(cls) -> "PLEXOSZone":
        """Create an example PLEXOSZone."""
        return PLEXOSZone(
            name="Example Zone",
        )
