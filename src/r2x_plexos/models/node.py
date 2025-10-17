"""The following file contains Pydantic models for a PLEXOS Node model."""

from typing import Annotated

from pydantic import Field

from .component import PLEXOSTopology
from .property_specification import PLEXOSProperty


class PLEXOSNode(PLEXOSTopology):
    """Class that holds attributes about PLEXOS Node."""

    ac_reactive_power: Annotated[
        float | int,
        PLEXOSProperty(units="MVA"),
        Field(
            alias="AC Reactive Power",
            description="The reactive power injected or withdrawn from a node, as determined by an AC power flow solution",
        ),
    ] = 0
    ac_voltage_magnitude: Annotated[
        float | int,
        PLEXOSProperty(units="pu"),
        Field(
            alias="AC Voltage Magnitude",
            description="The per-unit voltage magnitude of a node, as determined by an AC power flow solution",
        ),
    ] = 1
    allow_dump_energy: Annotated[
        int,
        Field(
            alias="Allow Dump Energy",
            description="Model Node [Dump Energy] in the mathematical program.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    allow_unserved_energy: Annotated[
        int,
        Field(
            alias="Allow Unserved Energy",
            description="Model Node [Unserved Energy] in the mathematical program.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    always_calculate_ptdf: Annotated[
        int,
        Field(
            alias="Always Calculate PTDF",
            description="Flag if the PDFFs associated with the node and transmission constraints will be calculated",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    dsp_bid_price: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="DSP Bid Price",
            description="Demand-side participation bid price",
        ),
    ] = 0
    dsp_bid_quantity: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="DSP Bid Quantity",
            description="Demand-side participation bid quantity",
        ),
    ] = 0
    dsp_bid_ratio: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="DSP Bid Ratio",
            description="Demand-side participation quantity as a percentage of nodal load",
            ge=0,
            le=100,
        ),
    ] = 0
    enable_atc_calculation: Annotated[
        int,
        Field(
            alias="Enable ATC Calculation",
            description="Flag if ATC calculation should be enabled for the node.",
            json_schema_extra={"enum": [0, 1, 2, 3]},
        ),
    ] = 0
    fixed_generation: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Fixed Generation",
            description="Fixed (or embedded) generation at the node",
        ),
    ] = 0
    fixed_load: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Fixed Load",
            description="Fixed load at the node",
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
    is_slack_bus: Annotated[
        int,
        Field(
            alias="Is Slack Bus",
            description="Set if this is the slack bus",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    is_unmapped_resource_bus: Annotated[
        int,
        Field(
            alias="Is Unmapped Resource Bus",
            description="Set if this is the unmapped resource bus",
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
            description="Proportion of region load that occurs at the node",
            ge=-1,
            le=1,
        ),
    ] = 1
    maintenance_factor: Annotated[
        float | int,
        Field(
            alias="Maintenance Factor",
            description="Maintenance biasing factor",
            ge=0,
        ),
    ] = 1
    max_maintenance: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Maintenance",
            description="Maximum generation capacity allowed to be scheduled on maintenance",
            ge=0,
        ),
    ] = 1e30
    max_net_injection: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Net Injection",
            description="Maximum net injection",
            ge=0,
        ),
    ] = 1e30
    max_net_offtake: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Net Offtake",
            description="Maximum net offtake",
            ge=0,
        ),
    ] = 1e30
    max_unserved_energy: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Unserved Energy",
            description="Maximum allowed Unserved Energy at the node.",
            ge=0,
        ),
    ] = 0
    min_capacity_reserve_margin: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Min Capacity Reserve Margin",
            description="Minimum capacity reserve margin",
        ),
    ] = -1e30
    min_capacity_reserves: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Min Capacity Reserves",
            description="Minimum capacity reserves",
        ),
    ] = -1e30
    must_report: Annotated[
        int,
        Field(
            alias="Must Report",
            description="If the node must be reported regardless of voltage",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    price: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Price",
            description="Locational marginal price",
        ),
    ] = 0
    rating: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Rating",
            description="Maximum power flow through the Node",
            ge=0,
        ),
    ] = 1e30
    reference_generation: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Reference Generation",
            description="Reference generation from the network case file",
        ),
    ] = 1
    reference_load: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Reference Load",
            description="Reference load for distributed load slack model",
        ),
    ] = 1
    units: Annotated[
        int,
        PLEXOSProperty(is_enum=True),
        Field(
            alias="Units",
            description="Flag if bus is in service",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 1
    voltage: Annotated[
        float | int,
        PLEXOSProperty(units="kV"),
        Field(
            alias="Voltage",
            description="Voltage",
            ge=0,
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
    def example(cls) -> "PLEXOSNode":
        """Create an example PLEXOSNode."""
        return PLEXOSNode(
            name="ExampleNode",
            object_id=1,
            is_slack_bus=-1,
            load=100.0,
            units=1,
        )
