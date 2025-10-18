"""The following file contains Pydantic models for a PLEXOS Interface model."""

from typing import Annotated

from pydantic import Field

from .component import PLEXOSObject
from .property_specification import PLEXOSProperty


class PLEXOSInterface(PLEXOSObject):
    """Class that holds attributes for the transmission interface."""

    build_non_anticipativity: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Build Non-anticipativity",
            description="Price for violating non-anticipativity constraints in scenario-wise decomposition mode",
        ),
    ] = -1
    contingency_limit_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Contingency Limit Penalty",
            description="Penalty for exceeding contingency flow limits",
        ),
    ] = -1
    economic_life: Annotated[
        float | int,
        PLEXOSProperty(units="yr"),
        Field(
            alias="Economic Life",
            description="Economic life of the interface (period over which expansion costs are recovered).",
            ge=0,
        ),
    ] = 30
    expansion_cost: Annotated[
        float | int,
        PLEXOSProperty(units="usd"),
        Field(
            alias="Expansion Cost",
            description="Cost of expanding the interface by one megawatt",
        ),
    ] = 0
    firm_capacity: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Firm Capacity",
            description="Contribution of the interface to region capacity reserves",
        ),
    ] = 0
    fixed_flow: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Fixed Flow",
            description="Fixed flow on interface",
        ),
    ] = 0
    fixed_flow_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Fixed Flow Penalty",
            description="Penalty for violation of [Fixed Flow].",
        ),
    ] = -1
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
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Limit Penalty",
            description="Penalty for violation of limits",
        ),
    ] = -1
    max_expansion: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Expansion",
            description="Maximum interface expansion",
            ge=0,
        ),
    ] = 0
    max_expansion_in_year: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Expansion In Year",
            description="Maximum interface expansion allowed in the year",
            ge=0,
        ),
    ] = 1e30
    max_flow: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Flow",
            description="Maximum flow on interface",
        ),
    ] = 1e30
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
    min_expansion: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Min Expansion",
            description="Minimum interface expansion",
            ge=0,
        ),
    ] = 0
    min_expansion_in_year: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Min Expansion In Year",
            description="Minimum interface expansion allowed in the year",
            ge=0,
        ),
    ] = 0
    min_flow: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Min Flow",
            description="Minimum flow on interface",
        ),
    ] = -1e30
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
    overload_max_rating: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Overload Max Rating",
            description="Emergency rating in the reference direction",
        ),
    ] = 0
    overload_min_rating: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Overload Min Rating",
            description="Emergency rating in the counter-reference direction",
        ),
    ] = 0
    ramp_penalty: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MW"),
        Field(
            alias="Ramp Penalty",
            description="Penalty for changes in flow on the line",
        ),
    ] = 0
    units: Annotated[
        int,
        PLEXOSProperty(is_enum=True),
        Field(
            alias="Units",
            description="Flag if interface is in service",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 1
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
    def example(cls) -> "PLEXOSInterface":
        """Create an example PLEXOSInterface."""
        return PLEXOSInterface(
            name="Example Line",
            max_flow=100,
        )
