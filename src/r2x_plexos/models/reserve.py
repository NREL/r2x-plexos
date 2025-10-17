"""The following file contains Pydantic models for a PLEXOS Reserve model."""

from typing import Annotated

from pydantic import Field

from .component import PLEXOSObject
from .property_specification import PLEXOSProperty


class PLEXOSReserve(PLEXOSObject):
    """Class that holds attributes about PLEXOS reserves."""

    cost_allocation_model: Annotated[
        int,
        Field(
            alias="Cost Allocation Model",
            description="Reserve cost allocation method.",
            json_schema_extra={"enum": [0, 1, 2, 3]},
        ),
    ] = 0
    cut_off_size: Annotated[
        float | int,
        Field(
            alias="Cut-off Size",
            description="The size below which a generator will not be considered for a share in reserve costs",
        ),
    ] = 0
    duration: Annotated[
        float | int,
        PLEXOSProperty(units="s"),
        Field(
            alias="Duration",
            description="Time over which the required response must be maintained",
            ge=0,
        ),
    ] = 0
    dynamic_risk: Annotated[
        int,
        Field(
            alias="Dynamic Risk",
            description="If elements in the Generator Contingencies and Line Contingencies collections are considered for dynamic risk calculations",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    energy_usage: Annotated[
        float | int,
        Field(
            alias="Energy Usage",
            description="Percentage of reserve dispatched in energy market",
            ge=0,
            le=100,
        ),
    ] = 0
    energy_usage_for_replacement: Annotated[
        int,
        Field(
            alias="Energy Usage For Replacement",
            description="If Reserve energy usage is used for replacement reserve.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    include_in_lt_plan: Annotated[
        int,
        Field(
            alias="Include in LT Plan",
            description="If the reserve is modelled in the LT Plan phase.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    include_in_mt_schedule: Annotated[
        int,
        Field(
            alias="Include in MT Schedule",
            description="If the reserve is modelled in the MT Schedule phase.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    include_in_st_schedule: Annotated[
        int,
        Field(
            alias="Include in ST Schedule",
            description="If the reserve is modelled in the ST Schedule phase.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    is_enabled: Annotated[
        int,
        PLEXOSProperty(is_enum=True),
        Field(
            alias="Is Enabled",
            description="Flag if the reserve is enabled",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    max_provision: Annotated[
        float | int,
        Field(
            alias="Max Provision",
            description="Maximum provision allowed for reserve class",
            ge=0,
        ),
    ] = 1e30
    max_sharing: Annotated[
        float | int,
        Field(
            alias="Max Sharing",
            description="Maximum reserve contribution from other regions/zones",
            ge=0,
        ),
    ] = 1e30
    min_provision: Annotated[
        float | int,
        Field(
            alias="Min Provision",
            description="Minimum required reserve",
            ge=0,
        ),
    ] = 0
    mutually_exclusive: Annotated[
        int,
        PLEXOSProperty(is_enum=True),
        Field(
            alias="Mutually Exclusive",
            description="If generation capacity providing this reserve is mutually exclusive to other reserves",
            json_schema_extra={"enum": [0, 1, 2]},
        ),
    ] = 0
    prevent_replacement_during_mdt: Annotated[
        int,
        Field(
            alias="Prevent Replacement during MDT",
            description="Whether to provide Replacement Reserves during a generator's Min Down Time.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    price: Annotated[
        float | int,
        Field(
            alias="Price",
            description="Price",
        ),
    ] = 0
    price_cap: Annotated[
        float | int,
        Field(
            alias="Price Cap",
            description="Cap on Reserve Price for settlement",
        ),
    ] = 1e30
    price_floor: Annotated[
        float | int,
        Field(
            alias="Price Floor",
            description="Floor on Reserve Price for settlement",
        ),
    ] = -1e30
    risk_adjustment_factor: Annotated[
        float | int,
        Field(
            alias="Risk Adjustment Factor",
            description="Proportion of contingency size (MW reserve/MW contingency)",
        ),
    ] = 1
    sharing_enabled: Annotated[
        int,
        Field(
            alias="Sharing Enabled",
            description="If sharing of reserve across the transmission network is enabled.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    sharing_losses_enabled: Annotated[
        int,
        Field(
            alias="Sharing Losses Enabled",
            description="If sharing of reserve accounts for transmission losses.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    static_risk: Annotated[
        float | int,
        Field(
            alias="Static Risk",
            description="Additional static risk over and above dynamic risk",
        ),
    ] = 0
    timeframe: Annotated[
        float | int,
        PLEXOSProperty(units="s"),
        Field(
            alias="Timeframe",
            description="Timeframe in which the reserve is required",
            ge=0,
        ),
    ] = 1e30
    type: Annotated[
        int,
        PLEXOSProperty(is_enum=True),
        Field(
            alias="Type",
            description="Reserve type",
            json_schema_extra={"enum": [1, 2, 3, 4, 5, 6, 7, 8]},
        ),
    ] = 1
    unit_commitment: Annotated[
        int,
        Field(
            alias="Unit Commitment",
            description="If the set of Generators providing the Reserve is optimized or always includes all members of the Reserve Generators collection.",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    vors: Annotated[
        float | int,
        PLEXOSProperty,
        Field(
            alias="VoRS",
            description="Value of reserve shortage (-1 sets hard constraint)",
        ),
    ] = -1
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
    def example(cls) -> "PLEXOSReserve":
        """Create an example reserve."""
        return PLEXOSReserve(
            name="ExampleReserve",
        )
