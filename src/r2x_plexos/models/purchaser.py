"""The following file contains Pydantic models for a PLEXOS purchaser model."""

from typing import Annotated, Literal

from pydantic import Field

from .component import PLEXOSObject
from .property_specification import PLEXOSProperty


class PLEXOSPurchaser(PLEXOSObject):
    """Class that holds attributes about PLEXOS purchasers."""

    benefit_function_shape: Annotated[
        int,
        Field(
            alias="Benefit Function Shape",
            description="Shape of the benefit function.",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 1
    bid_price: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Bid Price",
            description="Value of energy in band",
        ),
    ] = -10000
    bid_quantity: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Bid Quantity",
            description="Quantity bid in band",
            ge=0,
        ),
    ] = 0
    demand_fn_intercept: Annotated[
        float | int,
        PLEXOSProperty(units="usd"),
        Field(
            alias="Demand Fn Intercept",
            description="Demand function vertical intercept",
            ge=0,
        ),
    ] = 0
    demand_fn_slope: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Demand Fn Slope",
            description="Demand function slope",
            le=0,
        ),
    ] = 0
    fixed_load: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Fixed Load",
            description="Fixed load",
            ge=0,
        ),
    ] = 0
    interruptible_load_logic: Annotated[
        int,
        Field(
            alias="Interruptible Load Logic",
            description=(
                "If the interruptible load supplied by the Purchaser is limited "
                "by the amount of cleared load bids."
            ),
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = 0
    load_obligation: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Load Obligation",
            description="Load obligation for capacity reserves.",
            ge=0,
        ),
    ] = 0
    load_settlement_source: Annotated[
        Literal[0, 1],
        Field(
            alias="Load Settlement Source",
            description="Source used to determine price paid by loads.",
        ),
    ] = 0
    marginal_loss_factor: Annotated[
        float | int,
        PLEXOSProperty,
        Field(
            alias="Marginal Loss Factor",
            description="Transmission marginal loss factor (MLF or TLF)",
            ge=0,
        ),
    ] = 1
    max_benefit_function_tranches: Annotated[
        float | int,
        Field(
            alias="Max Benefit Function Tranches",
            description="Maximum number of tranches in the piecewise linear benefit function.",
            ge=1,
        ),
    ] = 10
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
            description="Penalty applied to violations of [Max Energy] and [Max Load Factor] constraints.",
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
    max_load: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Max Load",
            description="Maximum load (sum of cleared demand bids)",
            ge=0,
        ),
    ] = 1e30
    max_load_factor: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Load Factor",
            description="Maximum load factor",
            ge=0,
            le=100,
        ),
    ] = 100
    max_load_factor_day: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Load Factor Day",
            description="Maximum load factor in day",
            ge=0,
            le=100,
        ),
    ] = 100
    max_load_factor_hour: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Load Factor Hour",
            description="Maximum load factor in hour",
            ge=0,
            le=100,
        ),
    ] = 100
    max_load_factor_month: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Load Factor Month",
            description="Maximum load factor in month",
            ge=0,
            le=100,
        ),
    ] = 100
    max_load_factor_week: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Load Factor Week",
            description="Maximum load factor in week",
            ge=0,
            le=100,
        ),
    ] = 100
    max_load_factor_year: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Max Load Factor Year",
            description="Maximum load factor in year",
            ge=0,
            le=100,
        ),
    ] = 100
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
            description="Penalty applied to violations of [Min Energy] and [Min Load Factor] constraints.",
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
    min_load: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Min Load",
            description="Minimum load if any load is cleared.",
            ge=0,
        ),
    ] = 0
    min_load_factor: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Min Load Factor",
            description="Minimum load factor",
            ge=0,
            le=100,
        ),
    ] = 0
    min_load_factor_day: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Min Load Factor Day",
            description="Minimum load factor in day",
            ge=0,
            le=100,
        ),
    ] = 0
    min_load_factor_hour: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Min Load Factor Hour",
            description="Minimum load factor in hour",
            ge=0,
            le=100,
        ),
    ] = 0
    min_load_factor_month: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Min Load Factor Month",
            description="Minimum load factor in month",
            ge=0,
            le=100,
        ),
    ] = 0
    min_load_factor_week: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Min Load Factor Week",
            description="Minimum load factor in week",
            ge=0,
            le=100,
        ),
    ] = 0
    min_load_factor_year: Annotated[
        float | int,
        PLEXOSProperty(units="%"),
        Field(
            alias="Min Load Factor Year",
            description="Minimum load factor in year",
            ge=0,
            le=100,
        ),
    ] = 0
    price_setting: Annotated[
        int,
        Field(
            alias="Price Setting",
            description="Flag if the Purchaser can set price",
            json_schema_extra={"enum": [0, -1]},
        ),
    ] = -1
    strategic_load_rating: Annotated[
        float | int,
        PLEXOSProperty(units="MW"),
        Field(
            alias="Strategic Load Rating",
            description="Purchaser load rating for application in RSI capacity calculations.",
        ),
    ] = 0
    tariff: Annotated[
        float | int,
        PLEXOSProperty(units="usd/MWh"),
        Field(
            alias="Tariff",
            description="Price paid by customers for load bid cleared",
        ),
    ] = 0
    units: Annotated[
        int,
        PLEXOSProperty(is_enum=True),
        Field(
            alias="Units",
            description="Flag if the Purchaser is in service",
            json_schema_extra={"enum": [0, 1]},
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
    def example(cls) -> "PLEXOSPurchaser":
        """Create an example PLEXOSPurchaser."""
        return PLEXOSPurchaser(
            name="ExamplePurchaser",
            object_id=1,
            bid_price=-10000,
            bid_quantity=100,
            units=1,
        )
