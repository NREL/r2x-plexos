"""The following file contains Pydantic models for a PLEXOS fuel model."""

from typing import Annotated

from pydantic import Field

from .component import PLEXOSObject
from .property_specification import PLEXOSProperty


class PLEXOSFuel(PLEXOSObject):
    """Class that holds attributes about PLEXOS Fuels for thermal generators."""

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
        Field(
            alias="Decomposition Bound Penalty",
            description="Penalty applied to violation of stockpile bounds when the decomposition implies possible violations.",
            ge=0,
        ),
    ] = 1000000
    decomposition_method: Annotated[
        int,
        Field(
            alias="Decomposition Method",
            description="Method used to pass the optimal stockpile trajectory from one simulation phase to the next.",
            json_schema_extra={"enum": [0, 1, 2]},
        ),
    ] = 1
    decomposition_penalty_a: Annotated[
        float | int,
        Field(
            alias="Decomposition Penalty a",
            description="Decomposition stockpile target penalty function 'a' term.",
        ),
    ] = 0.0489
    decomposition_penalty_b: Annotated[
        float | int,
        Field(
            alias="Decomposition Penalty b",
            description="Decomposition stockpile target penalty function 'b' term.",
        ),
    ] = 0.6931
    decomposition_penalty_c: Annotated[
        float | int,
        Field(
            alias="Decomposition Penalty c",
            description="Decomposition stockpile target penalty function 'c' term.",
        ),
    ] = 0
    decomposition_penalty_x: Annotated[
        float | int,
        Field(
            alias="Decomposition Penalty x",
            description="Decomposition stockpile target penalty function 'x' term.",
        ),
    ] = 1.1
    delivery: Annotated[
        float | int,
        Field(
            alias="Delivery",
            description="Fuel delivered to the stockpile",
        ),
    ] = 0
    delivery_charge: Annotated[
        float | int,
        Field(
            alias="Delivery Charge",
            description="Cost of delivering fuel to the stockpile",
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
    internal_volume_scalar: Annotated[
        float | int,
        Field(
            alias="Internal Volume Scalar",
            description="Storage volume scaling factor used internal to the mathematical program.",
            gt=0,
        ),
    ] = 1
    inventory_charge: Annotated[
        float | int,
        Field(
            alias="Inventory Charge",
            description="Cost applied to closing inventory in the stockpile",
        ),
    ] = 0
    max_inventory: Annotated[
        float | int,
        Field(
            alias="Max Inventory",
            description="Maximum fuel allowed in stockpile",
        ),
    ] = 0
    max_offtake: Annotated[
        float | int,
        Field(
            alias="Max Offtake",
            description="Maximum fuel offtake per interval",
        ),
    ] = 1e30
    max_offtake_day: Annotated[
        float | int,
        Field(
            alias="Max Offtake Day",
            description="Maximum fuel offtake in day",
        ),
    ] = 1e30
    max_offtake_hour: Annotated[
        float | int,
        Field(
            alias="Max Offtake Hour",
            description="Maximum fuel offtake in hour",
        ),
    ] = 1e30
    max_offtake_month: Annotated[
        float | int,
        Field(
            alias="Max Offtake Month",
            description="Maximum fuel offtake in month",
        ),
    ] = 1e30
    max_offtake_penalty: Annotated[
        float | int,
        Field(
            alias="Max Offtake Penalty",
            description="Penalty applied to violations of [Max Offtake]constraints",
        ),
    ] = -1
    max_offtake_week: Annotated[
        float | int,
        Field(
            alias="Max Offtake Week",
            description="Maximum fuel offtake in week",
        ),
    ] = 1e30
    max_offtake_year: Annotated[
        float | int,
        Field(
            alias="Max Offtake Year",
            description="Maximum fuel offtake in year",
        ),
    ] = 1e30
    max_withdrawal: Annotated[
        float | int,
        Field(
            alias="Max Withdrawal",
            description="Maximum amount of fuel that can be taken from stockpile",
            ge=0,
        ),
    ] = 1e30
    max_withdrawal_day: Annotated[
        float | int,
        Field(
            alias="Max Withdrawal Day",
            description="Maximum amount of fuel that can be taken from stockpile in a day",
            ge=0,
        ),
    ] = 1e30
    max_withdrawal_hour: Annotated[
        float | int,
        Field(
            alias="Max Withdrawal Hour",
            description="Maximum amount of fuel that can be taken from stockpile in a hour",
            ge=0,
        ),
    ] = 1e30
    max_withdrawal_month: Annotated[
        float | int,
        Field(
            alias="Max Withdrawal Month",
            description="Maximum amount of fuel that can be taken from stockpile in a month",
            ge=0,
        ),
    ] = 1e30
    max_withdrawal_week: Annotated[
        float | int,
        Field(
            alias="Max Withdrawal Week",
            description="Maximum amount of fuel that can be taken from stockpile in a week",
            ge=0,
        ),
    ] = 1e30
    max_withdrawal_year: Annotated[
        float | int,
        Field(
            alias="Max Withdrawal Year",
            description="Maximum amount of fuel that can be taken from stockpile in a year",
            ge=0,
        ),
    ] = 1e30
    min_inventory: Annotated[
        float | int,
        Field(
            alias="Min Inventory",
            description="Minimum fuel required in stockpile",
        ),
    ] = 0
    min_offtake: Annotated[
        float | int,
        Field(
            alias="Min Offtake",
            description="Minimum fuel offtake per interval",
        ),
    ] = 0
    min_offtake_day: Annotated[
        float | int,
        Field(
            alias="Min Offtake Day",
            description="Minimum fuel offtake in day",
        ),
    ] = 0
    min_offtake_hour: Annotated[
        float | int,
        Field(
            alias="Min Offtake Hour",
            description="Minimum fuel offtake in hour",
        ),
    ] = 0
    min_offtake_month: Annotated[
        float | int,
        Field(
            alias="Min Offtake Month",
            description="Minimum fuel offtake in month",
        ),
    ] = 0
    min_offtake_penalty: Annotated[
        float | int,
        Field(
            alias="Min Offtake Penalty",
            description="Penalty applied to violations of [Min Offtake] constraints",
        ),
    ] = 1000
    min_offtake_week: Annotated[
        float | int,
        Field(
            alias="Min Offtake Week",
            description="Minimum fuel offtake in week",
        ),
    ] = 0
    min_offtake_year: Annotated[
        float | int,
        Field(
            alias="Min Offtake Year",
            description="Minimum fuel offtake in year",
        ),
    ] = 0
    min_withdrawal: Annotated[
        float | int,
        Field(
            alias="Min Withdrawal",
            description="Amount of fuel that must be taken from stockpile",
            ge=0,
        ),
    ] = 0
    min_withdrawal_day: Annotated[
        float | int,
        Field(
            alias="Min Withdrawal Day",
            description="Amount of fuel that must be taken from stockpile each day",
            ge=0,
        ),
    ] = 0
    min_withdrawal_hour: Annotated[
        float | int,
        Field(
            alias="Min Withdrawal Hour",
            description="Amount of fuel that must be taken from stockpile each hour",
            ge=0,
        ),
    ] = 0
    min_withdrawal_month: Annotated[
        float | int,
        Field(
            alias="Min Withdrawal Month",
            description="Amount of fuel that must be taken from stockpile each month",
            ge=0,
        ),
    ] = 0
    min_withdrawal_week: Annotated[
        float | int,
        Field(
            alias="Min Withdrawal Week",
            description="Amount of fuel that must be taken from stockpile each week",
            ge=0,
        ),
    ] = 0
    min_withdrawal_year: Annotated[
        float | int,
        Field(
            alias="Min Withdrawal Year",
            description="Amount of fuel that must be taken from stockpile each year",
            ge=0,
        ),
    ] = 0
    opening_inventory: Annotated[
        float | int,
        Field(
            alias="Opening Inventory",
            description="Initial fuel in the stockpile",
            ge=0,
        ),
    ] = 0
    price: Annotated[
        float | int,
        Field(
            alias="Price",
            description="Fuel price",
        ),
    ] = 0
    price_incr: Annotated[
        float | int,
        Field(
            alias="Price Incr",
            description="Increment to the price of the fuel",
        ),
    ] = 0
    price_scalar: Annotated[
        float | int,
        Field(
            alias="Price Scalar",
            description="Multiplier on the price of the fuel",
        ),
    ] = 1
    reservation_charge: Annotated[
        float | int,
        Field(
            alias="Reservation Charge",
            description="Cost applied to unused inventory capacity in the stockpile",
        ),
    ] = 0
    shadow_price: Annotated[
        float | int,
        Field(
            alias="Shadow Price",
            description="Shadow price of fuel (if defined as input, sets the internal price for fuel)",
        ),
    ] = 0
    shadow_price_incr: Annotated[
        float | int,
        Field(
            alias="Shadow Price Incr",
            description="Increment to the shadow price of the fuel (use only when Shadow Price is defined)",
        ),
    ] = 0
    shadow_price_scalar: Annotated[
        float | int,
        Field(
            alias="Shadow Price Scalar",
            description="Multiplier on the shadow price of the fuel (use only when Shadow Price is defined)",
        ),
    ] = 1
    tax: Annotated[
        float | int,
        Field(
            alias="Tax",
            description="Fuel tax",
        ),
    ] = 0
    units: Annotated[
        int,
        Field(
            alias="Units",
            description="Flag if fuel exists",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 1
    withdrawal_charge: Annotated[
        float | int,
        Field(
            alias="Withdrawal Charge",
            description="Incremental cost of taking fuel from stockpile",
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
    def example(cls) -> "PLEXOSFuel":
        """Create an example PLEXOSFuel."""
        return PLEXOSFuel(
            name="ExampleFuel",
        )
