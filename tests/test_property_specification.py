"""Tests for PropertySpec validator."""

from typing import Annotated

import pytest
from pydantic import BaseModel, ValidationError

from r2x_plexos import PLEXOSProperty, PLEXOSPropertyValue


class SimpleModel(BaseModel):
    value: Annotated[float | int, PLEXOSProperty(units="MW")]


class BandedModel(BaseModel):
    allowed: Annotated[float | int, PLEXOSProperty(units="MW")]
    no_bands: Annotated[float | int, PLEXOSProperty(units="%", allow_bands=False)]


def test_property_spec_float_input():
    model = SimpleModel(value=100.0)
    assert model.value == 100.0
    assert isinstance(model.value, float)


def test_property_spec_int_input():
    model = SimpleModel(value=100)
    assert model.value == 100.0
    assert isinstance(model.value, float)


def test_property_spec_dict_with_scenarios():
    # Use from_records since from_dict doesn't support collection formats
    prop = PLEXOSPropertyValue.from_records(
        [
            {"scenario": "Base", "value": 100},
            {"scenario": "High", "value": 120},
        ],
        units="MW",
    )
    model = SimpleModel(value=prop)
    assert isinstance(model.value, PLEXOSPropertyValue)
    assert model.value.units == "MW"
    assert model.value.get_scenarios() == ["Base", "High"]


def test_property_spec_dict_with_timeslices():
    # Use from_records since from_dict doesn't support collection formats
    prop = PLEXOSPropertyValue.from_records(
        [
            {"timeslice": "Peak", "value": 150},
            {"timeslice": "OffPeak", "value": 100},
        ],
        units="MW",
    )
    model = SimpleModel(value=prop)
    assert isinstance(model.value, PLEXOSPropertyValue)
    assert model.value.units == "MW"
    assert model.value.get_timeslices() == ["OffPeak", "Peak"]


def test_property_spec_dict_with_bands():
    # Use from_records since from_dict doesn't support collection formats
    prop = PLEXOSPropertyValue.from_records(
        [
            {"band": 1, "value": 100},
            {"band": 2, "value": 50},
        ]
    )
    model = SimpleModel(value=prop)
    assert isinstance(model.value, PLEXOSPropertyValue)
    assert model.value.get_bands() == [1, 2]


def test_property_spec_units_injection():
    model = SimpleModel(value={"scenario": "Base", "value": 100})
    assert model.value.units == "MW"


def test_property_spec_units_not_overridden():
    model = SimpleModel(value={"scenario": "Base", "value": 100, "units": "kW"})
    assert model.value.units == "kW"


def test_property_spec_no_bands_allows_single_band():
    model = BandedModel(allowed=100.0, no_bands={"scenario": "Base", "value": 2.5})
    assert isinstance(model.no_bands, PLEXOSPropertyValue)


def test_property_spec_no_bands_rejects_multi_band():
    # Create multi-band property and pass it directly
    prop = PLEXOSPropertyValue.from_records(
        [
            {"band": 1, "value": 2.5},
            {"band": 2, "value": 3.0},
        ]
    )
    with pytest.raises(ValidationError):
        BandedModel(allowed=100.0, no_bands=prop)


def test_property_spec_plexos_property_input():
    prop = PLEXOSPropertyValue.from_records([{"scenario": "Base", "value": 100}], units="kW")
    model = SimpleModel(value=prop)
    assert isinstance(model.value, PLEXOSPropertyValue)
    assert model.value.units == "kW"


def test_property_spec_plexos_property_units_injection():
    prop = PLEXOSPropertyValue.from_records([{"scenario": "Base", "value": 100}])
    model = SimpleModel(value=prop)
    assert model.value.units == "MW"


def test_property_spec_invalid_type():
    with pytest.raises(ValidationError):
        SimpleModel(value="not a number")
