"""Tests for PlexosComponent auto-resolution."""

from typing import Annotated

from pydantic import BaseModel

from r2x_plexos import PLEXOSComponent, PLEXOSProperty, PLEXOSPropertyValue, scenario_priority


class Generator(PLEXOSComponent):
    max_capacity: Annotated[float | int, PLEXOSProperty(units="MW")]
    min_capacity: Annotated[float | int, PLEXOSProperty(units="MW")]


def test_auto_resolve_float():
    gen = Generator(max_capacity=100.0, min_capacity=50.0)
    assert gen.max_capacity == 100.0
    assert gen.min_capacity == 50.0


def test_auto_resolve_property_no_priority():
    # Pass PLEXOSPropertyValue directly instead of dict
    prop = PLEXOSPropertyValue.from_records(
        [
            {"scenario": "Base", "value": 100},
            {"scenario": "High", "value": 120},
        ]
    )
    gen = Generator(max_capacity=prop, min_capacity=50.0)
    assert gen.max_capacity == {"Base": 100, "High": 120}
    assert gen.min_capacity == 50.0


def test_auto_resolve_property_with_priority():
    # Pass PLEXOSPropertyValue directly instead of dict
    prop = PLEXOSPropertyValue.from_records(
        [
            {"scenario": "Base", "value": 100},
            {"scenario": "High", "value": 120},
        ]
    )
    gen = Generator(max_capacity=prop, min_capacity=50.0)
    with scenario_priority({"High": 1, "Base": 2}):
        assert gen.max_capacity == 120.0
        assert gen.min_capacity == 50.0


def test_auto_resolve_single_scenario():
    # Pass PLEXOSPropertyValue directly instead of dict
    prop = PLEXOSPropertyValue.from_records([{"scenario": "Base", "value": 100}])
    gen = Generator(max_capacity=prop, min_capacity=50.0)
    assert gen.max_capacity == 100.0


def test_auto_resolve_preserves_property_access():
    # Pass PLEXOSPropertyValue directly instead of dict
    prop = PLEXOSPropertyValue.from_records(
        [
            {"scenario": "Base", "value": 100},
            {"scenario": "High", "value": 120},
        ]
    )
    gen = Generator(max_capacity=prop, min_capacity=50.0)
    assert isinstance(gen.__dict__["max_capacity"], PLEXOSPropertyValue)


def test_regular_basemodel_unchanged():
    class RegularModel(BaseModel):
        value: Annotated[float | int, PLEXOSProperty(units="MW")]

    model = RegularModel(value={"scenarios": {"Base": 100}})
    assert isinstance(model.value, PLEXOSPropertyValue)
