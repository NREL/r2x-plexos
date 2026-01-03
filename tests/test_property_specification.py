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
    enum_value: Annotated[int, PLEXOSProperty(is_enum=True)] = 1


def test_property_spec_float_input():
    model = SimpleModel(value=100.0)
    assert model.value == 100.0
    assert isinstance(model.value, float)


def test_property_spec_int_input():
    model = SimpleModel(value=100.0)
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


def test_get_filepath_and_references():
    from r2x_plexos import PLEXOSPropertyValue

    prop_file = PLEXOSPropertyValue.from_records(
        [
            {"value": 1, "text": "file.csv", "text_class_name": "Data File"},
        ]
    )
    assert prop_file.get_filepath() == "file.csv"

    prop_var = PLEXOSPropertyValue.from_records(
        [
            {"value": 2, "variable_name": "var1", "variable_id": 42},
        ]
    )
    assert prop_var.get_variable_reference() == {"name": "var1", "id": 42, "action": None}

    prop_df = PLEXOSPropertyValue.from_records([{"value": 3, "datafile_name": "df1", "datafile_id": 99}])
    assert prop_df.get_datafile_reference() == {"name": "df1", "id": 99}


def test_has_methods():
    from r2x_plexos import PLEXOSPropertyValue

    # has_bands
    prop_bands = PLEXOSPropertyValue.from_records([{"band": 1, "value": 1}, {"band": 2, "value": 2}])
    assert prop_bands.has_bands()

    # has_date_from and has_date_to
    prop_dates = PLEXOSPropertyValue.from_records(
        [{"value": 1, "date_from": "2024-01-01", "date_to": "2024-01-31"}]
    )
    assert prop_dates.has_date_from()
    assert prop_dates.has_date_to()

    # has_scenarios
    prop_scenarios = PLEXOSPropertyValue.from_records([{"scenario": "S1", "value": 1}])
    assert prop_scenarios.has_scenarios()

    # has_timeslices
    prop_timeslices = PLEXOSPropertyValue.from_records([{"timeslice": "T1", "value": 1}])
    assert prop_timeslices.has_timeslices()

    # has_datafile
    prop_datafile = PLEXOSPropertyValue.from_records([{"datafile_name": "df", "value": 1}])
    assert prop_datafile.has_datafile()

    # has_variable
    prop_variable = PLEXOSPropertyValue.from_records([{"variable_name": "v", "value": 1}])
    assert prop_variable.has_variable()

    # has_text
    prop_text = PLEXOSPropertyValue.from_records([{"text": "abc", "value": 1}])
    assert prop_text.has_text()
