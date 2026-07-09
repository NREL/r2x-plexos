"""Tests for PropertySpec validator."""

from typing import Annotated

import pytest
from pydantic import BaseModel, ValidationError

from r2x_plexos import PLEXOSProperty, PLEXOSPropertyValue
from r2x_plexos.models.property_specification import PropertySpecification


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
    model = SimpleModel(value=prop)  # ty: ignore[invalid-argument-type]
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
    model = SimpleModel(value=prop)  # ty: ignore[invalid-argument-type]
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
    model = SimpleModel(value=prop)  # ty: ignore[invalid-argument-type]
    assert isinstance(model.value, PLEXOSPropertyValue)
    assert model.value.get_bands() == [1, 2]


def test_property_spec_units_injection():
    model = SimpleModel(value={"scenario": "Base", "value": 100})  # ty: ignore[invalid-argument-type]
    assert model.value.units == "MW"  # ty: ignore[unresolved-attribute]


def test_property_spec_units_not_overridden():
    model = SimpleModel(value={"scenario": "Base", "value": 100, "units": "kW"})  # ty: ignore[invalid-argument-type]
    assert model.value.units == "kW"  # ty: ignore[unresolved-attribute]


def test_property_spec_no_bands_allows_single_band():
    model = BandedModel(allowed=100.0, no_bands={"scenario": "Base", "value": 2.5})  # ty: ignore[invalid-argument-type]
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
        BandedModel(allowed=100.0, no_bands=prop)  # ty: ignore[invalid-argument-type]


def test_property_spec_plexos_property_input():
    prop = PLEXOSPropertyValue.from_records([{"scenario": "Base", "value": 100}], units="kW")
    model = SimpleModel(value=prop)  # ty: ignore[invalid-argument-type]
    assert isinstance(model.value, PLEXOSPropertyValue)
    assert model.value.units == "kW"


def test_property_spec_plexos_property_units_injection():
    prop = PLEXOSPropertyValue.from_records([{"scenario": "Base", "value": 100}])
    model = SimpleModel(value=prop)  # ty: ignore[invalid-argument-type]
    assert model.value.units == "MW"  # ty: ignore[unresolved-attribute]


def test_property_spec_invalid_type():
    with pytest.raises(ValidationError):
        SimpleModel(value="not a number")  # ty: ignore[invalid-argument-type]


def test_get_filepath_and_references():
    from r2x_plexos import PLEXOSPropertyValue

    prop_file = PLEXOSPropertyValue.from_records(
        [
            {"value": 1, "text": "file.csv", "text_class_name": "Data File"},
        ]
    )
    assert prop_file.get_filepath() == "file.csv"


def test_validate_enum_value_integer_passes():
    spec = PropertySpecification(is_enum=True)
    spec._validate_enum_value(3)  # integer, should not raise


def test_validate_enum_value_whole_float_passes():
    spec = PropertySpecification(is_enum=True)
    spec._validate_enum_value(3.0)  # .is_integer() is True, should not raise


def test_validate_enum_value_non_integer_float_raises():
    spec = PropertySpecification(is_enum=True)
    with pytest.raises(ValueError, match="Enum field requires whole number"):
        spec._validate_enum_value(3.5)


def test_validate_bands_single_band_ok():
    spec = PropertySpecification(allow_bands=False, is_validator=True)
    prop = PLEXOSPropertyValue.from_records([{"band": 1, "value": 10}])
    spec._validate_bands(prop)  # should not raise


def test_validate_bands_multi_band_raises():
    spec = PropertySpecification(allow_bands=False, is_validator=True)
    prop = PLEXOSPropertyValue.from_records([{"band": 1, "value": 10}, {"band": 2, "value": 20}])
    with pytest.raises(ValueError, match="Multi-band"):
        spec._validate_bands(prop)


def test_apply_units_to_dict_without_units():
    spec = PropertySpecification(units="MW")
    d: dict = {"value": 100}
    spec._apply_units(d)
    assert d["units"] == "MW"


def test_apply_units_to_dict_with_existing_units():
    spec = PropertySpecification(units="MW")
    d: dict = {"value": 100, "units": "kW"}
    spec._apply_units(d)
    assert d["units"] == "kW"  # should NOT override


def test_apply_units_to_plexos_property_value_without_units():
    spec = PropertySpecification(units="MW")
    prop = PLEXOSPropertyValue.from_records([{"value": 100}])
    prop.units = None  # ensure no units set
    spec._apply_units(prop)
    assert prop.units == "MW"


def test_apply_units_no_units_configured_is_noop():
    spec = PropertySpecification(units=None)
    d: dict = {"value": 100}
    spec._apply_units(d)
    assert "units" not in d


def test_validate_value_list_input():
    """A list of record dicts should be converted to PLEXOSPropertyValue."""
    from typing import Annotated

    from pydantic import BaseModel

    class M(BaseModel):
        val: Annotated[float | int, PropertySpecification(units="MW")]

    records = [{"value": 50, "scenario": "Base"}]
    m = M(val=records)  # type: ignore[arg-type]
    assert isinstance(m.val, PLEXOSPropertyValue)


def test_validate_value_unsupported_type_raises():
    """Passing an unsupported type raises TypeError (converted to ValidationError by Pydantic)."""
    from typing import Annotated

    from pydantic import BaseModel, ValidationError

    class M(BaseModel):
        val: Annotated[float | int, PropertySpecification()]

    with pytest.raises(ValidationError):
        M(val=object())  # type: ignore[arg-type]


def test_property_spec_enum_with_integer_via_pydantic():
    """is_enum=True with a whole-number float passes validation."""
    model = BandedModel(allowed=100.0, no_bands=2.0, enum_value=2)
    assert model.enum_value == 2


def test_property_spec_helper_returns_specification():
    from r2x_plexos.models.property_specification import _property_spec

    spec = _property_spec(units="MW", allow_bands=True, is_enum=False)
    assert isinstance(spec, PropertySpecification)
    assert spec.units == "MW"
    assert spec.allow_bands is True
    assert spec.is_enum is False


def test_property_spec_helper_defaults():
    from r2x_plexos.models.property_specification import _property_spec

    spec = _property_spec()
    assert spec.units is None
    assert spec.allow_bands is True
    assert spec.is_enum is False

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


def test_property_spec_list_deserialization_applies_units():
    model = SimpleModel(value=[{"scenario": "Base", "value": 100}])
    assert isinstance(model.value, PLEXOSPropertyValue)
    assert model.value.units == "MW"


def test_property_spec_enum_rejects_non_integer_in_property_value():
    class EnumModel(BaseModel):
        value: Annotated[int, PLEXOSProperty(is_enum=True)]

    prop = PLEXOSPropertyValue.from_records([{"value": 1.5}])
    with pytest.raises(ValidationError):
        EnumModel(value=prop)


def test_property_spec_serialize_none_and_passthrough():
    spec = PropertySpecification()
    assert spec._serialize_property_value(None, None) is None
    assert spec._serialize_property_value("plain", None) == "plain"


def test_property_spec_validate_value_unsupported_type_raises():
    spec = PropertySpecification()
    with pytest.raises(TypeError):
        spec._validate_value(object(), None)
