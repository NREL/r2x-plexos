from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from r2x_plexos.models.base import PLEXOSRow
from r2x_plexos.models.context import set_horizon, set_scenario_priority
from r2x_plexos.models.property import PLEXOSPropertyValue
from r2x_plexos.models.property_specification import PropertySpecification
from r2x_plexos.models.utils import get_field_name_by_alias


class PropertyValueModel(BaseModel):
    prop: PLEXOSPropertyValue


class AliasModel(BaseModel):
    max_value: int = 0
    model_config = {"populate_by_name": True}


def test_property_value_custom_schema_validate_and_serialize() -> None:
    model = PropertyValueModel(prop=[{"value": 5.0, "scenario_name": "Base", "band": 1}])  # ty: ignore[invalid-argument-type]
    assert isinstance(model.prop, PLEXOSPropertyValue)
    assert model.prop.get_value() == 5.0

    dumped = model.model_dump()
    assert isinstance(dumped["prop"], list)
    assert dumped["prop"][0]["value"] == 5.0


def test_property_value_custom_schema_legacy_entries_dict() -> None:
    payload: dict[str, Any] = {
        "entries": {
            "legacy": {
                "value": 7.0,
                "scenario_name": "High",
                "band": 1,
                "text": "path.csv",
            }
        },
        "units": "MW",
    }
    model = PropertyValueModel(prop=payload)  # ty: ignore[invalid-argument-type]
    assert isinstance(model.prop, PLEXOSPropertyValue)
    assert model.prop.units == "MW"
    assert model.prop.get_value() == 7.0


def test_property_value_custom_schema_invalid_type() -> None:
    with pytest.raises(ValidationError):
        PropertyValueModel(prop="bad-type")  # ty: ignore[invalid-argument-type]


def test_property_value_priority_and_horizon_resolution() -> None:
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=1.0, scenario="Base", date_from="2024-01-01", date_to="2024-12-31")
    prop.add_entry(value=2.0, scenario="High", date_from="2024-01-01", date_to="2024-12-31")
    prop.add_entry(value=9.0, date_from="2020-01-01", date_to="2020-12-31")

    set_scenario_priority({"Base": 1, "High": 2})
    set_horizon(("2024-06-01", "2024-06-30"))

    assert prop.get_value() == 2.0
    entry = prop.get_entry()
    assert entry is not None
    assert entry.value == 2.0

    set_horizon(("2030-01-01", "2030-01-31"))
    assert prop.get_value() is None


def test_property_value_add_from_db_rows_and_helpers() -> None:
    row = PLEXOSRow(
        value=3.5,
        scenario_name="Base",
        band=2,
        timeslice_name="Peak",
        variable_name="v1",
        variable_id=11,
        action="*",
        text="f.csv",
        text_class_name="Data File",
        units="MW",
    )
    prop = PLEXOSPropertyValue()
    prop.add_from_db_rows(row)

    assert prop.get_bands() == [2]
    assert prop.get_timeslices() == ["Peak"]
    assert prop.get_scenarios() == ["Base"]
    assert prop.has_variable()
    assert prop.has_text()
    assert prop.get_text_value() == "f.csv"


def test_property_value_resolve_variants_and_comparison() -> None:
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=10.0, band=1)
    prop.add_entry(value=20.0, band=2)

    values = prop.get_value()
    assert isinstance(values, dict)
    assert values[1] == 10.0
    assert values[2] == 20.0
    assert prop > 0


def test_property_value_priority_text_and_variable_fallback() -> None:
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=1.0, scenario="Base", text="base.csv", variable_name="base_var", variable_id=1)
    prop.add_entry(value=1.0, scenario="High", text="high.csv", variable_name="high_var", variable_id=2)

    set_scenario_priority({"Base": 1, "High": 2})

    assert prop.get_text_with_priority() == "high.csv"
    var = prop.get_variable_with_priority()
    assert var is not None
    assert var["name"] == "high_var"


def test_property_specification_private_paths() -> None:
    spec = PropertySpecification(units="MW", allow_bands=False, is_enum=True, is_validator=True)

    with pytest.raises(ValueError):
        spec._validate_enum_value(1.25)

    banded = PLEXOSPropertyValue.from_records([{"band": 1, "value": 1}, {"band": 2, "value": 2}])
    with pytest.raises(ValueError):
        spec._validate_bands(banded)

    dict_value = {"value": 2}
    converted = spec._validate_dict(dict_value)
    assert isinstance(converted, PLEXOSPropertyValue)
    assert converted.units == "MW"

    serialized = spec._serialize_property_value(converted, info=None)
    assert isinstance(serialized, list)
    assert serialized[0]["value"] == 2

    assert spec._validate_value(None, info=None) is None  # ty: ignore[invalid-argument-type]
    schema = PropertySpecification.__get_pydantic_json_schema__(None, handler=None)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
    assert schema == {"oneOf": [{"type": "number"}, {"type": "object"}]}


def test_get_field_name_by_alias_no_match() -> None:
    assert get_field_name_by_alias(AliasModel(max_value=1), "does not exist") is None
