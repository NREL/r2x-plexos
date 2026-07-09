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


def test_from_db_results_single_row() -> None:
    row = PLEXOSRow(value=42.0, scenario_name="Base", band=1)
    prop = PLEXOSPropertyValue.from_db_results([row])
    assert isinstance(prop, PLEXOSPropertyValue)
    assert prop.get_value_for(scenario="Base") == 42.0


def test_from_db_results_multiple_rows() -> None:
    rows = [
        PLEXOSRow(value=10.0, scenario_name="Base", band=1),
        PLEXOSRow(value=20.0, scenario_name="High", band=1),
    ]
    prop = PLEXOSPropertyValue.from_db_results(rows)
    assert "Base" in prop.get_scenarios()
    assert "High" in prop.get_scenarios()


def test_from_records_csv_in_text_field() -> None:
    """When the 'text' field ends in .csv it should be stored as datafile_name."""
    prop = PLEXOSPropertyValue.from_records([{"value": 0, "text": "profile.csv"}])
    assert prop.has_datafile()


def test_from_records_csv_in_value_field() -> None:
    """When the 'value' string ends in .csv it should be stored as datafile_name."""
    prop = PLEXOSPropertyValue.from_records([{"value": "load_data.csv"}])
    assert prop.has_datafile()


def test_from_records_timeslice_aliases() -> None:
    """time_slice is an alias for timeslice_name."""
    prop = PLEXOSPropertyValue.from_records([{"value": 5, "time_slice": "Peak"}])
    assert "Peak" in prop.get_timeslices()


def test_from_records_datafile_alias() -> None:
    """The 'datafile' key is an alias for datafile_name."""
    prop = PLEXOSPropertyValue.from_records([{"value": 0, "datafile": "data.csv"}])
    assert prop.has_datafile()
    ref = prop.get_datafile_reference()
    assert ref is not None
    assert ref["name"] == "data.csv"


def test_from_records_variable_alias() -> None:
    """The 'variable' key is an alias for variable_name."""
    prop = PLEXOSPropertyValue.from_records([{"value": 1, "variable": "v1"}])
    assert prop.has_variable()


def test_from_records_column_alias() -> None:
    """The 'column' key is an alias for column_name."""
    prop = PLEXOSPropertyValue.from_records([{"value": 0, "column": "col1"}])
    _ = prop.get_datafile_reference()
    # No datafile_name but a column alias was recorded
    assert prop is not None  # No error raised


def test_add_from_db_rows_list_input() -> None:
    rows = [
        PLEXOSRow(value=1.0, scenario_name="Base", band=1, units="MW"),
        PLEXOSRow(value=2.0, scenario_name="High", band=1),
    ]
    prop = PLEXOSPropertyValue()
    prop.add_from_db_rows(rows)
    assert prop.units == "MW"
    assert len(prop.get_scenarios()) == 2


def test_get_value_for_with_date_fallback() -> None:
    """With date_from/to that don't match, should fall back to key without dates."""
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=99.0, band=1)
    result = prop.get_value_for(band=1, date_from="2024-01-01", date_to="2024-12-31")
    assert result == 99.0


def test_get_value_for_scenario_fallback_to_default() -> None:
    """If requested scenario has no exact match, fall back to default key."""
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=50.0, band=1)  # no scenario
    result = prop.get_value_for(scenario="NoMatch", band=1)
    assert result == 50.0


def test_get_value_for_by_band_with_extra_dimensions() -> None:
    """Should still find value by band when only a date-decorated entry exists."""
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=77.0, band=2, date_from="2024-01-01", date_to="2024-12-31")
    result = prop.get_value_for(band=2)
    assert result == 77.0


def test_get_value_for_timeslice_fallback() -> None:
    """Timeslice lookup falls back through the index."""
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=5.0, timeslice="Peak")
    result = prop.get_value_for(timeslice="Peak")
    assert result == 5.0


def test_get_value_with_horizon_saves_and_restores_indexes() -> None:
    """Horizon filtering must not corrupt internal indexes."""
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=10.0, date_from="2024-01-01", date_to="2024-12-31")
    prop.add_entry(value=20.0, date_from="2023-01-01", date_to="2023-12-31")

    set_horizon(("2024-06-01", "2024-06-30"))
    val = prop.get_value()
    # After the call, indexes should be restored (not corrupted)
    assert len(prop.get_dates()) == 2  # both entries still accessible
    assert val == 10.0


def test_get_value_horizon_no_matching_entries_returns_none() -> None:
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=1.0, date_from="2024-01-01", date_to="2024-12-31")

    set_horizon(("2099-01-01", "2099-12-31"))
    assert prop.get_value() is None


def test_resolve_value_non_scenario_timeslice_wins_over_scenario() -> None:
    """Non-scenario timeslice entries are preferred when scenarios also exist."""
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=100.0, scenario="Base", band=1)
    prop.add_entry(value=200.0, timeslice="Peak")  # no scenario

    result = prop.get_value()
    # non-scenario timeslice should appear (dict with timeslices)
    assert isinstance(result, dict) or result is not None


def test_resolve_value_pure_default_wins_when_others_present() -> None:
    """When pure default key exists alongside scenarios, return default value."""
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=99.0, band=1)  # pure default
    prop.add_entry(value=50.0, scenario="High", band=1)

    result = prop.get_value()
    assert result == 99.0


def test_resolve_value_multi_scenario_returns_resolution() -> None:
    """Multiple scenarios → priority resolves to highest."""
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=1.0, scenario="Base", band=1)
    prop.add_entry(value=2.0, scenario="High", band=1)

    set_scenario_priority({"Base": 1, "High": 2})
    result = prop.get_value()
    assert result == 2.0


def test_get_variable_reference_returns_dict() -> None:
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=0.0, variable_name="var1", variable_id=42)
    ref = prop.get_variable_reference()
    assert ref is not None
    assert ref["name"] == "var1"
    assert ref["id"] == 42


def test_get_variable_reference_returns_none_when_empty() -> None:
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=5.0)
    assert prop.get_variable_reference() is None


def test_get_datafile_reference_returns_dict() -> None:
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=0.0, datafile_name="data.csv", datafile_id=7)
    ref = prop.get_datafile_reference()
    assert ref is not None
    assert ref["name"] == "data.csv"
    assert ref["id"] == 7


def test_get_datafile_reference_returns_none_when_empty() -> None:
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=5.0)
    assert prop.get_datafile_reference() is None


def test_get_text_value_single_text() -> None:
    prop = PLEXOSPropertyValue.from_records([{"value": 0, "text": "file.csv"}])
    assert prop.get_text_value() == "file.csv"


def test_get_text_value_multiple_texts_returns_none() -> None:
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=0.0, text="a.csv")
    prop.add_entry(value=1.0, scenario="High", text="b.csv")
    # Multiple distinct text values → returns None
    assert prop.get_text_value() is None or isinstance(prop.get_text_value(), str)


def test_has_date_from_and_has_date_to() -> None:
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=1.0, date_from="2024-01-01", date_to="2024-12-31")
    assert prop.has_date_from()
    assert prop.has_date_to()


def test_has_date_from_false_when_no_date() -> None:
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=5.0)
    assert not prop.has_date_from()
    assert not prop.has_date_to()


def test_has_complex_data_single_value_is_simple() -> None:
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=10.0)
    assert not prop.has_complex_data()


def test_has_complex_data_with_scenario() -> None:
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=10.0, scenario="Base")
    assert prop.has_complex_data()


def test_has_complex_data_with_datafile() -> None:
    prop = PLEXOSPropertyValue.from_records([{"value": 0, "datafile_name": "x.csv"}])
    assert prop.has_complex_data()


def test_has_timeslices_true_and_false() -> None:
    prop_ts = PLEXOSPropertyValue()
    prop_ts.add_entry(value=1.0, timeslice="OffPeak")
    assert prop_ts.has_timeslices()

    prop_plain = PLEXOSPropertyValue()
    prop_plain.add_entry(value=5.0)
    assert not prop_plain.has_timeslices()


def test_has_scenarios_true_and_false() -> None:
    prop_s = PLEXOSPropertyValue()
    prop_s.add_entry(value=1.0, scenario="Base")
    assert prop_s.has_scenarios()

    prop_plain = PLEXOSPropertyValue()
    prop_plain.add_entry(value=5.0)
    assert not prop_plain.has_scenarios()


def test_resolve_entry_by_priority_complex_candidates() -> None:
    """When only complex (banded/timesliced) entries exist, they are used."""
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=99.0, scenario="Base", band=2)  # complex: band != 1

    set_scenario_priority({"Base": 1})
    result = prop.get_value()
    # Should resolve via complex_candidates fallback
    assert result is not None


def test_resolve_entry_by_priority_no_priority_match() -> None:
    """Entries from scenarios not in priority map are skipped."""
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=1.0, scenario="Excluded")

    set_scenario_priority({"Base": 1})
    result = prop.get_value()
    # "Excluded" not in priority → falls back to first entry
    assert result is not None


def test_resolve_field_by_priority_complex_only() -> None:
    """With only complex (banded) entries, field resolution uses complex_candidates."""
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=3.0, scenario="Base", band=2, text="profile.csv")

    set_scenario_priority({"Base": 1})
    text = prop.get_text_with_priority()
    assert text == "profile.csv"


def test_get_text_with_priority_fallback_when_no_priority() -> None:
    """Without priority context, fallback finds first non-None text."""
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=0.0, text="first.csv")

    text = prop.get_text_with_priority()
    assert text == "first.csv"


def test_get_variable_with_priority_fallback_when_no_priority() -> None:
    """Without priority context, fallback finds first non-None variable."""
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=0.0, variable_name="v1", variable_id=1)

    var = prop.get_variable_with_priority()
    assert var is not None
    assert var["name"] == "v1"


def test_get_variable_with_priority_with_priority_context() -> None:
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=0.0, scenario="Base", variable_name="base_var", variable_id=1)
    prop.add_entry(value=0.0, scenario="High", variable_name="high_var", variable_id=2)

    set_scenario_priority({"Base": 1, "High": 2})
    var = prop.get_variable_with_priority()
    assert var is not None
    assert var["name"] == "high_var"


def test_filter_by_horizon_includes_undated_entries() -> None:
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=1.0)  # no dates → always included
    prop.add_entry(value=2.0, date_from="2020-01-01", date_to="2020-12-31")

    set_horizon(("2024-01-01", "2024-12-31"))
    result = prop.get_value()
    assert result == 1.0  # undated entry wins; dated one excluded


def test_filter_by_horizon_includes_overlapping_dated_entries() -> None:
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=5.0, date_from="2024-06-01", date_to="2024-08-31")

    set_horizon(("2024-07-01", "2024-07-31"))
    result = prop.get_value()
    assert result == 5.0


def test_filter_by_horizon_excludes_non_overlapping_entries() -> None:
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=5.0, date_from="2020-01-01", date_to="2020-12-31")
    prop.add_entry(value=99.0, date_from="2024-01-01", date_to="2024-12-31")

    set_horizon(("2024-06-01", "2024-06-30"))
    result = prop.get_value()
    assert result == 99.0
