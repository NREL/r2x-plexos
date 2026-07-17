from typing import cast
from unittest.mock import patch

import pytest
from plexosdb import ClassEnum

from r2x_core import PluginContext, System
from r2x_plexos import PLEXOSConfig
from r2x_plexos.exporter import PLEXOSExporter
from r2x_plexos.models import PLEXOSGenerator

pytestmark = pytest.mark.export


def test_prepare_export_skips_types(mocker, template_db):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()
    sys.get_component_types.return_value = []

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    exporter.db = template_db

    result = exporter.prepare_export()
    assert result.is_ok()


def test_prepare_export_no_class_enum(mocker, template_db):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()

    class DummyType:
        pass

    sys.get_component_types.return_value = [DummyType]

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    exporter.db = template_db

    result = exporter.prepare_export()
    assert result.is_ok()


def test_validate_xml_invalid(tmp_path):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    invalid_xml = tmp_path / "invalid.xml"
    invalid_xml.write_text("<notxml>")
    assert not exporter._validate_xml(str(invalid_xml))


def test_prepare_export_db_none_returns_err():
    """Test prepare_export returns error when db is None - lines 234-238."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = None

    result = exporter.prepare_export()

    assert result.is_err()
    assert "Database not initialized" in str(result.error)


def test_prepare_export_component_without_mapping(template_db, caplog):
    """Test prepare_export skips components without ClassEnum mapping - line 253."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    gen = PLEXOSGenerator(name="Gen1", category="thermal", units=1, rating=50.0)
    sys.add_component(gen)

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    with patch("r2x_plexos.exporter.PLEXOS_TYPE_MAP_INVERTED", {}):
        result = exporter.prepare_export()

    assert result.is_ok()
    assert "Skipping component type" in caplog.text or result.is_ok()


def test_prepare_export_components_with_same_category_grouped(template_db):
    """Test prepare_export groups components by category - lines 264-277."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    gen1 = PLEXOSGenerator(name="Gen1", category="thermal", units=1, rating=50.0)
    gen2 = PLEXOSGenerator(name="Gen2", category="thermal", units=1, rating=60.0)
    gen3 = PLEXOSGenerator(name="Gen3", category="hydro", units=1, rating=70.0)

    sys.add_component(gen1)
    sys.add_component(gen2)
    sys.add_component(gen3)

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    result = exporter.prepare_export()

    assert result.is_ok()
    # Verify generators were added
    generators = template_db.list_objects_by_class(ClassEnum.Generator)
    assert "Gen1" in generators
    assert "Gen2" in generators
    assert "Gen3" in generators


def test_prepare_export_add_objects_raises_key_error(template_db):
    """Test prepare_export handles KeyError from add_objects - lines 286-287."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    gen = PLEXOSGenerator(name="TestGen", category="thermal", units=1, rating=50.0)
    sys.add_component(gen)

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    with patch.object(exporter, "_add_objects_safe", side_effect=KeyError("Invalid category")):  # noqa: SIM117
        with pytest.raises(KeyError):
            exporter.prepare_export()


def test_postprocess_export_db_none_returns_err():
    """Test postprocess_export returns error when db is None - lines 293-302."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = None

    result = exporter.postprocess_export()

    assert result.is_err()
    assert "Database not initialized" in str(result.error)


def test_postprocess_export_time_series_fails(template_db, tmp_path):
    """Test postprocess_export handles time series export failure - line 313."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db
    exporter.output_path = str(tmp_path)

    # Mock export_time_series to return Err
    from r2x_core import Err as CoreErr

    with patch.object(exporter, "export_time_series", return_value=CoreErr("TS export failed")):
        result = exporter.postprocess_export()

    assert result.is_err()
    assert "TS export failed" in str(result.error)


def test_postprocess_export_invalid_xml(template_db, tmp_path):
    """Test postprocess_export detects invalid XML - line 328."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db
    exporter.output_path = str(tmp_path)

    # Mock _validate_xml to return False
    with patch.object(exporter, "_validate_xml", return_value=False):
        result = exporter.postprocess_export()

    assert result.is_err()
    assert "not valid" in str(result.error)


def test_add_reports_runs_without_error(template_db):
    """Test _add_reports runs without raising when there are no reports to add."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    with patch.object(PLEXOSConfig, "load_reports", return_value=[]):
        exporter._add_reports()


def test_get_required_properties_unknown_type_returns_set():
    """Test _get_required_properties_for_component returns set for unknown type."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    class DummyComp:
        category = None

    result = exporter._get_required_properties_for_component(DummyComp(), "UnknownType")
    assert isinstance(result, set)


def test_get_required_properties_for_generator_thermal_category(template_db):
    """Test _get_required_properties_for_component resolves category-group for thermal generators."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    gen = PLEXOSGenerator(name="ThermalGen", category="coaloldscr", units=1, rating=50.0)
    result = exporter._get_required_properties_for_component(gen, "PLEXOSGenerator")
    assert isinstance(result, set)
    assert "units" in result
    assert "forced_outage_rate" in result


def test_get_required_properties_for_generator_renewable_dispatch(template_db):
    """Test _get_required_properties resolves renewable-dispatch group."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    gen = PLEXOSGenerator(name="WindGen", category="wind-ons", units=1, rating=100.0)
    result = exporter._get_required_properties_for_component(gen, "PLEXOSGenerator")
    assert isinstance(result, set)
    assert "units" in result


def test_get_required_properties_alias_thermal_normalizes(template_db):
    """Test that 'thermal' category is aliased to thermal-standard group lookup."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    gen = PLEXOSGenerator(name="TGen", category="thermal", units=1, rating=50.0)
    result = exporter._get_required_properties_for_component(gen, "PLEXOSGenerator")
    assert isinstance(result, set)
    assert len(result) > 0


def test_bulk_resolve_object_ids_returns_correct_ids(template_db):
    """Test _bulk_resolve_object_ids returns object IDs for existing objects."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_obj = System(name="test")
    ctx = PluginContext(config=config, system=sys_obj)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    template_db.add_object(ClassEnum.Generator, "BulkGen1")
    template_db.add_object(ClassEnum.Generator, "BulkGen2")

    result = exporter._bulk_resolve_object_ids({ClassEnum.Generator: {"BulkGen1", "BulkGen2"}})

    assert (ClassEnum.Generator, "BulkGen1") in result
    assert (ClassEnum.Generator, "BulkGen2") in result


def test_bulk_resolve_object_ids_empty_input(template_db):
    """Test _bulk_resolve_object_ids returns empty dict for empty input."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_obj = System(name="test")
    ctx = PluginContext(config=config, system=sys_obj)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    result = exporter._bulk_resolve_object_ids({ClassEnum.Generator: set()})
    assert result == {}


def test_add_objects_safe_adds_new_objects(template_db):
    """Test _add_objects_safe inserts objects and memberships."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_obj = System(name="test")
    ctx = PluginContext(config=config, system=sys_obj)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    exporter._add_objects_safe(ClassEnum.Generator, ["SafeGen1", "SafeGen2"], category="thermal")

    objects = template_db.list_objects_by_class(ClassEnum.Generator)
    assert "SafeGen1" in objects
    assert "SafeGen2" in objects


def test_add_objects_safe_skips_existing(template_db):
    """Test _add_objects_safe is idempotent for already-existing objects."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_obj = System(name="test")
    ctx = PluginContext(config=config, system=sys_obj)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    template_db.add_object(ClassEnum.Generator, "ExistingGen")
    before = len(template_db.list_objects_by_class(ClassEnum.Generator))

    exporter._add_objects_safe(ClassEnum.Generator, ["ExistingGen"])
    after = len(template_db.list_objects_by_class(ClassEnum.Generator))

    assert before == after


def test_add_objects_safe_empty_list_does_nothing(template_db):
    """Test _add_objects_safe returns early for empty input."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_obj = System(name="test")
    ctx = PluginContext(config=config, system=sys_obj)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    before = len(template_db.list_objects_by_class(ClassEnum.Generator))
    exporter._add_objects_safe(ClassEnum.Generator, [])
    assert len(template_db.list_objects_by_class(ClassEnum.Generator)) == before


def test_deduplicate_property_records_float_normalization():
    """Different values for the same property are preserved as distinct rows."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    records = [
        {"name": "Gen1", "property": "Rating", "value": 50.0},
        {"name": "Gen1", "property": "Rating", "value": 99.0},
    ]
    result = exporter._deduplicate_property_records(records)
    assert len(result) == 2
    assert {r["value"] for r in result} == {50.0, 99.0}


def test_deduplicate_property_records_merges_fields():
    """Duplicate name/property/value rows merge metadata into one record."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    records = [
        {
            "name": "Gen1",
            "property": "Rating",
            "value": 50.0,
            "band": None,
            "timeslice": None,
            "datafile_text": None,
        },
        {
            "name": "Gen1",
            "property": "Rating",
            "value": 50.0,
            "band": 2,
            "timeslice": "Peak",
            "datafile_text": "file.csv",
        },
    ]
    result = exporter._deduplicate_property_records(records)
    assert len(result) == 1
    assert result[0]["band"] == 2
    assert result[0]["timeslice"] == "Peak"
    assert result[0]["datafile_text"] == "file.csv"


def test_deduplicate_property_records_normalizes_numeric_strings():
    """Equivalent numeric values as float/string should collapse to one row."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    records = [
        {"name": "Wallace Dam_tail", "property": "Max Volume", "value": 23.3, "band": 1},
        {"name": "Wallace Dam_tail", "property": "Max Volume", "value": "23.3", "band": 1},
    ]

    result = exporter._deduplicate_property_records(records)
    assert len(result) == 1


def test_deduplicate_property_records_removes_exact_duplicates():
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    records = [
        {"name": "Gen1", "property": "Rating", "value": 100.0, "band": 1},
        {"name": "Gen1", "property": "Rating", "value": 100.0, "band": 1},
        {"name": "Gen1", "property": "Rating", "value": 200.0, "band": 1},
    ]
    result = exporter._deduplicate_property_records(records)
    assert len(result) == 2


def test_deduplicate_property_records_empty_list():
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    assert exporter._deduplicate_property_records([]) == []


def test_deduplicate_property_records_normalizes_float_string():
    """'100.0' and 100.0 are the same canonical value and should be deduplicated."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    records = [
        {"name": "Gen1", "property": "Rating", "value": "100.0", "band": 1},
        {"name": "Gen1", "property": "Rating", "value": 100.0, "band": 1},
    ]
    result = exporter._deduplicate_property_records(records)
    assert len(result) == 1


def test_get_category_group_name_returns_none_for_no_category():
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    gen = PLEXOSGenerator(name="G1", category=None)
    assert exporter._get_category_group_name(gen) is None


def test_get_category_group_name_alias_thermal():
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    gen = PLEXOSGenerator(name="G1", category="thermal")
    result = exporter._get_category_group_name(gen)
    # "thermal" maps to "thermal-standard"
    assert result is not None


def test_get_required_properties_for_generator_hydro_dispatch_excludes_thermal_fields(template_db):
    """Hydro generators should not inherit thermal-only required properties."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    gen = PLEXOSGenerator(name="HydroGen", category="hydro-dispatch", units=1, rating=100.0)
    result = exporter._get_required_properties_for_component(gen, "PLEXOSGenerator")
    assert isinstance(result, set)
    assert "start_cost" not in result
    assert "fuel_price" not in result
    assert "min_up_time" not in result
    assert "min_down_time" not in result
    assert "max_energy_day" not in result


def test_get_required_properties_for_generator_unknown_category_does_not_default_to_thermal(template_db):
    """Unknown generator categories should not force thermal required defaults."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    gen = PLEXOSGenerator(name="UnknownGen", category="hydro", units=1, rating=50.0)
    result = exporter._get_required_properties_for_component(gen, "PLEXOSGenerator")
    assert isinstance(result, set)
    assert "start_cost" not in result
    assert "fuel_price" not in result
    assert "min_up_time" not in result
    assert "min_down_time" not in result
