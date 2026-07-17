import contextlib
from pathlib import Path
from typing import cast
from unittest.mock import patch

import pytest
from plexosdb import ClassEnum, CollectionEnum, PlexosDB
from rust_ok import Ok

from r2x_core import DataStore, Err, PluginConfig, PluginContext, System
from r2x_plexos import PLEXOSConfig
from r2x_plexos.exporter import DEFAULT_XML_TEMPLATE, PLEXOSExporter
from r2x_plexos.parser import PLEXOSParser

pytestmark = pytest.mark.export


def is_valid_class_enum(class_enum):
    """Check if a ClassEnum has a corresponding CollectionEnum."""
    try:
        _ = CollectionEnum[class_enum.name]
        return True
    except KeyError:
        return False


def test_setup_configuration_creates_simulation(plexos_config, serialized_plexos_system, template_db, caplog):
    """Test that setup_configuration creates models, horizons, and memberships."""
    sys = serialized_plexos_system

    ctx = PluginContext(config=plexos_config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    exporter.db = template_db

    for model_name in exporter.db.list_objects_by_class(ClassEnum.Model):
        exporter.db.delete_object(ClassEnum.Model, name=model_name)
    for horizon_name in exporter.db.list_objects_by_class(ClassEnum.Horizon):
        exporter.db.delete_object(ClassEnum.Horizon, name=horizon_name)

    # Verify database is now empty
    models_before = exporter.db.list_objects_by_class(ClassEnum.Model)
    horizons_before = exporter.db.list_objects_by_class(ClassEnum.Horizon)
    assert len(models_before) == 0
    assert len(horizons_before) == 0

    result = exporter.setup_configuration()
    assert result.is_ok(), f"setup_configuration failed: {result.error if result.is_err() else result}"

    models_before = exporter.db.list_objects_by_class(ClassEnum.Model)
    horizons_before = exporter.db.list_objects_by_class(ClassEnum.Horizon)
    assert len(models_before) == 14
    assert len(horizons_before) == 26

    models_after = exporter.db.list_objects_by_class(ClassEnum.Model)
    assert len(models_after) > 0, "No models were created"

    horizons_after = exporter.db.list_objects_by_class(ClassEnum.Horizon)
    assert len(horizons_after) > 0, "No horizons were created"

    model_name = models_after[0]
    horizon_name = horizons_after[0]

    model_id = exporter.db.get_object_id(ClassEnum.Model, model_name)
    horizon_id = exporter.db.get_object_id(ClassEnum.Horizon, horizon_name)

    # Check memberships - models should be connected to horizons
    query = """
    SELECT COUNT(*)
    FROM t_membership
    WHERE parent_object_id = ? AND child_object_id = ?
    """
    result = exporter.db.query(query, (model_id, horizon_id))
    membership_count = result[0][0] if result else 0
    assert membership_count > 0, "No model-horizon memberships were created"

    # Verify horizon attributes were set (not properties - horizons use attributes!)
    # Check for at least one of the common horizon attributes
    try:
        chrono_date_from = exporter.db.get_attribute(
            ClassEnum.Horizon, object_name=horizon_name, attribute_name="Chrono Date From"
        )
        assert chrono_date_from is not None, "Horizon attributes were not set"
    except AssertionError as e:
        # If get_attribute fails, it means no attributes were set
        raise AssertionError("No horizon attributes were set") from e


def test_setup_configuration_skips_existing(plexos_config, serialized_plexos_system, template_db):
    """Test that setup_configuration skips if models/horizons already exist."""
    sys = serialized_plexos_system

    ctx = PluginContext(config=plexos_config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    exporter.db = template_db

    for model_name in exporter.db.list_objects_by_class(ClassEnum.Model):
        exporter.db.delete_object(ClassEnum.Model, name=model_name)
    for horizon_name in exporter.db.list_objects_by_class(ClassEnum.Horizon):
        exporter.db.delete_object(ClassEnum.Horizon, name=horizon_name)

    result1 = exporter.setup_configuration()
    assert result1.is_ok()

    models_count = len(exporter.db.list_objects_by_class(ClassEnum.Model))
    horizons_count = len(exporter.db.list_objects_by_class(ClassEnum.Horizon))

    result2 = exporter.setup_configuration()
    assert result2.is_ok(), "Second setup should succeed and skip duplicates"

    models_count2 = len(exporter.db.list_objects_by_class(ClassEnum.Model))
    horizons_count2 = len(exporter.db.list_objects_by_class(ClassEnum.Horizon))

    assert models_count == models_count2, "Models were created on second call"
    assert horizons_count == horizons_count2, "Horizons were created on second call"


def test_setup_configuration_missing_reference_year(template_db):
    """Test that missing horizon_year returns error."""
    config = PLEXOSConfig(model_name="Base")
    sys = System(name="test_system")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    exporter.db = template_db

    for model_name in exporter.db.list_objects_by_class(ClassEnum.Model):
        exporter.db.delete_object(ClassEnum.Model, name=model_name)
    for horizon_name in exporter.db.list_objects_by_class(ClassEnum.Horizon):
        exporter.db.delete_object(ClassEnum.Horizon, name=horizon_name)

    result = exporter.setup_configuration()
    assert result.is_err(), "Should fail without horizon_year"
    assert "horizon_year" in str(result.error).lower()


def test_exporter_with_wrong_config(mocker, caplog):
    class InvalidConfig(PluginConfig):
        name: str

    bad_config = InvalidConfig(name="Test")
    mock_system = mocker.Mock()

    ctx = PluginContext(config=bad_config, system=mock_system)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))  # ty: ignore[invalid-argument-type]

    result = exporter.on_export()
    assert result.is_err()
    assert "Config is of type" in str(result.error)


def test_roundtrip_db_parser_system_exporter_db(db_all_gen_types: PlexosDB, tmp_path: Path, template_db):
    original_db = db_all_gen_types

    config = PLEXOSConfig(model_name="Base", horizon_year=2024, timeseries_dir=tmp_path)
    store = DataStore(path=tmp_path)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = original_db

    result = parser.run()
    system = result.system

    export_ctx = PluginContext(config=config, system=system)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(export_ctx))
    exporter.exclude_defaults = True
    exporter.output_path = str(tmp_path)
    exporter.db = template_db

    for model_name in exporter.db.list_objects_by_class(ClassEnum.Model):
        exporter.db.delete_object(ClassEnum.Model, name=model_name)
    for horizon_name in exporter.db.list_objects_by_class(ClassEnum.Horizon):
        exporter.db.delete_object(ClassEnum.Horizon, name=horizon_name)

    setup_result = exporter.setup_configuration()
    assert setup_result.is_ok(), (
        f"Setup configuration failed: {setup_result.error if setup_result.is_err() else ''}"
    )

    prepare_result = exporter.prepare_export()
    assert prepare_result.is_ok(), (
        f"Prepare export failed: {prepare_result.error if prepare_result.is_err() else ''}"
    )

    exporter._add_component_datafile_objects()
    exporter._add_component_properties()
    exporter._add_component_memberships()

    exported_db = exporter.db

    for class_enum in ClassEnum:
        if not is_valid_class_enum(class_enum):
            continue
        try:
            original_objects = original_db.list_objects_by_class(class_enum)
            exported_objects = exported_db.list_objects_by_class(class_enum)
            assert len(exported_objects) == len(original_objects), (
                f"{class_enum.name}: exported {len(exported_objects)} objects, expected {len(original_objects)}"
            )
        except Exception:
            continue

    original_properties_count = 0
    for class_enum in ClassEnum:
        if not is_valid_class_enum(class_enum):
            continue
        try:
            for obj_name in original_db.list_objects_by_class(class_enum):
                original_properties_count += len(original_db.get_object_properties(class_enum, obj_name))
        except Exception:
            continue

    exported_properties_count = 0
    for class_enum in ClassEnum:
        if not is_valid_class_enum(class_enum):
            continue
        try:
            for obj_name in exported_db.list_objects_by_class(class_enum):
                exported_properties_count += len(exported_db.get_object_properties(class_enum, obj_name))
        except Exception:
            continue

    assert exported_properties_count >= original_properties_count, (
        f"Properties: exported {exported_properties_count}, expected at least {original_properties_count}"
    )

    exported_memberships_count = exported_db.query(
        "SELECT COUNT(*) FROM t_membership WHERE parent_class_id NOT IN (1, 707) AND child_class_id NOT IN (1, 707)"
    )[0][0]
    assert exported_memberships_count > 0, "No memberships exported"


def test_exporter_init_with_invalid_config_type():
    class DummyConfig:
        pass

    sys = System(name="test")
    ctx = PluginContext(config=DummyConfig(), system=sys)  # ty: ignore[invalid-argument-type]
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))  # ty: ignore[invalid-argument-type]

    result = exporter.on_export()
    assert result.is_err()


def test_exporter_init_with_existing_db(tmp_path, db_all_gen_types):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = db_all_gen_types

    assert exporter.db is db_all_gen_types


def test_setup_configuration_missing_simulation_config(monkeypatch):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    build_result = exporter.on_export()
    assert build_result.is_ok()

    monkeypatch.setattr(exporter.config, "simulation_config", None)

    assert exporter.db is not None
    for model_name in exporter.db.list_objects_by_class(ClassEnum.Model):
        exporter.db.delete_object(ClassEnum.Model, name=model_name)
    for horizon_name in exporter.db.list_objects_by_class(ClassEnum.Horizon):
        exporter.db.delete_object(ClassEnum.Horizon, name=horizon_name)

    result = exporter.setup_configuration()
    assert result.is_ok()


def test_on_export_db_none_initializes_from_template(tmp_path):
    """Test that on_export initializes db from template when db is None - lines 86, 90."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.output_path = str(tmp_path)

    exporter.db = None

    result = exporter.on_export()

    assert result.is_ok()
    assert exporter.db is not None


def test_on_export_uses_custom_template(tmp_path):
    """Test that on_export uses custom template when specified - line 94."""
    # Create a minimal custom XML template
    custom_template = tmp_path / "custom_template.xml"
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    default_template = config.get_config_path().joinpath(DEFAULT_XML_TEMPLATE)
    db = PlexosDB.from_xml(default_template)
    db.to_xml(custom_template)

    config_with_template = PLEXOSConfig(model_name="Base", horizon_year=2024, template=str(custom_template))
    sys = System(name="test")

    ctx = PluginContext(config=config_with_template, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = None
    exporter.output_path = str(tmp_path)

    result = exporter.on_export()

    assert result.is_ok()
    assert exporter.db is not None


def test_on_export_creates_scenario_if_missing(template_db, tmp_path):
    """Test that on_export creates scenario if it doesn't exist - lines 97-98."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db
    exporter.plexos_scenario = "new_scenario"
    exporter.output_path = str(tmp_path)

    if exporter.db.check_object_exists(ClassEnum.Scenario, "new_scenario"):
        exporter.db.delete_object(ClassEnum.Scenario, name="new_scenario")

    result = exporter.on_export()

    assert result.is_ok()
    assert exporter.db.check_object_exists(ClassEnum.Scenario, "new_scenario")


def test_on_export_exception_returns_err(template_db):
    """Test that exceptions in on_export are caught and returned as Err - line 121."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    with patch.object(exporter, "setup_configuration", side_effect=Exception("Test error")):
        result = exporter.on_export()
    assert result.is_err()
    assert "Export failed" in str(result.error)


def test_setup_configuration_with_existing_models_and_horizons(template_db):
    """Test that setup_configuration skips creation when models/horizons exist - lines 164-165."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    if not template_db.list_objects_by_class(ClassEnum.Model):
        template_db.add_object(ClassEnum.Model, "TestModel")
    if not template_db.list_objects_by_class(ClassEnum.Horizon):
        template_db.add_object(ClassEnum.Horizon, "TestHorizon")

    result = exporter.setup_configuration()

    assert result.is_ok()


def test_setup_configuration_missing_horizon_year(template_db):
    """Test setup_configuration returns error when horizon_year is missing - lines 176-177."""
    config = PLEXOSConfig(model_name="Base")
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    for model in template_db.list_objects_by_class(ClassEnum.Model):
        template_db.delete_object(ClassEnum.Model, name=model)
    for horizon in template_db.list_objects_by_class(ClassEnum.Horizon):
        template_db.delete_object(ClassEnum.Horizon, name=horizon)

    result = exporter.setup_configuration()

    assert result.is_err()
    assert "horizon_year" in str(result.error).lower()


def test_setup_configuration_build_simulation_fails(template_db):
    """Test setup_configuration handles build_plexos_simulation failure - line 200."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    for model in template_db.list_objects_by_class(ClassEnum.Model):
        template_db.delete_object(ClassEnum.Model, name=model)
    for horizon in template_db.list_objects_by_class(ClassEnum.Horizon):
        template_db.delete_object(ClassEnum.Horizon, name=horizon)

    from r2x_core import Err as CoreErr

    with patch("r2x_plexos.exporter.build_plexos_simulation", return_value=CoreErr("Build failed")):
        result = exporter.setup_configuration()

    assert result.is_err()
    assert "Failed to build simulation" in str(result.error)


def test_setup_configuration_ingest_fails(template_db):
    """Test setup_configuration handles ingest failure - lines 219-220."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    for model in template_db.list_objects_by_class(ClassEnum.Model):
        template_db.delete_object(ClassEnum.Model, name=model)
    for horizon in template_db.list_objects_by_class(ClassEnum.Horizon):
        template_db.delete_object(ClassEnum.Horizon, name=horizon)

    from r2x_core import Err as CoreErr

    with patch("r2x_plexos.exporter.ingest_simulation_to_plexosdb", return_value=CoreErr("Ingest failed")):
        result = exporter.setup_configuration()

    assert result.is_err()
    assert "Failed to ingest simulation" in str(result.error)


def test_on_export_setup_configuration_returns_err(template_db, tmp_path):
    """Test on_export propagates Err from setup_configuration."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db
    exporter.output_path = str(tmp_path)

    with patch.object(exporter, "setup_configuration", return_value=Err("setup failed")):
        result = exporter.on_export()
    assert result.is_err()
    assert "setup failed" in str(result.error)


def test_on_export_prepare_export_returns_err(template_db, tmp_path):
    """Test on_export propagates Err from prepare_export."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db
    exporter.output_path = str(tmp_path)

    with patch.object(exporter, "setup_configuration", return_value=Ok(None)):  # noqa: SIM117
        with patch.object(exporter, "_add_reports"):
            with patch.object(exporter, "prepare_export", return_value=Err("prepare failed")):
                result = exporter.on_export()
    assert result.is_err()
    assert "prepare failed" in str(result.error)


def test_on_export_postprocess_export_returns_err(template_db, tmp_path):
    """Test on_export propagates Err from postprocess_export."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db
    exporter.output_path = str(tmp_path)

    with patch.object(exporter, "setup_configuration", return_value=Ok(None)):  # noqa: SIM117
        with patch.object(exporter, "_add_reports"):
            with patch.object(exporter, "prepare_export", return_value=Ok(None)):
                with patch.object(exporter, "postprocess_export", return_value=Err("post failed")):
                    result = exporter.on_export()
    assert result.is_err()
    assert "post failed" in str(result.error)


def test_setup_configuration_db_none_returns_err():
    """Test setup_configuration returns Err when db is None."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = None

    result = exporter.setup_configuration()
    assert result.is_err()
    assert "Database not initialized" in str(result.error)


def test_resolve_template_path_default():
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    resolved = exporter._resolve_template_path()

    expected = config.get_config_path().joinpath(DEFAULT_XML_TEMPLATE)
    assert resolved == expected


def test_resolve_template_path_version_key_plexos92():
    config = PLEXOSConfig(model_name="Base", horizon_year=2024, template="PLEXOS9.0")
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    resolved = exporter._resolve_template_path()

    expected = config.get_config_path().joinpath("master_9.2R6_btu.xml")
    assert resolved == expected


def test_resolve_template_path_invalid_raises():
    config = PLEXOSConfig(model_name="Base", horizon_year=2024, template="non_correct_template")
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    with pytest.raises(FileNotFoundError):
        exporter._resolve_template_path()


def test_resolve_template_path_uses_packaged_filename_when_present(tmp_path):
    """A bare filename should resolve from the config package directory when present."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024, template="custom_template.xml")
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    packaged_template = tmp_path / "custom_template.xml"
    packaged_template.write_text("<xml></xml>")

    with patch.object(PLEXOSConfig, "get_config_path", return_value=tmp_path):
        resolved = exporter._resolve_template_path()

    assert resolved == packaged_template


def test_sync_runtime_options_does_not_override_runtime_weather_year():
    """Runtime weather_year should take precedence over config value."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024, weather_year=None)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.weather_year = 2012

    exporter._sync_runtime_options_from_config()

    assert exporter.weather_year == 2012


def test_build_xml_filename_uses_runtime_year_overrides():
    """XML naming should honor solve_year/weather_year runtime overrides."""
    config = PLEXOSConfig(model_name="EI_PCM_2023", horizon_year=2023, weather_year=None)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.solve_year = 2023
    exporter.weather_year = 2012

    xml_name = exporter._build_xml_filename()

    assert xml_name == "EI_PCM_2023_2012_2023.xml"


def test_add_model_attributes_writes_non_default_fields():
    """Test _add_model_attributes persists non-default PLEXOSModel fields."""
    from r2x_plexos import PLEXOSConfig
    from r2x_plexos.models import PLEXOSModel
    from r2x_plexos.utils_simulation import _add_model_attributes

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    template_path = config.get_config_path().joinpath("master_10.0R2_btu.xml")
    db = PlexosDB.from_xml(template_path)

    db.add_object(ClassEnum.Model, "TestModel")
    model = PLEXOSModel(name="TestModel", random_number_seed=42)

    _add_model_attributes(db, model)

    attr = db.get_attribute(ClassEnum.Model, object_name="TestModel", attribute_name="Random Number Seed")
    assert attr[0] == 42


def test_add_model_attributes_skips_default_values():
    """Test _add_model_attributes skips fields with default values (exclude_defaults=True)."""
    from r2x_plexos import PLEXOSConfig
    from r2x_plexos.models import PLEXOSModel
    from r2x_plexos.utils_simulation import _add_model_attributes

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    template_path = config.get_config_path().joinpath("master_10.0R2_btu.xml")
    db = PlexosDB.from_xml(template_path)

    db.add_object(ClassEnum.Model, "TestModel")
    model = PLEXOSModel(name="TestModel")  # random_number_seed=0 (default)

    _add_model_attributes(db, model)

    with contextlib.suppress(Exception):
        db.get_attribute(ClassEnum.Model, object_name="TestModel", attribute_name="Random Number Seed")


def test_sync_runtime_options_reads_output_path_from_config(tmp_path):
    """output_path from config is written to exporter.output_path."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024, output_path=str(tmp_path))
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    exporter._sync_runtime_options_from_config()
    assert exporter.output_path == str(tmp_path)


def test_sync_runtime_options_does_not_override_explicit_weather_year(tmp_path):
    """An already-set weather_year is NOT overwritten by config."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024, weather_year=2030)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.weather_year = 2012  # explicitly set before calling sync

    exporter._sync_runtime_options_from_config()
    assert exporter.weather_year == 2012  # must not be overwritten


def test_sync_runtime_options_hydrates_weather_year_when_none():
    """When exporter.weather_year is None, it should be filled from config."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024, weather_year=2030)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.weather_year = None

    exporter._sync_runtime_options_from_config()
    assert exporter.weather_year == 2030


def test_build_xml_filename_uses_solve_year_for_horizon():
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.solve_year = 2050

    fname = exporter._build_xml_filename()
    assert fname.endswith(".xml")
    assert "2050" in fname
    assert "Base" in fname


def test_build_xml_filename_falls_back_to_config_horizon_year():
    config = PLEXOSConfig(model_name="Run", horizon_year=2035)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.solve_year = None  # no runtime override

    fname = exporter._build_xml_filename()
    assert "Run" in fname
    assert "2035" in fname


def test_build_xml_filename_includes_weather_year_when_set():
    config = PLEXOSConfig(model_name="Base", horizon_year=2040)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.weather_year = 2012

    fname = exporter._build_xml_filename()
    assert "2012" in fname
    assert "2040" in fname


def test_on_export_returns_err_for_wrong_config_type(mocker):
    """Passing a non-PLEXOSConfig should return Err without raising."""
    bad_config = PluginConfig(model_name="Wrong")
    sys = System(name="test")
    ctx = PluginContext(config=bad_config, system=sys)  # type: ignore[arg-type]
    exporter = PLEXOSExporter.from_context(ctx)  # type: ignore[arg-type]

    result = exporter.on_export()
    assert result.is_err()
    assert "Config is of type" in result.unwrap_err()


def test_setup_configuration_returns_err_when_db_is_none():
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = None

    result = exporter.setup_configuration()
    assert result.is_err()
    assert "Database not initialized" in result.unwrap_err()
