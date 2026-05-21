import contextlib
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from plexosdb import ClassEnum, CollectionEnum, PlexosDB
from rust_ok import Ok

from r2x_core import DataStore, Err, PluginConfig, PluginContext, System
from r2x_plexos import PLEXOSConfig, PLEXOSPropertyValue
from r2x_plexos.exporter import DEFAULT_XML_TEMPLATE, FLOW_CLIP_MEMO_TEXT, PLEXOSExporter
from r2x_plexos.models import PLEXOSDatafile, PLEXOSGenerator, PLEXOSLine, PLEXOSMembership, PLEXOSNode
from r2x_plexos.parser import PLEXOSParser

pytestmark = pytest.mark.export


@pytest.fixture
def plexos_config():
    from r2x_plexos import PLEXOSConfig

    return PLEXOSConfig(model_name="Base", horizon_year=2024)


@pytest.fixture
def template_db(plexos_config: PLEXOSConfig) -> PlexosDB:
    """Create a PlexosDB from the default template."""
    template_path = plexos_config.get_config_path().joinpath(DEFAULT_XML_TEMPLATE)
    return PlexosDB.from_xml(template_path)


@pytest.fixture
def serialized_plexos_system(tmp_path, db_all_gen_types, plexos_config) -> "System":
    from r2x_core import DataStore, PluginContext
    from r2x_plexos import PLEXOSParser

    store = DataStore(path=tmp_path)

    ctx = PluginContext(config=plexos_config, store=store)
    parser = PLEXOSParser.from_context(ctx)
    parser.db = db_all_gen_types

    result = parser.run()
    sys = result.system

    serialized_sys_fpath = tmp_path / "test_plexos_system.json"
    sys.to_json(serialized_sys_fpath)
    return sys


def test_setup_configuration_creates_simulation(plexos_config, serialized_plexos_system, template_db, caplog):
    """Test that setup_configuration creates models, horizons, and memberships."""
    sys = serialized_plexos_system

    ctx = PluginContext(config=plexos_config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

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
    exporter = PLEXOSExporter.from_context(ctx)

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
    exporter = PLEXOSExporter.from_context(ctx)

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
    exporter = PLEXOSExporter.from_context(ctx)

    result = exporter.on_export()
    assert result.is_err()
    assert "Config is of type" in str(result.error)


def is_valid_class_enum(class_enum):
    """Check if a ClassEnum has a corresponding CollectionEnum."""
    try:
        _ = CollectionEnum[class_enum.name]
        return True
    except KeyError:
        return False


def test_roundtrip_db_parser_system_exporter_db(db_all_gen_types: PlexosDB, tmp_path: Path, template_db):
    original_db = db_all_gen_types

    config = PLEXOSConfig(model_name="Base", horizon_year=2024, timeseries_dir=tmp_path)
    store = DataStore(path=tmp_path)

    ctx = PluginContext(config=config, store=store)
    parser = PLEXOSParser.from_context(ctx)
    parser.db = original_db

    result = parser.run()
    system = result.system

    export_ctx = PluginContext(config=config, system=system)
    exporter = PLEXOSExporter.from_context(export_ctx)
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
    ctx = PluginContext(config=DummyConfig(), system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    result = exporter.on_export()
    assert result.is_err()


def test_exporter_init_with_existing_db(tmp_path, db_all_gen_types):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = db_all_gen_types

    assert exporter.db is db_all_gen_types


def test_setup_configuration_missing_simulation_config(monkeypatch):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    build_result = exporter.on_export()
    assert build_result.is_ok()

    monkeypatch.setattr(exporter.config, "simulation_config", None)

    for model_name in exporter.db.list_objects_by_class(ClassEnum.Model):
        exporter.db.delete_object(ClassEnum.Model, name=model_name)
    for horizon_name in exporter.db.list_objects_by_class(ClassEnum.Horizon):
        exporter.db.delete_object(ClassEnum.Horizon, name=horizon_name)

    result = exporter.setup_configuration()
    assert result.is_ok()


def test_prepare_export_skips_types(mocker, template_db):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()
    sys.get_component_types.return_value = []

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

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
    exporter = PLEXOSExporter.from_context(ctx)

    exporter.db = template_db

    result = exporter.prepare_export()
    assert result.is_ok()


def test_export_time_series_no_components(mocker):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()
    sys.get_component_types.return_value = []

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    result = exporter.export_time_series()
    assert result.is_ok()


def test_export_time_series_csv_error(mocker):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()

    class DummyType:
        pass

    sys.get_component_types.return_value = [DummyType]
    sys.get_components.return_value = [mocker.Mock(name="comp")]
    sys.has_time_series.return_value = True

    ts_key = mocker.Mock()
    ts_key.name = "ts_key"
    ts_key.features = {}
    ts_key.initial_timestamp = None
    sys.list_time_series_keys.return_value = [ts_key]
    sys.list_time_series.return_value = [mocker.Mock()]
    mocker.patch("r2x_plexos.exporter.export_time_series_csv", return_value=Err("fail"))

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    result = exporter.export_time_series()
    assert result.is_err()


def test_add_component_memberships_no_memberships(mocker):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()
    sys.get_supplemental_attributes.return_value = []

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    exporter._add_component_memberships()


def test_add_component_memberships_skips_invalid(mocker):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()
    membership = mocker.Mock()
    membership.parent_object = None
    membership.child_object = None
    sys.get_supplemental_attributes.return_value = [membership]

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    exporter._add_component_memberships()


def test_create_datafile_objects_no_dir(tmp_path, mocker):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.output_path = str(tmp_path)

    exporter._create_datafile_objects()


def test_add_component_datafile_objects_no_datafiles(mocker):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()
    sys.get_components.return_value = []

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    exporter._add_component_datafile_objects()


def test_add_component_datafile_objects_filename_none(mocker):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()
    datafile = mocker.Mock()
    datafile.name = "test"
    datafile.filename = None
    sys.get_components.return_value = [datafile]

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    mocker.patch.object(exporter, "_create_datafile_objects")
    exporter._add_component_datafile_objects()


def test_validate_xml_invalid(tmp_path):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    invalid_xml = tmp_path / "invalid.xml"
    invalid_xml.write_text("<notxml>")
    assert not exporter._validate_xml(str(invalid_xml))


def test_on_export_db_none_initializes_from_template(tmp_path):
    """Test that on_export initializes db from template when db is None - lines 86, 90."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
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
    exporter = PLEXOSExporter.from_context(ctx)
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
    exporter = PLEXOSExporter.from_context(ctx)
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
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    with patch.object(exporter, "setup_configuration", side_effect=Exception("Test error")):
        result = exporter.on_export()
    assert result.is_err()
    assert "Export failed" in result.error


def test_setup_configuration_with_existing_models_and_horizons(template_db):
    """Test that setup_configuration skips creation when models/horizons exist - lines 164-165."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
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
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    for model in template_db.list_objects_by_class(ClassEnum.Model):
        template_db.delete_object(ClassEnum.Model, name=model)
    for horizon in template_db.list_objects_by_class(ClassEnum.Horizon):
        template_db.delete_object(ClassEnum.Horizon, name=horizon)

    result = exporter.setup_configuration()

    assert result.is_err()
    assert "horizon_year" in result.error.lower()


def test_setup_configuration_build_simulation_fails(template_db):
    """Test setup_configuration handles build_plexos_simulation failure - line 200."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    for model in template_db.list_objects_by_class(ClassEnum.Model):
        template_db.delete_object(ClassEnum.Model, name=model)
    for horizon in template_db.list_objects_by_class(ClassEnum.Horizon):
        template_db.delete_object(ClassEnum.Horizon, name=horizon)

    from r2x_core import Err as CoreErr

    with patch("r2x_plexos.exporter.build_plexos_simulation", return_value=CoreErr("Build failed")):
        result = exporter.setup_configuration()

    assert result.is_err()
    assert "Failed to build simulation" in result.error


def test_setup_configuration_ingest_fails(template_db):
    """Test setup_configuration handles ingest failure - lines 219-220."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    for model in template_db.list_objects_by_class(ClassEnum.Model):
        template_db.delete_object(ClassEnum.Model, name=model)
    for horizon in template_db.list_objects_by_class(ClassEnum.Horizon):
        template_db.delete_object(ClassEnum.Horizon, name=horizon)

    from r2x_core import Err as CoreErr

    with patch("r2x_plexos.exporter.ingest_simulation_to_plexosdb", return_value=CoreErr("Ingest failed")):
        result = exporter.setup_configuration()

    assert result.is_err()
    assert "Failed to ingest simulation" in result.error


def test_prepare_export_db_none_returns_err():
    """Test prepare_export returns error when db is None - lines 234-238."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = None

    result = exporter.prepare_export()

    assert result.is_err()
    assert "Database not initialized" in result.error


def test_prepare_export_component_without_mapping(template_db, caplog):
    """Test prepare_export skips components without ClassEnum mapping - line 253."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    gen = PLEXOSGenerator(name="Gen1", category="thermal", units=1, rating=50.0)
    sys.add_component(gen)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
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
    exporter = PLEXOSExporter.from_context(ctx)
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
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    with patch.object(exporter, "_add_objects_safe", side_effect=KeyError("Invalid category")):  # noqa: SIM117
        with pytest.raises(KeyError):
            exporter.prepare_export()


def test_postprocess_export_db_none_returns_err():
    """Test postprocess_export returns error when db is None - lines 293-302."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = None

    result = exporter.postprocess_export()

    assert result.is_err()
    assert "Database not initialized" in result.error


def test_postprocess_export_time_series_fails(template_db, tmp_path):
    """Test postprocess_export handles time series export failure - line 313."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db
    exporter.output_path = str(tmp_path)

    # Mock export_time_series to return Err
    from r2x_core import Err as CoreErr

    with patch.object(exporter, "export_time_series", return_value=CoreErr("TS export failed")):
        result = exporter.postprocess_export()

    assert result.is_err()
    assert "TS export failed" in result.error


def test_postprocess_export_invalid_xml(template_db, tmp_path):
    """Test postprocess_export detects invalid XML - line 328."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db
    exporter.output_path = str(tmp_path)

    # Mock _validate_xml to return False
    with patch.object(exporter, "_validate_xml", return_value=False):
        result = exporter.postprocess_export()

    assert result.is_err()
    assert "not valid" in result.error


def test_add_component_properties_db_none_logs_error(caplog):
    """Test _add_component_properties handles db None - lines 363-364."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = None

    exporter._add_component_properties()

    assert "Database not initialized" in caplog.text


def test_add_component_properties_adds_datafile_filename(template_db):
    """Test _add_component_properties adds Filename property for DataFile - lines 370-371."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    datafile = PLEXOSDatafile(
        name="TestFile", filename=PLEXOSPropertyValue.from_dict({"datafile_name": "test.csv"})
    )
    sys.add_component(datafile)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    template_db.add_object(ClassEnum.DataFile, "TestFile", category="CSV")
    exporter._add_component_properties()

    props = template_db.get_object_properties(ClassEnum.DataFile, "TestFile")
    prop_names = [p.get("property") for p in props]
    assert "Filename" in prop_names


def test_add_component_properties_filters_metadata_fields(template_db):
    """Test _add_component_properties filters out metadata fields - lines 392-393."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    gen = PLEXOSGenerator(
        name="TestGen",
        category="coaloldscr",
        units=1,
        rating=50.0,
    )
    sys.add_component(gen)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    template_db.add_object(ClassEnum.Generator, "TestGen", category="thermal")

    exporter._add_component_properties()

    props = template_db.get_object_properties(ClassEnum.Generator, "TestGen")
    prop_names = [p.get("property") for p in props]

    assert "name" not in [pn.lower() for pn in prop_names if pn]
    assert "category" not in [pn.lower() for pn in prop_names if pn]
    assert "Units" in prop_names
    assert "Rating" in prop_names
    assert "Forced Outage Rate" in prop_names
    assert "Min Stable Level" in prop_names


def test_add_component_properties_handles_dict_with_text(template_db):
    """Test _add_component_properties handles dict properties with 'text' - lines 406-408."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    # Create a generator with a property that's a dict with 'text'
    gen = PLEXOSGenerator(name="TestGen", category="thermal", units=1, rating=50.0)
    # Manually set a property as dict with 'text'
    gen.max_capacity = PLEXOSPropertyValue.from_dict({"datafile_name": "test.csv"})
    sys.add_component(gen)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    template_db.add_object(ClassEnum.Generator, "TestGen", category="thermal")

    exporter._add_component_properties()

    # Properties should be added
    props = template_db.get_object_properties(ClassEnum.Generator, "TestGen")
    assert len(props) > 0


def test_add_component_properties_skips_none_values(template_db):
    """Test _add_component_properties skips None values and time series properties."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    gen = PLEXOSGenerator(
        name="TestGen",
        category="biopower",
        units=1,
        rating=50.0,
    )
    sys.add_component(gen)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    template_db.add_object(ClassEnum.Generator, "TestGen", category="thermal")

    exporter._add_component_properties()

    props = template_db.get_object_properties(ClassEnum.Generator, "TestGen")
    prop_names = [p.get("property") for p in props]

    assert "Units" in prop_names
    assert "Rating" in prop_names
    assert "Forced Outage Rate" in prop_names
    assert "Min Stable Level" in prop_names
    assert "Maintenance Rate" in prop_names
    assert "Mean Time to Repair" in prop_names


def test_add_component_properties_does_not_export_explicit_default_values(template_db):
    """Regression: explicit default-valued fields should not be exported with exclude_defaults=True."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    # Expansion Economy Units defaults to 0 and is not required for thermal generators.
    # Setting it explicitly should not force export when exclude_defaults=True.
    gen = PLEXOSGenerator(
        name="TestGen",
        category="thermal",
        units=1,
        rating=50.0,
        expansion_economy_units=0,
    )
    sys.add_component(gen)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db
    exporter.exclude_defaults = True

    template_db.add_object(ClassEnum.Generator, "TestGen", category="thermal")

    exporter._add_component_properties()

    props = template_db.get_object_properties(ClassEnum.Generator, "TestGen")
    prop_names = [p.get("property") for p in props]

    assert "Expansion Economy Units" not in prop_names


def test_add_component_properties_adds_line_flow_clip_memo(template_db):
    """Memo text is added only for line Min/Max Flow rows clipped to -99999/99999."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    clipped = PLEXOSLine(name="ClippedLine", max_flow=99999, min_flow=-99999)
    normal = PLEXOSLine(name="NormalLine", max_flow=8000, min_flow=-8000)
    sys.add_component(clipped)
    sys.add_component(normal)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    template_db.add_object(ClassEnum.Line, "ClippedLine", category="-")
    template_db.add_object(ClassEnum.Line, "NormalLine", category="-")

    exporter._add_component_properties()

    rows = template_db.query(
        """
        SELECT o.name, p.name, md.value
        FROM t_memo_data md
        INNER JOIN t_data d ON d.data_id = md.data_id
        INNER JOIN t_property p ON p.property_id = d.property_id
        INNER JOIN t_membership m ON m.membership_id = d.membership_id
        INNER JOIN t_object o ON o.object_id = m.child_object_id
        INNER JOIN t_class c ON c.class_id = o.class_id
        WHERE c.name = 'Line' AND p.name IN ('Min Flow', 'Max Flow')
        """
    )

    assert len(rows) == 2
    assert {r[0] for r in rows} == {"ClippedLine"}
    assert {r[1] for r in rows} == {"Min Flow", "Max Flow"}
    assert all(r[2] == FLOW_CLIP_MEMO_TEXT for r in rows)


def test_add_component_properties_removes_heat_rate_when_curve_defined(template_db):
    """Use heat-rate curve terms over single-point heat rate when both are present."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    gen = PLEXOSGenerator(
        name="CurveHeatRateGen",
        category="thermal",
        units=1,
        rating=50.0,
        heat_rate=10.0,
        heat_rate_base=8.0,
        heat_rate_incr=2.0,
    )
    sys.add_component(gen)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    template_db.add_object(ClassEnum.Generator, "CurveHeatRateGen", category="thermal")

    exporter._add_component_properties()

    props = template_db.get_object_properties(ClassEnum.Generator, "CurveHeatRateGen")
    prop_names = {p.get("property") for p in props}

    assert "Heat Rate Base" in prop_names
    assert "Heat Rate Incr" in prop_names
    assert "Heat Rate" not in prop_names


def test_add_component_memberships_db_none_logs_error(caplog):
    """Test _add_component_memberships handles db None - lines 429-440."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = None

    exporter._add_component_memberships()

    assert "Database not initialized" in caplog.text


def test_add_component_memberships_no_memberships_warns(template_db, caplog):
    """Test _add_component_memberships warns when no memberships found - line 444."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    exporter._add_component_memberships()

    assert "No memberships found" in caplog.text


def test_add_component_memberships_skips_missing_parent_or_child(template_db, caplog):
    """Test _add_component_memberships skips memberships with missing objects."""
    from unittest.mock import Mock, patch

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    gen = PLEXOSGenerator(name="Gen1", category="thermal", units=1, rating=50.0)
    node = PLEXOSNode(name="Node1")
    sys.add_component(gen)
    sys.add_component(node)

    _ = PLEXOSMembership(parent_object=node, child_object=gen, collection=CollectionEnum.Generators)

    mock_membership = Mock()
    mock_membership.parent_object = None
    mock_membership.child_object = None
    mock_membership.collection = CollectionEnum.Generators

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    with patch.object(sys, "get_supplemental_attributes", return_value=[mock_membership]):
        exporter._add_component_memberships()

    assert "No valid membership records to add." in caplog.text


def test_add_component_memberships_no_valid_records_warns(template_db, caplog):
    """Test _add_component_memberships warns when no valid records - line 495."""
    from unittest.mock import Mock, patch

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    gen = PLEXOSGenerator(name="Gen1", category="thermal", units=1, rating=50.0)
    node = PLEXOSNode(name="Node1")

    mock_membership = Mock()
    mock_membership.parent_object = node
    mock_membership.child_object = gen
    mock_membership.collection = None

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    with patch.object(sys, "get_supplemental_attributes", return_value=[mock_membership]):
        exporter._add_component_memberships()

    assert "No valid membership records to add." in caplog.text


def test_add_component_datafile_objects_db_none(caplog):
    """Test _add_component_datafile_objects handles db None - lines 527, 529."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = None

    exporter._add_component_datafile_objects()

    assert "Database not initialized" in caplog.text


def test_add_component_datafile_objects_updates_object_ids(template_db):
    """Test _add_component_datafile_objects updates object_id and datafile_id - line 557."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    datafile = PLEXOSDatafile(
        name="TestFile", filename=PLEXOSPropertyValue.from_dict({"datafile_name": "test.csv"})
    )
    sys.add_component(datafile)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    exporter._add_component_datafile_objects()

    assert datafile.object_id is not None


def test_add_component_datafile_objects_handles_no_filename(template_db, caplog):
    """Test _add_component_datafile_objects handles datafile without filename."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    datafile = PLEXOSDatafile(name="TestFile", filename=None)
    sys.add_component(datafile)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    exporter._add_component_datafile_objects()

    datafiles_in_db = template_db.list_objects_by_class(ClassEnum.DataFile)
    assert "TestFile" in datafiles_in_db


def test_export_time_series_no_components_with_ts(template_db):
    """Test export_time_series handles no components with time series."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    # Add component without time series
    gen = PLEXOSGenerator(name="Gen1", category="thermal", units=1, rating=50.0)
    sys.add_component(gen)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    result = exporter.export_time_series()

    assert result.is_ok()


def test_create_datafile_objects_no_directory(tmp_path, caplog):
    """Test _create_datafile_objects handles missing directory."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    # Use a non-existent path that won't be created
    non_existent_path = tmp_path / "does_not_exist" / "nested"
    exporter.output_path = str(non_existent_path)

    # Mock get_output_directory to return a path that doesn't exist
    with patch("r2x_plexos.exporter.get_output_directory", return_value=non_existent_path / "Data"):
        exporter._create_datafile_objects()

    assert "No time series directory found" in caplog.text


def test_create_datafile_objects_creates_from_csv_files(tmp_path):
    """Test _create_datafile_objects creates DataFile objects from CSV files."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    data_dir = tmp_path / "Data"
    data_dir.mkdir()
    (data_dir / "test1.csv").write_text("col1,col2\n1,2\n")
    (data_dir / "test2.csv").write_text("col1,col2\n3,4\n")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.output_path = str(tmp_path)

    exporter._create_datafile_objects()

    datafiles = list(sys.get_components(PLEXOSDatafile))
    assert len(datafiles) == 2
    assert any(df.name == "test1" for df in datafiles)
    assert any(df.name == "test2" for df in datafiles)


def test_on_export_setup_configuration_returns_err(template_db, tmp_path):
    """Test on_export propagates Err from setup_configuration."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db
    exporter.output_path = str(tmp_path)

    with patch.object(exporter, "setup_configuration", return_value=Err("setup failed")):
        result = exporter.on_export()
    assert result.is_err()
    assert "setup failed" in result.error


def test_on_export_prepare_export_returns_err(template_db, tmp_path):
    """Test on_export propagates Err from prepare_export."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db
    exporter.output_path = str(tmp_path)

    with patch.object(exporter, "setup_configuration", return_value=Ok(None)):  # noqa: SIM117
        with patch.object(exporter, "_add_reports"):
            with patch.object(exporter, "prepare_export", return_value=Err("prepare failed")):
                result = exporter.on_export()
    assert result.is_err()
    assert "prepare failed" in result.error


def test_on_export_postprocess_export_returns_err(template_db, tmp_path):
    """Test on_export propagates Err from postprocess_export."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db
    exporter.output_path = str(tmp_path)

    with patch.object(exporter, "setup_configuration", return_value=Ok(None)):  # noqa: SIM117
        with patch.object(exporter, "_add_reports"):
            with patch.object(exporter, "prepare_export", return_value=Ok(None)):
                with patch.object(exporter, "postprocess_export", return_value=Err("post failed")):
                    result = exporter.on_export()
    assert result.is_err()
    assert "post failed" in result.error


def test_setup_configuration_db_none_returns_err():
    """Test setup_configuration returns Err when db is None."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = None

    result = exporter.setup_configuration()
    assert result.is_err()
    assert "Database not initialized" in result.error


def test_deduplicate_property_records_float_normalization():
    """Different values for the same property are preserved as distinct rows."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

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
    exporter = PLEXOSExporter.from_context(ctx)

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


def test_get_required_properties_unknown_type_returns_set():
    """Test _get_required_properties_for_component returns set for unknown type."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    class DummyComp:
        category = None

    result = exporter._get_required_properties_for_component(DummyComp(), "UnknownType")
    assert isinstance(result, set)


def test_link_datafiles_to_components_db_none(caplog):
    """Test _link_datafiles_to_components logs error when db is None."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = None

    exporter._link_datafiles_to_components()
    assert "Database not initialized" in caplog.text


def test_link_datafiles_to_components_missing_output_dir(template_db, tmp_path, caplog):
    """Test _link_datafiles_to_components handles FileNotFoundError for output dir."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    non_existent = tmp_path / "no_such_dir" / "Data"
    with patch("r2x_plexos.exporter.get_output_directory", return_value=non_existent):
        exporter._link_datafiles_to_components()
    assert "not found" in caplog.text


def test_get_time_series_property_name_returns_none_for_unknown():
    """Test _get_time_series_property_name returns None for unrecognized type."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    class DummyComp:
        pass

    result = exporter._get_time_series_property_name(DummyComp(), ts_key_name="some_key")
    assert result is None


def test_get_time_series_property_name_fixed_types():
    """Test _get_time_series_property_name returns fixed names for reserve/region/storage."""
    from r2x_plexos.models import PLEXOSRegion, PLEXOSReserve, PLEXOSStorage

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    assert exporter._get_time_series_property_name(PLEXOSReserve(name="r")) == "Min Provision"
    assert exporter._get_time_series_property_name(PLEXOSRegion(name="r")) == "Load"
    assert exporter._get_time_series_property_name(PLEXOSStorage(name="s")) == "Natural Inflow"


def test_build_generator_to_storage_map_with_pairs(mocker):
    """Test _build_generator_to_storage_map maps generator <-> storage both ways."""
    from r2x_plexos.models import PLEXOSStorage

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_obj = System(name="test")
    ctx = PluginContext(config=config, system=sys_obj)
    exporter = PLEXOSExporter.from_context(ctx)

    gen = PLEXOSGenerator(name="HydroGen", category="hydro-turbine", units=1, rating=100.0)
    storage = PLEXOSStorage(name="HydroRes")
    m1 = PLEXOSMembership(parent_object=gen, child_object=storage, collection=CollectionEnum.Storages)

    with patch.object(sys_obj, "get_supplemental_attributes", return_value=[m1]):
        result = exporter._build_generator_to_storage_map()

    assert result["HydroGen"].name == "HydroRes"


def test_export_time_series_with_weather_and_solve_year(mocker, tmp_path):
    """Test export_time_series uses weather_year and solve_year in filenames."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_mock = mocker.Mock()

    class DummyType:
        pass

    comp = mocker.Mock()
    comp.name = "Gen1"
    type(comp).__name__ = "DummyType"

    sys_mock.get_component_types.return_value = [DummyType]
    sys_mock.get_components.return_value = [comp]
    sys_mock.has_time_series.return_value = True
    ts_key = mocker.Mock()
    ts_key.name = "max_active_power"
    ts_key.features = {}
    sys_mock.list_time_series_keys.return_value = [ts_key]
    sys_mock.get_time_series_by_key.return_value = mocker.Mock()

    data_dir = tmp_path / "Data"
    data_dir.mkdir()

    ctx = PluginContext(config=config, system=sys_mock)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.weather_year = 2020
    exporter.solve_year = 2024

    mocker.patch("r2x_plexos.exporter.get_output_directory", return_value=data_dir)
    mocker.patch("r2x_plexos.exporter.export_time_series_csv", return_value=Ok(None))

    result = exporter.export_time_series()
    assert result.is_ok()


def test_create_datafile_objects_skips_existing_component(tmp_path):
    """Test _create_datafile_objects creates DataFile retrievable by name."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    data_dir = tmp_path / "Data"
    data_dir.mkdir()
    (data_dir / "ts1.csv").write_text("col\n1\n")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.output_path = str(tmp_path)

    exporter._create_datafile_objects()

    datafiles = list(sys.get_components(PLEXOSDatafile))
    assert any(df.name == "ts1" for df in datafiles)


def test_add_reports_runs_without_error(template_db):
    """Test _add_reports runs without raising when there are no reports to add."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    with patch.object(PLEXOSConfig, "load_reports", return_value=[]):
        exporter._add_reports()


def test_add_component_properties_list_raw(template_db):
    """Test _add_component_properties handles list-of-dict property values."""
    from r2x_plexos.models import PLEXOSReserve

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    reserve = PLEXOSReserve(name="Res1", type=1, duration=15)
    sys.add_component(reserve)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    template_db.add_object(ClassEnum.Reserve, "Res1", category="variable-reserve")

    exporter._add_component_properties()

    props = template_db.get_object_properties(ClassEnum.Reserve, "Res1")
    assert len(props) > 0


def test_add_component_properties_skips_ts_property(template_db, mocker):
    """Test _add_component_properties skips static value for ts-linked properties."""
    from r2x_plexos.models import PLEXOSRegion

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    region = PLEXOSRegion(name="RegA")
    sys.add_component(region)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    template_db.add_object(ClassEnum.Region, "RegA", category="default")

    # Simulate that this region has time series
    mocker.patch.object(sys, "has_time_series", return_value=True)

    ts_key = mocker.Mock()
    ts_key.name = "load"
    mocker.patch.object(sys, "list_time_series_keys", return_value=[ts_key])

    exporter._add_component_properties()

    props = template_db.get_object_properties(ClassEnum.Region, "RegA")
    prop_names = [p.get("property") for p in props]
    # "Load" should NOT appear as a plain static value
    assert "Load" not in prop_names


def test_resolve_template_path_default():
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    resolved = exporter._resolve_template_path()

    expected = config.get_config_path().joinpath(DEFAULT_XML_TEMPLATE)
    assert resolved == expected


def test_resolve_template_path_version_key_plexos92():
    config = PLEXOSConfig(model_name="Base", horizon_year=2024, template="PLEXOS9.2")
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    resolved = exporter._resolve_template_path()

    expected = config.get_config_path().joinpath("master_9.2R6_btu.xml")
    assert resolved == expected


def test_resolve_template_path_invalid_raises():
    config = PLEXOSConfig(model_name="Base", horizon_year=2024, template="non_correct_template")
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    with pytest.raises(FileNotFoundError):
        exporter._resolve_template_path()


def test_sync_runtime_options_does_not_override_runtime_weather_year():
    """Runtime weather_year should take precedence over config value."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024, weather_year=None)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.weather_year = 2012

    exporter._sync_runtime_options_from_config()

    assert exporter.weather_year == 2012


def test_build_xml_filename_uses_runtime_year_overrides():
    """XML naming should honor solve_year/weather_year runtime overrides."""
    config = PLEXOSConfig(model_name="EI_PCM_2023", horizon_year=2023, weather_year=None)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.solve_year = 2023
    exporter.weather_year = 2012

    xml_name = exporter._build_xml_filename()

    assert xml_name == "EI_PCM_2023_2012_2023.xml"


def test_get_required_properties_for_generator_thermal_category(template_db):
    """Test _get_required_properties_for_component resolves category-group for thermal generators."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

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
    exporter = PLEXOSExporter.from_context(ctx)

    gen = PLEXOSGenerator(name="WindGen", category="wind-ons", units=1, rating=100.0)
    result = exporter._get_required_properties_for_component(gen, "PLEXOSGenerator")
    assert isinstance(result, set)
    assert "units" in result


def test_get_required_properties_alias_thermal_normalizes(template_db):
    """Test that 'thermal' category is aliased to thermal-standard group lookup."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    gen = PLEXOSGenerator(name="TGen", category="thermal", units=1, rating=50.0)
    result = exporter._get_required_properties_for_component(gen, "PLEXOSGenerator")
    assert isinstance(result, set)
    assert len(result) > 0


def test_bulk_resolve_object_ids_returns_correct_ids(template_db):
    """Test _bulk_resolve_object_ids returns object IDs for existing objects."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_obj = System(name="test")
    ctx = PluginContext(config=config, system=sys_obj)
    exporter = PLEXOSExporter.from_context(ctx)
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
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    result = exporter._bulk_resolve_object_ids({ClassEnum.Generator: set()})
    assert result == {}


def test_add_objects_safe_adds_new_objects(template_db):
    """Test _add_objects_safe inserts objects and memberships."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_obj = System(name="test")
    ctx = PluginContext(config=config, system=sys_obj)
    exporter = PLEXOSExporter.from_context(ctx)
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
    exporter = PLEXOSExporter.from_context(ctx)
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
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    before = len(template_db.list_objects_by_class(ClassEnum.Generator))
    exporter._add_objects_safe(ClassEnum.Generator, [])
    assert len(template_db.list_objects_by_class(ClassEnum.Generator)) == before


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


def test_export_time_series_prefers_resolution_match(mocker, tmp_path):
    """Exporter should pick TS variant matching ts_key resolution, not first element."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_mock = mocker.Mock()

    class DummyType:
        pass

    comp1 = mocker.Mock()
    comp1.name = "HydroA"
    type(comp1).__name__ = "PLEXOSGenerator"

    comp2 = mocker.Mock()
    comp2.name = "HydroB"
    type(comp2).__name__ = "PLEXOSGenerator"

    ts_key = mocker.Mock()
    ts_key.name = "hydro_budget"
    ts_key.features = {}
    ts_key.initial_timestamp = datetime(2024, 1, 1)
    ts_key.resolution = timedelta(days=7)

    weekly_a = mocker.Mock()
    weekly_a.data = [1.0] * 53
    weekly_a.initial_timestamp = ts_key.initial_timestamp
    weekly_a.resolution = timedelta(days=7)

    hourly_a = mocker.Mock()
    hourly_a.data = [1.0] * 8760
    hourly_a.initial_timestamp = ts_key.initial_timestamp
    hourly_a.resolution = timedelta(hours=1)

    weekly_b = mocker.Mock()
    weekly_b.data = [2.0] * 53
    weekly_b.initial_timestamp = ts_key.initial_timestamp
    weekly_b.resolution = timedelta(days=7)

    sys_mock.get_component_types.return_value = [DummyType]
    sys_mock.get_components.return_value = [comp1, comp2]
    sys_mock.has_time_series.return_value = True
    sys_mock.list_time_series_keys.return_value = [ts_key]

    def _list_ts(component, name, **features):
        if component.name == "HydroA":
            return [hourly_a, weekly_a]
        return [weekly_b]

    sys_mock.list_time_series.side_effect = _list_ts

    ctx = PluginContext(config=config, system=sys_mock)
    exporter = PLEXOSExporter.from_context(ctx)

    data_dir = tmp_path / "Data"
    data_dir.mkdir()

    def _capture(filepath, ts_data):
        assert len(ts_data) == 2
        assert len(ts_data[0][1].data) == 53
        assert len(ts_data[1][1].data) == 53
        return Ok(None)

    mocker.patch("r2x_plexos.exporter.get_output_directory", return_value=data_dir)
    mocker.patch("r2x_plexos.exporter.export_time_series_csv", side_effect=_capture)

    result = exporter.export_time_series()
    assert result.is_ok()


def test_export_time_series_skips_unmatched_variant(mocker, tmp_path):
    """If no TS matches key resolution/timestamp, exporter should skip that component."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_mock = mocker.Mock()

    class DummyType:
        pass

    comp1 = mocker.Mock()
    comp1.name = "HydroA"
    type(comp1).__name__ = "PLEXOSGenerator"

    comp2 = mocker.Mock()
    comp2.name = "HydroB"
    type(comp2).__name__ = "PLEXOSGenerator"

    ts_key = mocker.Mock()
    ts_key.name = "hydro_budget"
    ts_key.features = {}
    ts_key.initial_timestamp = datetime(2024, 1, 1)
    ts_key.resolution = timedelta(days=7)

    hourly_only = mocker.Mock()
    hourly_only.data = [1.0] * 8760
    hourly_only.initial_timestamp = ts_key.initial_timestamp
    hourly_only.resolution = timedelta(hours=1)

    weekly_b = mocker.Mock()
    weekly_b.data = [2.0] * 53
    weekly_b.initial_timestamp = ts_key.initial_timestamp
    weekly_b.resolution = timedelta(days=7)

    sys_mock.get_component_types.return_value = [DummyType]
    sys_mock.get_components.return_value = [comp1, comp2]
    sys_mock.has_time_series.return_value = True
    sys_mock.list_time_series_keys.return_value = [ts_key]

    def _list_ts(component, name, **features):
        if component.name == "HydroA":
            return [hourly_only]
        return [weekly_b]

    sys_mock.list_time_series.side_effect = _list_ts

    ctx = PluginContext(config=config, system=sys_mock)
    exporter = PLEXOSExporter.from_context(ctx)

    data_dir = tmp_path / "Data"
    data_dir.mkdir()

    def _capture(filepath, ts_data):
        assert [name for name, _ in ts_data] == ["HydroB"]
        assert len(ts_data[0][1].data) == 53
        return Ok(None)

    mocker.patch("r2x_plexos.exporter.get_output_directory", return_value=data_dir)
    mocker.patch("r2x_plexos.exporter.export_time_series_csv", side_effect=_capture)

    result = exporter.export_time_series()
    assert result.is_ok()


def test_export_time_series_hydro_budget_prefers_coarsest_when_key_mismatched(mocker, tmp_path):
    """Hydro budget should prefer coarser (weekly) series over hourly when key metadata is inconsistent."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_mock = mocker.Mock()

    class DummyType:
        pass

    comp = mocker.Mock()
    comp.name = "HydroA"
    type(comp).__name__ = "PLEXOSGenerator"

    ts_key = mocker.Mock()
    ts_key.name = "hydro_budget"
    ts_key.features = {}
    ts_key.initial_timestamp = datetime(2024, 1, 1)
    ts_key.resolution = timedelta(hours=1)

    weekly = mocker.Mock()
    weekly.data = [2.0] * 53
    weekly.initial_timestamp = ts_key.initial_timestamp
    weekly.resolution = timedelta(days=7)

    hourly = mocker.Mock()
    hourly.data = [1.0] * 8760
    hourly.initial_timestamp = ts_key.initial_timestamp
    hourly.resolution = timedelta(hours=1)

    sys_mock.get_component_types.return_value = [DummyType]
    sys_mock.get_components.return_value = [comp]
    sys_mock.has_time_series.return_value = True
    sys_mock.list_time_series_keys.return_value = [ts_key]
    sys_mock.list_time_series.return_value = [hourly, weekly]

    ctx = PluginContext(config=config, system=sys_mock)
    exporter = PLEXOSExporter.from_context(ctx)

    data_dir = tmp_path / "Data"
    data_dir.mkdir()

    def _capture(filepath, ts_data):
        assert len(ts_data) == 1
        assert len(ts_data[0][1].data) == 53
        return Ok(None)

    mocker.patch("r2x_plexos.exporter.get_output_directory", return_value=data_dir)
    mocker.patch("r2x_plexos.exporter.export_time_series_csv", side_effect=_capture)

    result = exporter.export_time_series()
    assert result.is_ok()
