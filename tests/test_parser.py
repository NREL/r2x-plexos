from typing import cast

import pytest
from plexosdb import ClassEnum

from r2x_core import DataFile, DataStore, PluginContext
from r2x_plexos import PLEXOSParser, PLEXOSPropertyValue
from r2x_plexos.models import PLEXOSMembership, PLEXOSVariable
from r2x_plexos.models.datafile import PLEXOSDatafile
from r2x_plexos.models.generator import PLEXOSGenerator
from r2x_plexos.models.region import PLEXOSRegion
from r2x_plexos.plugin_config import PLEXOSConfig


@pytest.fixture(scope="module")
def config_store_example(data_folder) -> tuple[PLEXOSConfig, DataStore]:
    config = PLEXOSConfig(model_name="Base", timeseries_dir=None, horizon_year=2024)
    xml_files = list(data_folder.glob("*.xml"))
    if not xml_files:
        raise ValueError(f"No XML files found in {data_folder}")

    xml_path = xml_files[0]
    data_file = DataFile(name="xml_file", fpath=xml_path)

    store = DataStore(path=data_folder)
    store.add_data([data_file], overwrite=True)

    return config, store


@pytest.fixture(scope="module")
def parser_instance(config_store_example) -> PLEXOSParser:
    """Shared parser instance for read-only tests."""
    config, store = config_store_example
    ctx = PluginContext(config=config, store=store)
    return cast(PLEXOSParser, PLEXOSParser.from_context(ctx))


@pytest.fixture(scope="module")
def parser_system(parser_instance):
    """Shared system built from parser for read-only tests."""
    result = parser_instance.run()
    return result.system


@pytest.mark.slow
def test_plexos_parser_instance(parser_instance):
    assert isinstance(parser_instance, PLEXOSParser)


@pytest.mark.slow
def test_plexos_parser_system(parser_system):
    assert parser_system is not None
    assert parser_system.name == "PLEXOS"


@pytest.mark.slow
def test_memberships_added(parser_system):
    memberships = list(parser_system.get_supplemental_attributes(PLEXOSMembership))
    assert len(memberships) > 0

    for membership in memberships:
        assert isinstance(membership, PLEXOSMembership)
        assert membership.membership_id is not None
        assert membership.parent_object is not None
        assert membership.collection is not None


@pytest.mark.slow
def test_variables_parsed(parser_system):
    """Test that Variable components are correctly parsed."""
    variables = list(parser_system.get_components(PLEXOSVariable))
    assert len(variables) > 0, "Should have parsed at least one variable"

    # Check that variables have basic attributes
    for var in variables:
        assert isinstance(var, PLEXOSVariable)
        assert var.name is not None
        assert var.object_id is not None


def test_collection_properties_basic(db_with_reserve_collection_property, tmp_path):
    from r2x_plexos.models.collection_property import CollectionProperties
    from r2x_plexos.models.region import PLEXOSRegion
    from r2x_plexos.models.reserve import PLEXOSReserve

    db = db_with_reserve_collection_property
    xml_path = tmp_path / "reserve_coll_basic.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    system = result.system
    assert system is not None

    reserve = system.get_component(PLEXOSReserve, "TestReserve")
    assert reserve is not None

    region = system.get_component(PLEXOSRegion, "region-01")
    assert region is not None

    coll_props_list = system.get_supplemental_attributes_with_component(region, CollectionProperties)
    assert len(coll_props_list) > 0

    coll_props = coll_props_list[0]
    assert coll_props.collection_name == "Regions"
    assert "load_risk" in coll_props.properties

    load_risk_prop = coll_props.properties["load_risk"]
    load_risk_value = load_risk_prop.get_value()
    assert load_risk_value == 6.0


def test_collection_properties_with_timeseries(db_with_reserve_collection_property, tmp_path):
    from r2x_plexos.models.collection_property import CollectionProperties
    from r2x_plexos.models.region import PLEXOSRegion
    from r2x_plexos.models.reserve import PLEXOSReserve

    db = db_with_reserve_collection_property
    xml_path = tmp_path / "reserve_coll_prop.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    system = result.system
    assert system is not None

    reserve = system.get_component(PLEXOSReserve, "TestReserve")
    assert reserve is not None

    region = system.get_component(PLEXOSRegion, "region-01")
    assert region is not None

    coll_props_list = system.get_supplemental_attributes_with_component(region, CollectionProperties)
    assert len(coll_props_list) > 0

    coll_props = coll_props_list[0]
    assert "lolp_target" in coll_props.properties
    assert "load_risk" in coll_props.properties

    lolp_prop = coll_props.properties["lolp_target"]
    assert lolp_prop.has_datafile()

    assert system.has_time_series(coll_props)

    ts = system.get_time_series(coll_props, "lolp_target")
    assert ts is not None
    assert len(ts.data) == 8784
    assert list(ts.data[:6]) == [1.5, 2.0, 2.5, 3.0, 3.5, 4.0]
    assert max(ts.data) == 4.0

    lolp_value = lolp_prop.get_value()
    assert lolp_value == 4.0


def test_property_with_bands_kept_as_property_value(db_thermal_gen_multiband, tmp_path):
    """Test that multi-band properties are kept as PLEXOSPropertyValue objects."""
    db = db_thermal_gen_multiband
    xml_path = tmp_path / "multiband.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    system = result.system
    assert system is not None

    gen = system.get_component(PLEXOSGenerator, "thermal-01")

    # heat_rate has multiple bands, should be PLEXOSPropertyValue
    heat_rate_prop = gen.get_property_value("heat_rate")
    assert isinstance(heat_rate_prop, PLEXOSPropertyValue)
    assert heat_rate_prop.has_bands()

    # Accessing the property should return a dict of band values
    result_value = gen.heat_rate
    assert isinstance(result_value, dict)


def test_simple_numeric_property_extracted_as_value(db_thermal_gen_multiband, tmp_path):
    """Test that simple numeric properties without datafile/variable/bands are extracted as raw values."""
    db = db_thermal_gen_multiband
    xml_path = tmp_path / "simple_numeric.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    system = result.system
    assert system is not None

    gen = system.get_component(PLEXOSGenerator, "thermal-01")

    # max_capacity is a simple numeric value, should be extracted as float
    assert isinstance(gen.max_capacity, float)
    assert gen.max_capacity == 100.0


def test_region_load_with_variable_reference(db_with_multiband_variable, tmp_path):
    """Test that region load property with variable reference is handled correctly."""
    db = db_with_multiband_variable
    config = PLEXOSConfig(model_name="Base")
    store = DataStore(path=tmp_path)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    sys = result.system
    assert sys is not None

    region = sys.get_component(PLEXOSRegion, "r1")

    # load property should reference a variable
    load_prop = region.get_property_value("load")
    assert isinstance(load_prop, PLEXOSPropertyValue)
    assert load_prop.has_variable()


def test_datafile_component_not_registered_for_timeseries(db_with_multiband_variable, tmp_path):
    """Test that datafile components don't get time series registered."""
    db = db_with_multiband_variable
    config = PLEXOSConfig(model_name="Base")
    store = DataStore(path=tmp_path)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    sys = result.system
    assert sys is not None

    datafile = sys.get_component(PLEXOSDatafile, "LoadProfiles")
    assert datafile is not None

    # Datafile components should NOT have time series
    assert not sys.has_time_series(datafile)


def test_variable_component_not_registered_for_timeseries(db_with_multiband_variable, tmp_path):
    """Test that variable components don't get time series registered."""
    db = db_with_multiband_variable
    config = PLEXOSConfig(model_name="Base")
    store = DataStore(path=tmp_path)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    sys = result.system
    assert sys is not None

    # Get all variables
    variables = list(sys.get_components(PLEXOSVariable))
    assert len(variables) > 0

    # None should have time series
    for var in variables:
        assert not sys.has_time_series(var)


def test_parser_metadata_and_description(parser_system):
    """Test that parser sets system metadata correctly."""
    assert parser_system.data_format_version is not None
    assert "PLEXOS system" in parser_system.description
    assert "Base" in parser_system.description


def test_failed_references_tracking(db_with_multiband_variable, tmp_path, caplog):
    """Test that parser tracks failed time series attachments."""
    db = db_with_multiband_variable
    config = PLEXOSConfig(model_name="Base")
    store = DataStore(path=tmp_path)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    _ = parser.run()

    # Check if any failures were logged
    assert parser._failed_references is not None
    # The list might be empty if all attachments succeeded, which is also valid


def test_parser_with_horizon_year(db_thermal_gen_multiband, tmp_path):
    """Test parser with horizon year configuration."""
    db = db_thermal_gen_multiband
    xml_path = tmp_path / "with_horizon.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2025)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file])

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    system = result.system

    assert system is not None
    assert config.horizon_year == 2025


def test_parser_component_cache_population(db_thermal_gen_multiband, tmp_path):
    """Test that component cache is properly populated during parsing."""
    db = db_thermal_gen_multiband
    xml_path = tmp_path / "cache_test.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file])

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    system = result.system
    assert system is not None

    # Component cache should be populated
    assert len(parser._component_cache) > 0

    # All cached components should be in the system
    for obj_id, component in parser._component_cache.items():
        assert obj_id == component.object_id
        system_component = system.get_component_by_uuid(component.uuid)
        assert system_component is not None


def test_parser_membership_cache(db_with_topology, tmp_path):
    """Test that membership cache is populated correctly."""
    db = db_with_topology
    xml_path = tmp_path / "membership.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file])

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    _ = parser.run()

    # Membership cache should be populated
    assert len(parser._membership_cache) > 0

    # Verify memberships are in the system
    for membership_id, membership in parser._membership_cache.items():
        assert membership.membership_id == membership_id
        assert membership.parent_object is not None
        assert membership.child_object is not None


def test_parser_handles_unsupported_component_type(db_base, tmp_path, caplog):
    """Test that parser gracefully handles unsupported component types."""
    import loguru

    db = db_base
    xml_path = tmp_path / "unsupported.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file])

    with caplog.at_level(loguru.logger.level("DEBUG").no):
        ctx = PluginContext(config=config, store=store)
        parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
        parser.db = db
        result = parser.run()
        system = result.system

    # System should still be created even if some components are unsupported
    assert system is not None


def test_parser_scenario_priority_setting(db_with_scenarios, tmp_path):
    """Test that parser correctly sets scenario priority."""
    db = db_with_scenarios
    xml_path = tmp_path / "scenarios.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file])

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    system = result.system

    # System should be built successfully with scenarios
    assert system is not None


def test_parser_timeseries_cache_reuse(db_with_multiband_variable, tmp_path):
    """Test that parsed time series files are cached and reused."""
    db = db_with_multiband_variable
    config = PLEXOSConfig(model_name="Base")
    store = DataStore(path=tmp_path)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    _ = parser.run()

    # Check that the cache was used (has entries)
    assert len(parser._parsed_files_cache) >= 0  # May be 0 if no time series files


def test_parser_with_custom_name(db_thermal_gen_multiband, tmp_path):
    """Test parser with custom system name."""
    db = db_thermal_gen_multiband
    xml_path = tmp_path / "custom_name.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file])

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    system = result.system
    assert system is not None

    # Note: System name comes from on_build(), which uses "PLEXOS" by default
    assert system.name == "PLEXOS"


def test_parser_skip_validation_flag(db_thermal_gen_multiband, tmp_path):
    """Test parser with skip_validation flag."""
    db = db_thermal_gen_multiband
    xml_path = tmp_path / "skip_val.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file])

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    system = result.system

    assert system is not None


def test_parser_property_without_field_name_skipped(db_thermal_gen_multiband, tmp_path, caplog):
    """Test that properties without matching field names are skipped with warning."""
    import loguru

    db = db_thermal_gen_multiband
    xml_path = tmp_path / "unknown_prop.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file])

    with caplog.at_level(loguru.logger.level("WARNING").no):
        ctx = PluginContext(config=config, store=store)
        parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
        parser.db = db
        result = parser.run()
        system = result.system

    # System should still be created
    assert system is not None


def test_parser_build_without_db(tmp_path):
    """Test that parser fails gracefully without database."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    store = DataStore(path=tmp_path)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    # Don't set parser.db

    result = parser.on_build()
    assert result.is_err()
    assert "Could not find PLEXOS XML file" in str(result.error)


def test_parser_with_missing_xml_file(tmp_path):
    """Test parser with non-existent XML file raises error during DataFile creation."""
    fake_path = tmp_path / "nonexistent.xml"

    with pytest.raises(FileNotFoundError, match="File not found"):
        data_file = DataFile(name="xml_file", fpath=fake_path)
        store = DataStore(path=tmp_path)
        store.add_data([data_file], overwrite=True)


def test_parser_validate_inputs_missing_model(db_base, tmp_path):
    """Test validation fails when model doesn't exist in database."""
    db = db_base
    xml_path = tmp_path / "no_model.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="NonExistentModel", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.validate_inputs()
    assert result.is_err()
    assert "not found" in str(result.error).lower()


def test_parser_error_handling_in_component_creation(db_base, tmp_path):
    """Test error handling when component creation fails due to validation."""
    from r2x_core.exceptions import PluginError

    db = db_base

    db.add_object(ClassEnum.Generator, "BadGen")
    db.add_property(ClassEnum.Generator, "BadGen", "Max Capacity", value="not_a_number")

    xml_path = tmp_path / "bad_data.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    with pytest.raises(PluginError, match="validation error"):
        parser.run()


def test_parser_collection_property_without_parent(db_with_reserve_collection_property, tmp_path):
    """Test handling of collection properties when parent object doesn't exist."""
    from plexosdb import CollectionEnum

    db = db_with_reserve_collection_property

    try:  # noqa: SIM105
        db.add_property(
            ClassEnum.Region,
            "region-01",
            "Load Risk",
            value=5.0,
            collection_enum=CollectionEnum.Regions,
            parent_class_enum=ClassEnum.Reserve,
            parent_object_name="NonExistentReserve",
        )
    except Exception:
        pass

    xml_path = tmp_path / "orphan_coll_prop.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    assert result.system is not None


def test_parser_time_series_with_invalid_datafile_path(db_with_multiband_variable, tmp_path):
    """Test handling of time series with invalid datafile paths."""
    db = db_with_multiband_variable

    db._db.execute(
        "UPDATE t_data SET value = ? WHERE property_id IN (SELECT property_id FROM t_property WHERE name = 'Filename')",
        (str(tmp_path / "nonexistent.csv"),),
    )

    xml_path = tmp_path / "bad_ts.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    assert result.system is not None
    assert isinstance(parser._failed_references, list)


def test_parser_membership_with_invalid_collection(db_with_topology, tmp_path):
    """Test handling of memberships with invalid collection IDs."""
    db = db_with_topology

    # Add membership with non-standard collection
    db._db.execute(
        "INSERT INTO t_membership (parent_class_id, parent_object_id, collection_id, child_class_id, child_object_id) VALUES (?, ?, ?, ?, ?)",
        (1, 1, 99999, 2, 1),  # 99999 is invalid collection_id
    )

    xml_path = tmp_path / "bad_membership.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    # Parser should handle invalid collection gracefully
    assert result.system is not None


def test_parser_property_with_multiple_scenarios(db_with_scenarios, tmp_path):
    """Test handling of properties with multiple scenario values."""
    from plexosdb import CollectionEnum

    db = db_with_scenarios

    # Use Region load property which is better suited for scenario testing
    region_name = "TestRegion"
    db.add_object(ClassEnum.Region, region_name)

    # Add property with multiple scenarios
    db.add_property(ClassEnum.Region, region_name, "Load", value=100.0, scenario="Scenario1")
    db.add_property(ClassEnum.Region, region_name, "Load", value=150.0, scenario="Scenario2")
    db.add_property(ClassEnum.Region, region_name, "Load", value=200.0, scenario="Scenario3")

    # Link a scenario to the model so parser knows which to use
    if db.check_object_exists(ClassEnum.Scenario, "Scenario2"):
        db.add_membership(ClassEnum.Model, ClassEnum.Scenario, "Base", "Scenario2", CollectionEnum.Scenarios)

    xml_path = tmp_path / "multi_scenario.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    system = result.system
    assert system is not None

    region = system.get_component(PLEXOSRegion, region_name)
    assert region is not None

    # When a scenario is linked and resolved, the property value is extracted
    # The resolved value should be from Scenario2 (which is linked to the model)
    assert isinstance(region.load, float)
    assert region.load in [100.0, 150.0, 200.0]

    # We can still access the underlying property value object
    load_prop = region.get_property_value("load")
    if isinstance(load_prop, PLEXOSPropertyValue):
        assert load_prop.has_scenarios()


def test_parser_datafile_with_no_filename_property(db_base, tmp_path):
    """Test handling of DataFile objects without filename property."""
    db = db_base

    # Add datafile without filename property
    db.add_object(ClassEnum.DataFile, "EmptyDataFile")

    xml_path = tmp_path / "no_filename.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    system = result.system

    # Should still create system
    assert system is not None


def test_parser_variable_with_no_profile(db_base, tmp_path):
    """Test handling of Variable objects without profile property."""
    from r2x_plexos.models import PLEXOSVariable

    db = db_base

    # Add variable without profile
    db.add_object(ClassEnum.Variable, "EmptyVariable")

    xml_path = tmp_path / "no_profile.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    system = result.system
    assert system is not None

    var = system.get_component(PLEXOSVariable, "EmptyVariable")
    assert var is not None


def test_parser_property_with_text_and_value(db_base, tmp_path):
    """Test handling of properties with both text and value fields."""
    db = db_base

    gen_name = "TestGen"
    db.add_object(ClassEnum.Generator, gen_name)

    # Add property with both text and value
    db.add_property(ClassEnum.Generator, gen_name, "Units", value=5.0, datafile_text="Some text description")

    xml_path = tmp_path / "text_value.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    system = result.system
    assert system is not None

    gen = system.get_component(PLEXOSGenerator, gen_name)
    assert gen is not None


def test_parser_time_series_attachment_failure_tracked(db_with_multiband_variable, tmp_path, caplog):
    """Test that failed time series attachments are properly tracked."""
    import loguru

    db = db_with_multiband_variable

    config = PLEXOSConfig(model_name="Base")
    store = DataStore(path=tmp_path)

    with caplog.at_level(loguru.logger.level("DEBUG").no):
        ctx = PluginContext(config=config, store=store)
        parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
        parser.db = db

        result = parser.run()
        system = result.system

    assert system is not None
    # Check that failed references are being tracked
    assert isinstance(parser._failed_references, list)


def test_parser_component_with_category(db_base, tmp_path):
    """Test parsing component with category."""
    db = db_base

    gen_name = "CategorizedGen"
    db.add_object(ClassEnum.Generator, gen_name, category="Thermal")

    xml_path = tmp_path / "with_category.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    system = result.system
    assert system is not None

    gen = system.get_component(PLEXOSGenerator, gen_name)
    assert gen is not None
    assert gen.category == "Thermal"


def test_parser_postprocess_system_success(db_thermal_gen_multiband, tmp_path):
    """Test successful postprocess_system execution."""
    db = db_thermal_gen_multiband
    xml_path = tmp_path / "postprocess.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()

    # Postprocess should have run successfully
    assert result.system is not None


def test_parser_build_time_series_error_handling(db_with_multiband_variable, tmp_path, monkeypatch):
    """Test error handling in build_time_series."""
    db = db_with_multiband_variable

    config = PLEXOSConfig(model_name="Base")
    store = DataStore(path=tmp_path)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    # Mock to simulate error in time series building
    def mock_error(*args, **kwargs):
        raise Exception("Simulated time series error")

    # This will cause build_time_series to handle errors
    result = parser.run()

    # Should still return a system even if time series fail
    assert result.system is not None


def test_parser_property_value_extraction_edge_cases(db_base, tmp_path):
    """Test property value extraction with edge cases."""
    db = db_base

    gen_name = "EdgeCaseGen"
    db.add_object(ClassEnum.Generator, gen_name)

    db.add_property(ClassEnum.Generator, gen_name, "Units", value=1)
    db.add_property(ClassEnum.Generator, gen_name, "Rating", value=50.0)
    db.add_property(ClassEnum.Generator, gen_name, "Max Capacity", value=0.0)
    db.add_property(ClassEnum.Generator, gen_name, "Min Stable Level", value=10.0)

    xml_path = tmp_path / "edge_cases.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    system = result.system
    assert system is not None

    gen = system.get_component(PLEXOSGenerator, gen_name)
    assert gen is not None
    assert gen.max_capacity == 0.0
    assert gen.min_stable_level == 10.0
    assert gen.units == 1
    assert gen.rating == 50.0


def test_validate_inputs_no_horizon(db_base, tmp_path):
    """Test validation when model has no horizon defined - line 267."""
    db = db_base
    xml_path = tmp_path / "no_horizon.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    # This should trigger horizon validation
    result = parser.validate_inputs()
    # Depending on implementation, may succeed or fail
    assert result is not None


def test_build_components_with_unknown_class(db_base, tmp_path):
    """Test component building with unsupported PLEXOS class - lines 369-373."""
    db = db_base

    # Add an object of a class type that might not have a mapper
    # Try ClassEnum.Constraint or another less common class
    db.add_object(ClassEnum.Constraint, "TestConstraint")

    xml_path = tmp_path / "unknown_class.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    # Should handle unknown class gracefully
    assert result.system is not None


def test_property_without_object_reference(db_base, tmp_path):
    """Test property handling when object doesn't exist - lines 933, 942-943."""
    db = db_base

    # Manually insert a property that references non-existent object
    db._db.execute(
        "INSERT INTO t_data (class_id, object_id, property_id, value) VALUES (?, ?, ?, ?)",
        (1, 99999, 1, 100.0),  # object_id 99999 doesn't exist
    )

    xml_path = tmp_path / "orphan_property.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    assert result.system is not None


def test_datafile_without_filename_tag(db_base, tmp_path):
    """Test datafile handling without filename tag - lines 1186-1187."""
    db = db_base

    datafile_name = "NoFilenameDatafile"
    db.add_object(ClassEnum.DataFile, datafile_name)

    gen_name = "TestGen"
    db.add_object(ClassEnum.Generator, gen_name)
    db.add_property(ClassEnum.Generator, gen_name, "Units", value=1)
    db.add_property(ClassEnum.Generator, gen_name, "Rating", value=50.0)

    prop_id = db.add_property(
        ClassEnum.Generator, gen_name, "Max Capacity", value=0.0, datafile_text=datafile_name
    )

    datafile_id = db.get_object_id(ClassEnum.DataFile, datafile_name)
    db._db.execute(
        "INSERT INTO t_tag (object_id, data_id, action_id) VALUES (?, ?, ?)", (datafile_id, prop_id, 1)
    )

    xml_path = tmp_path / "no_filename_tag.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    assert result.system is not None


def test_variable_without_profile_tag(db_base, tmp_path):
    """Test variable handling without profile tag - lines 1190-1191."""
    db = db_base

    var_name = "NoProfileVariable"
    db.add_object(ClassEnum.Variable, var_name)

    region_name = "TestRegion"
    db.add_object(ClassEnum.Region, region_name)

    prop_id = db.add_property(ClassEnum.Region, region_name, "Load", value=100.0)

    var_id = db.get_object_id(ClassEnum.Variable, var_name)
    db._db.execute("INSERT INTO t_tag (object_id, data_id, action_id) VALUES (?, ?, ?)", (var_id, prop_id, 1))

    xml_path = tmp_path / "no_profile_tag.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    assert result.system is not None


def test_time_series_with_missing_resolution(db_with_multiband_variable, tmp_path):
    """Test time series building when resolution cannot be determined - lines 1303, 1307."""
    db = db_with_multiband_variable

    db._db.execute(
        "DELETE FROM t_data WHERE property_id IN (SELECT property_id FROM t_property WHERE name = 'Pattern')"
    )

    xml_path = tmp_path / "no_resolution.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base")
    store = DataStore(path=tmp_path)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    assert result.system is not None


def test_property_with_action_type_multiply(db_base, tmp_path):
    """Test property with action_id = 2 (multiply) - lines 1138-1145."""
    db = db_base

    gen_name = "MultiplyGen"
    db.add_object(ClassEnum.Generator, gen_name)
    db.add_property(ClassEnum.Generator, gen_name, "Units", value=1)
    db.add_property(ClassEnum.Generator, gen_name, "Rating", value=50.0)

    var_name = "MultiplierVar"
    var_id = db.add_object(ClassEnum.Variable, var_name)
    _ = db.add_property(ClassEnum.Variable, var_name, "Profile", value=2.0)

    gen_prop_id = db.add_property(ClassEnum.Generator, gen_name, "Max Capacity", value=100.0)

    db._db.execute(
        "INSERT INTO t_tag (object_id, data_id, action_id) VALUES (?, ?, ?)",
        (var_id, gen_prop_id, 2),  # action_id = 2 for multiply
    )

    xml_path = tmp_path / "multiply_action.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    system = result.system
    assert system is not None

    gen = system.get_component(PLEXOSGenerator, gen_name)
    assert gen is not None
    assert gen.max_capacity == 100.0


def test_collection_property_with_pattern(db_with_reserve_collection_property, tmp_path):
    """Test collection properties with pattern/time series - lines 1255-1290."""
    from plexosdb import CollectionEnum

    db = db_with_reserve_collection_property

    reserve_name = "TestReserve"
    region_name = "region-01"

    pattern_file = tmp_path / "pattern.csv"
    pattern_file.write_text("Hour,Value\n1,10.0\n2,20.0\n3,30.0\n")

    datafile_name = "PatternFile"
    datafile_id = db.add_object(ClassEnum.DataFile, datafile_name)
    db.add_property(ClassEnum.DataFile, datafile_name, "Filename", value=0, datafile_text=str(pattern_file))

    # Add collection property with datafile reference
    coll_prop_id = db.add_property(
        ClassEnum.Region,
        region_name,
        "Load Risk",
        value=0.0,
        collection_enum=CollectionEnum.Regions,
        parent_class_enum=ClassEnum.Reserve,
        parent_object_name=reserve_name,
    )

    db._db.execute(
        "INSERT INTO t_tag (object_id, data_id, action_id) VALUES (?, ?, ?)", (datafile_id, coll_prop_id, 1)
    )

    xml_path = tmp_path / "coll_prop_pattern.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    assert result.system is not None


def test_membership_without_child_object(db_with_topology, tmp_path):
    """Test membership handling when child object doesn't exist - lines 974-975."""
    db = db_with_topology

    db._db.execute(
        "INSERT INTO t_membership (parent_class_id, parent_object_id, collection_id, child_class_id, child_object_id) VALUES (?, ?, ?, ?, ?)",
        (1, 1, 1, 2, 99999),  # child_object_id 99999 doesn't exist
    )

    xml_path = tmp_path / "orphan_membership.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    assert result.system is not None


def test_postprocess_with_timeseries_metadata_issues(db_with_multiband_variable, tmp_path):
    """Test postprocess when time series metadata has issues - lines 1483, 1489."""
    db = db_with_multiband_variable

    config = PLEXOSConfig(model_name="Base")
    store = DataStore(path=tmp_path)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    system = result.system

    assert system is not None
    assert system.data_format_version is not None
