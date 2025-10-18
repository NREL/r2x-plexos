import pytest

from r2x_core import DataFile, DataStore
from r2x_plexos import PLEXOSParser
from r2x_plexos.config import PLEXOSConfig
from r2x_plexos.models import PLEXOSMembership, PLEXOSVariable


@pytest.fixture(scope="module")
def config_store_example(data_folder) -> tuple[PLEXOSConfig, DataStore]:
    config = PLEXOSConfig(model_name="Base", timeseries_dir=None, reference_year=2024)
    data_file = DataFile(name="xml_file", glob="*.xml")
    store = DataStore(folder=data_folder)
    store.add_data_file(data_file)
    return config, store


@pytest.fixture(scope="module")
def parser_instance(config_store_example) -> PLEXOSParser:
    """Shared parser instance for read-only tests."""
    config, store = config_store_example
    return PLEXOSParser(config, store)


@pytest.fixture(scope="module")
def parser_system(parser_instance):
    """Shared system built from parser for read-only tests."""
    return parser_instance.build_system()


@pytest.mark.slow
def test_plexos_parser_instance(parser_instance):
    assert isinstance(parser_instance, PLEXOSParser)


@pytest.mark.slow
def test_plexos_parser_system(parser_system):
    assert parser_system is not None
    assert parser_system.name == "system"


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


@pytest.mark.slow
def test_collection_properties_basic(simple_xml_with_reserve_collection_property):
    """Test that collection properties are parsed and added as supplemental attributes."""
    from r2x_plexos.models.collection_property import CollectionProperties
    from r2x_plexos.models.reserve import PLEXOSReserve

    config = PLEXOSConfig(model_name="Base", timeseries_dir=None, reference_year=2024)
    data_file = DataFile(name="xml_file", fpath=simple_xml_with_reserve_collection_property)
    store = DataStore(folder=simple_xml_with_reserve_collection_property.parent)
    store.add_data_file(data_file)

    parser = PLEXOSParser(config, store)
    system = parser.build_system()

    reserve = system.get_component(PLEXOSReserve, "TestReserve")
    assert reserve is not None, "TestReserve should exist"

    from r2x_plexos.models.region import PLEXOSRegion

    regions = list(system.get_components(PLEXOSRegion))
    assert len(regions) > 0, "Should have at least one region"

    region = regions[0]
    coll_props_list = system.get_supplemental_attributes_with_component(region, CollectionProperties)

    assert len(coll_props_list) > 0, "Should have collection properties"

    coll_props = coll_props_list[0]
    assert coll_props.collection_name == "Regions"
    assert "load_risk" in coll_props.properties

    load_risk_prop = coll_props.properties["load_risk"]
    load_risk_value = load_risk_prop.get_value()
    assert load_risk_value == 6.0, f"Load Risk should be 6.0, got {load_risk_value}"


@pytest.mark.slow
def test_collection_properties_with_timeseries(simple_xml_with_reserve_collection_property, data_folder):
    """Test that collection properties with time series are correctly resolved."""
    from r2x_plexos.models.collection_property import CollectionProperties
    from r2x_plexos.models.region import PLEXOSRegion
    from r2x_plexos.models.reserve import PLEXOSReserve

    config = PLEXOSConfig(model_name="Base", timeseries_dir=None, reference_year=2024)
    data_file = DataFile(name="xml_file", fpath=simple_xml_with_reserve_collection_property)
    store = DataStore(folder=data_folder)
    store.add_data_file(data_file)

    parser = PLEXOSParser(config, store)
    system = parser.build_system()

    reserve = system.get_component(PLEXOSReserve, "TestReserve")
    assert reserve is not None, "TestReserve should exist"

    regions = list(system.get_components(PLEXOSRegion))
    assert len(regions) > 0, "Should have at least one region"

    region = regions[0]
    coll_props_list = system.get_supplemental_attributes_with_component(region, CollectionProperties)

    assert len(coll_props_list) > 0, "Should have collection properties"

    coll_props = coll_props_list[0]
    assert "lolp_target" in coll_props.properties, (
        f"Should have lolp_target, got {list(coll_props.properties.keys())}"
    )

    lolp_prop = coll_props.properties["lolp_target"]
    assert lolp_prop.has_datafile(), "LOLP Target should have datafile reference"

    has_ts = system.has_time_series(coll_props)
    assert has_ts, "Collection property should have time series attached"

    ts_list = system.list_time_series(coll_props)
    assert len(ts_list) > 0, "Should have at least one time series"

    ts = system.get_time_series(coll_props, "lolp_target")
    assert ts is not None, "Should be able to get time series"
    assert len(ts.data) == 8784, f"Time series should have full year of data, got {len(ts.data)}"
    assert list(ts.data[:6]) == [1.5, 2.0, 2.5, 3.0, 3.5, 4.0], (
        f"First 6 values should match CSV, got {list(ts.data[:6])}"
    )
    assert max(ts.data) == 4.0, f"Max value should be 4.0, got {max(ts.data)}"

    lolp_value = lolp_prop.get_value()
    assert lolp_value == 4.0, (
        f"Property value should be updated to max of time series (4.0), got {lolp_value}"
    )
