import pytest

from r2x_core import DataFile, DataStore
from r2x_plexos import PLEXOSParser
from r2x_plexos.config import PLEXOSConfig
from r2x_plexos.models import PLEXOSMembership, PLEXOSVariable


@pytest.fixture(scope="module")
def config_store_example(data_folder) -> tuple[PLEXOSConfig, DataStore]:
    config = PLEXOSConfig(model_name="Base", timeseries_dir=None, reference_year=2024)
    data_file = DataFile(name="xml_file", glob="*.xml")
    store = DataStore(path=data_folder)
    store.add_data(data_file)
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


def test_collection_properties_basic(db_with_reserve_collection_property, tmp_path):
    from r2x_plexos.models.collection_property import CollectionProperties
    from r2x_plexos.models.region import PLEXOSRegion
    from r2x_plexos.models.reserve import PLEXOSReserve

    db = db_with_reserve_collection_property
    xml_path = tmp_path / "reserve_coll_basic.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", reference_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data(data_file)

    parser = PLEXOSParser(config, store)
    system = parser.build_system()

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

    config = PLEXOSConfig(model_name="Base", reference_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data(data_file)

    parser = PLEXOSParser(config, store)
    system = parser.build_system()

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
