import pytest

from r2x_core import DataFile, DataStore
from r2x_plexos import PLEXOSParser
from r2x_plexos.config import PLEXOSConfig
from r2x_plexos.models import PLEXOSMembership, PLEXOSVariable


@pytest.fixture
def config_store_example(data_folder) -> tuple[PLEXOSConfig, DataStore]:
    config = PLEXOSConfig(model_name="Base", timeseries_dir=None, reference_year=2024)
    data_file = DataFile(name="xml_file", glob="*.xml")
    store = DataStore(folder=data_folder)
    store.add_data_file(data_file)
    return config, store


def test_plexos_parser_instance(config_store_example):
    config, store = config_store_example
    parser = PLEXOSParser(config, store)
    assert isinstance(parser, PLEXOSParser)


def test_plexos_parser_system(config_store_example):
    config, store = config_store_example
    parser = PLEXOSParser(config, store)
    system = parser.build_system()
    assert system is not None
    assert system.name == "system"


def test_memberships_added(config_store_example):
    config, store = config_store_example
    parser = PLEXOSParser(config, store)
    system = parser.build_system()

    memberships = list(system.get_supplemental_attributes(PLEXOSMembership))
    assert len(memberships) > 0

    for membership in memberships:
        assert isinstance(membership, PLEXOSMembership)
        assert membership.membership_id is not None
        assert membership.parent_object is not None
        assert membership.collection is not None


def test_variables_parsed(config_store_example):
    """Test that Variable components are correctly parsed."""
    config, store = config_store_example
    parser = PLEXOSParser(config, store)
    system = parser.build_system()

    variables = list(system.get_components(PLEXOSVariable))
    assert len(variables) > 0, "Should have parsed at least one variable"

    # Check that variables have basic attributes
    for var in variables:
        assert isinstance(var, PLEXOSVariable)
        assert var.name is not None
        assert var.object_id is not None
