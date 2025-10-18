from pathlib import Path

import pytest

from r2x_plexos.models.context import set_horizon, set_scenario_priority

DATA_FOLDER = "tests/data"
SIMPLE_XML = "5_bus_system_variables.xml"


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')")
    config.addinivalue_line("markers", "fast: marks tests as fast (select with '-m fast')")


@pytest.fixture(autouse=True)
def reset_global_context():
    """Reset global scenario priority and horizon context between tests."""
    # Clear before test
    set_scenario_priority(None)
    set_horizon(None)
    yield
    # Clear after test
    set_scenario_priority(None)
    set_horizon(None)


@pytest.fixture(scope="session")
def data_folder(pytestconfig: pytest.Config) -> Path:
    return pytestconfig.rootpath.joinpath(DATA_FOLDER)


@pytest.fixture(scope="session")
def simple_xml(data_folder: Path) -> Path:
    xml_path = data_folder.joinpath(SIMPLE_XML)
    return xml_path


@pytest.fixture(scope="session")
def simple_xml_with_reserve_collection_property(
    simple_xml: Path, tmp_path_factory: pytest.TempPathFactory
) -> Path:
    """Create a test XML with a Reserve that has a collection property on a Region."""
    from plexosdb import ClassEnum, CollectionEnum, PlexosDB

    db = PlexosDB.from_xml(simple_xml)

    regions = db.list_objects_by_class(ClassEnum.Region)
    if not regions:
        raise ValueError("No regions found in the XML")

    first_region = regions[0]

    db.add_object(ClassEnum.Reserve, "TestReserve")

    _ = db.add_membership(
        parent_class_enum=ClassEnum.Reserve,
        child_class_enum=ClassEnum.Region,
        parent_object_name="TestReserve",
        child_object_name=first_region,
        collection_enum=CollectionEnum.Regions,
    )

    db.add_property(
        ClassEnum.Region,
        first_region,
        "Load Risk",
        6.0,
        collection_enum=CollectionEnum.Regions,
        parent_class_enum=ClassEnum.Reserve,
        parent_object_name="TestReserve",
    )

    db.add_property(
        ClassEnum.Region,
        first_region,
        "LOLP Target",
        1.0,
        text={ClassEnum.DataFile: "test_collection_prop_ts.csv"},
        collection_enum=CollectionEnum.Regions,
        parent_class_enum=ClassEnum.Reserve,
        parent_object_name="TestReserve",
    )

    tmp_path = tmp_path_factory.mktemp("test_xml")
    output_path = tmp_path / "test_reserve_collection_prop.xml"
    db.to_xml(output_path)

    return output_path
