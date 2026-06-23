import pytest
from plexosdb import ClassEnum, CollectionEnum, PlexosDB

from r2x_plexos.utils_plexosdb import get_collection_enum, get_collection_name


@pytest.fixture
def empty_db(data_folder):
    db = PlexosDB.from_xml(data_folder.joinpath("5_bus_system_variables.xml"))
    return db


@pytest.mark.slow
def test_plexosdb_get_collection_name(empty_db):
    collection = CollectionEnum.Generators
    collection_id = empty_db.get_collection_id(collection, ClassEnum.System, ClassEnum.Generator)
    collection_name_returned = get_collection_name(empty_db, collection_id)
    assert collection_name_returned == collection
    assert not get_collection_name(empty_db, 1000)


def test_plexosdb_get_collection_enum():
    assert get_collection_enum("Generators") == CollectionEnum.Generators
    assert not get_collection_enum("Not")


def test_get_collection_enum_reference_node_by_member_name():
    """ReferenceNode must be accessible by member name via get_collection_enum.

    CollectionEnum.ReferenceNode may have value != name on older plexosdb
    releases (compat path): value='Reference Node', name='ReferenceNode'.
    The lookup must use __members__[name], not CollectionEnum(value).
    """
    result = get_collection_enum("ReferenceNode")
    assert result is not None
    assert result is CollectionEnum.__members__["ReferenceNode"]


def test_get_collection_enum_unknown_name_returns_none():
    """get_collection_enum returns None for names not in CollectionEnum.__members__."""
    assert get_collection_enum("DoesNotExistCollection") is None
