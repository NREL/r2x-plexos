import pytest
from plexosdb import ClassEnum, CollectionEnum, PlexosDB

from r2x_plexos.utils_plexosdb import get_collection_enum, get_collection_name


@pytest.fixture
def empty_db(data_folder):
    db = PlexosDB.from_xml(data_folder.joinpath("5_bus_system_variables.xml"))
    return db


def test_plexosdb_get_collection_name(empty_db):
    collection = CollectionEnum.Generators
    collection_id = empty_db.get_collection_id(collection, ClassEnum.System, ClassEnum.Generator)
    collection_name_returned = get_collection_name(empty_db, collection_id)
    assert collection_name_returned == collection
    assert not get_collection_name(empty_db, 1000)


def test_plexosdb_get_collection_enum():
    assert get_collection_enum("Generators") == CollectionEnum.Generators
    assert not get_collection_enum("Not")
