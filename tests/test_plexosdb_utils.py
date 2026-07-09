import pytest
from plexosdb import ClassEnum, CollectionEnum, PlexosDB

from r2x_plexos.utils_plexosdb import (
    get_collection_enum,
    get_collection_name,
    ole_date_to_datetime,
    validate_simulation_attribute,
)


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


def test_ole_date_to_datetime_known_value():
    """January 1, 2030 corresponds to OLE date 47484."""
    from datetime import datetime

    result = ole_date_to_datetime(47484.0)
    assert result == datetime(2030, 1, 1, 0, 0)


def test_ole_date_to_datetime_epoch():
    """OLE epoch 0 corresponds to December 30, 1899."""
    from datetime import datetime

    result = ole_date_to_datetime(0.0)
    assert result == datetime(1899, 12, 30, 0, 0)


def test_ole_date_to_datetime_roundtrip():
    """ole_date_to_datetime should invert datetime_to_ole_date."""
    from datetime import datetime

    from r2x_plexos.utils_simulation import datetime_to_ole_date

    dt = datetime(2024, 6, 15, 12, 0, 0)
    ole = datetime_to_ole_date(dt)
    result = ole_date_to_datetime(ole)
    assert result == dt


@pytest.fixture
def template_db():
    """Minimal PlexosDB loaded from the bundled master template."""
    import pathlib

    template_path = (
        pathlib.Path(__file__).resolve().parent.parent
        / "src"
        / "r2x_plexos"
        / "config"
        / "master_10.0R2_btu.xml"
    )
    if not template_path.exists():
        pytest.skip("Template XML not found")
    return PlexosDB.from_xml(xml_path=template_path)


@pytest.mark.slow
def test_validate_simulation_attribute_valid(template_db):
    result = validate_simulation_attribute(template_db, ClassEnum.Performance, "SOLVER")
    assert result.is_ok()


@pytest.mark.slow
def test_validate_simulation_attribute_invalid(template_db):
    result = validate_simulation_attribute(template_db, ClassEnum.Performance, "NOT_A_REAL_ATTR")
    assert result.is_err()
    assert "NOT_A_REAL_ATTR" in result.unwrap_err()


def test_validate_simulation_attribute_exception_path():
    """Passing a non-PlexosDB object triggers the except branch → Err."""
    result = validate_simulation_attribute(object(), ClassEnum.Performance, "SOLVER")  # type: ignore[arg-type]
    assert result.is_err()
