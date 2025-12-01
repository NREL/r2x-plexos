import pytest
from plexosdb import ClassEnum

pytestmark = pytest.mark.fixtures


def test_fixture_xmls(db_all_gen_types):
    from rich import print

    db = db_all_gen_types

    print(db.get_object_properties(ClassEnum.Generator, "thermal-01"))
    print(db.get_object_properties(ClassEnum.Generator, "solar-01"))
