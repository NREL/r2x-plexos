"""Test variable resolution with constant values."""

from pathlib import Path

import pytest
from plexosdb import ClassEnum, CollectionEnum, PlexosDB

from r2x_core import DataFile, DataStore
from r2x_plexos import PLEXOSConfig, PLEXOSParser
from r2x_plexos.models import CollectionProperties, PLEXOSRegion


@pytest.fixture
def xml_with_variables(tmp_path):
    """Create a test XML with a generator that has max capacity referencing a variable."""
    db: PlexosDB = PlexosDB.from_xml(Path("tests/data/5_bus_system_variables.xml"))
    datafile_path = tmp_path / "generator_capacity.csv"
    datafile_path.write_text("Name,Value\nTestBattery,100.0\n")
    region_name = "Region"
    region_id = db.add_object(ClassEnum.Region, region_name)
    reserve_name = "RegionSpinning"
    _ = db.add_object(ClassEnum.Reserve, reserve_name)
    db.add_membership(ClassEnum.Reserve, ClassEnum.Region, reserve_name, region_name, CollectionEnum.Regions)

    variable_name = "MinReserveMargin"
    variable_id = db.add_object(ClassEnum.Variable, variable_name)
    variable_prop_id = db.add_property(
        ClassEnum.Variable,
        variable_name,
        "Profile",
        value=3.0,
    )
    db._db.execute(
        "INSERT INTO t_tag(object_id,data_id,action_id) VALUES (?,?,?)", (region_id, variable_prop_id, 1)
    )
    db._db.execute(
        "INSERT INTO t_band(band_id,data_id) VALUES (?,?)",
        (
            1,
            variable_prop_id,
        ),
    )

    load_risk = db.add_property(
        ClassEnum.Region,
        region_name,
        "Load Risk",
        value=0.0,  # Placeholder when using datafile+variable
        collection_enum=CollectionEnum.Regions,
        parent_class_enum=ClassEnum.Reserve,
        parent_object_name=reserve_name,
    )
    db._db.execute("INSERT INTO t_band(band_id,data_id) VALUES (?,?)", (1, load_risk))
    db._db.execute(
        "INSERT INTO t_tag(object_id,data_id, action_id) VALUES (?,?,?)",
        (variable_id, load_risk, 1),
    )

    xml_path = tmp_path / "collection_properties.xml"
    db.to_xml(xml_path)

    return xml_path


def test_battery_capacity_with_constant_variable(xml_with_variables, tmp_path, caplog):
    """Test generator max_capacity computed as base_value * variable_value."""
    config = PLEXOSConfig(model_name="Base", reference_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_with_variables)
    store = DataStore(path=tmp_path)
    store.add_data(data_file)

    parser = PLEXOSParser(config, store)
    sys = parser.build_system()
    region = sys.get_component(PLEXOSRegion, "Region")

    assert sys.has_supplemental_attribute(region)
    assert len(sys.get_supplemental_attributes_with_component(region, CollectionProperties)) == 1
    sup = sys.get_supplemental_attributes_with_component(region, CollectionProperties)[0]
    assert "load_risk" in sup.properties
    assert sup.properties["load_risk"].get_value() == 3.0
