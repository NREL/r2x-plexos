"""Test variable resolution with constant values."""

from pathlib import Path

import pytest
from plexosdb import ClassEnum, CollectionEnum, PlexosDB

from r2x_core import DataFile, DataStore
from r2x_plexos import PLEXOSConfig, PLEXOSParser
from r2x_plexos.models import PLEXOSBattery, PLEXOSDatafile, PLEXOSObject, PLEXOSPropertyValue, PLEXOSVariable


@pytest.fixture
def xml_with_variables(tmp_path):
    """Create a test XML with a generator that has max capacity referencing a variable."""
    db: PlexosDB = PlexosDB.from_xml(Path("tests/data/5_bus_system_variables.xml"))
    datafile_path = tmp_path / "generator_capacity.csv"
    datafile_path.write_text("Name,Value\nTestBattery,100.0\n")
    datafile_name = "BatteryCapacities"
    datafile_id = db.add_object(ClassEnum.DataFile, datafile_name)
    db.add_property(
        ClassEnum.DataFile,
        "BatteryCapacities",
        "Filename",
        value=0,
        text={ClassEnum.DataFile: str(datafile_path)},
    )
    variable_name = "CapacityMultiplier"
    variable_id = db.add_object(ClassEnum.Variable, variable_name)
    variable_prop_id = db.add_property(
        ClassEnum.Variable,
        variable_name,
        "Profile",
        value=3.0,
    )
    db._db.execute(
        "INSERT INTO t_tag(object_id,data_id,action_id) VALUES (?,?,?)", (datafile_id, variable_prop_id, 1)
    )
    db._db.execute(
        "INSERT INTO t_band(band_id,data_id) VALUES (?,?)",
        (
            1,
            variable_prop_id,
        ),
    )

    battery = "TestBattery"
    db.add_object(ClassEnum.Battery, battery, collection_enum=CollectionEnum.Batteries)
    battery_max_power_id = db.add_property(
        ClassEnum.Battery,
        battery,
        "Max Power",
        value=0.0,  # Placeholder when using datafile+variable
        text={ClassEnum.DataFile: "BatteryCapacities"},
        collection_enum=CollectionEnum.Batteries,
    )
    db._db.execute("INSERT INTO t_band(band_id,data_id) VALUES (?,?)", (1, battery_max_power_id))
    db._db.execute(
        "INSERT INTO t_tag(object_id,data_id, action_id) VALUES (?,?,?)",
        (datafile_id, battery_max_power_id, 1),
    )
    battery_capacity_id = db.add_property(
        ClassEnum.Battery,
        battery,
        "Capacity",
        value=0.0,  # Placeholder when using datafile+variable
        collection_enum=CollectionEnum.Batteries,
    )
    db._db.execute("INSERT INTO t_band(band_id,data_id) VALUES (?,?)", (1, battery_capacity_id))
    db._db.execute(
        "INSERT INTO t_tag(object_id,data_id,action_id) VALUES (?,?,?)", (variable_id, battery_capacity_id, 1)
    )
    db._db.execute(
        "INSERT INTO t_tag(object_id,data_id,action_id) VALUES (?,?,?)", (datafile_id, battery_capacity_id, 0)
    )

    xml_path = tmp_path / "variable.xml"
    db.to_xml(xml_path)

    return xml_path


def test_battery_capacity_with_constant_variable(xml_with_variables, tmp_path, caplog):
    """Test generator max_capacity computed as base_value * variable_value."""
    config = PLEXOSConfig(model_name="Base", reference_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_with_variables)
    store = DataStore(folder=tmp_path)
    store.add_data_file(data_file)

    parser = PLEXOSParser(config, store)
    sys = parser.build_system()

    battery_component = sys.get_component(PLEXOSBattery, "TestBattery")
    datafile_component = sys.get_component(PLEXOSDatafile, "BatteryCapacities")
    variable_component = sys.get_component(PLEXOSVariable, "CapacityMultiplier")

    assert isinstance(battery_component, PLEXOSObject)
    assert isinstance(battery_component, PLEXOSBattery)

    max_power_property_value = battery_component.get_property_value("max_power")
    assert isinstance(max_power_property_value, PLEXOSPropertyValue)
    assert max_power_property_value.get_entry().datafile_name == datafile_component.name
    assert max_power_property_value.has_datafile()
    assert battery_component.max_power == 100.0

    capacity_property_value = battery_component.get_property_value("capacity")
    assert isinstance(capacity_property_value, PLEXOSPropertyValue)
    assert capacity_property_value.get_entry().variable_name == variable_component.name
    assert capacity_property_value.has_variable()
    assert battery_component.capacity == 100.0 * 3
    assert not sys.has_time_series(battery_component)
