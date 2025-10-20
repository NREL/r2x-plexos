"""Test variable resolution with constant values."""

from pathlib import Path

import pytest
from plexosdb import ClassEnum, CollectionEnum, PlexosDB

from r2x_core import DataFile, DataStore
from r2x_plexos import PLEXOSConfig, PLEXOSParser
from r2x_plexos.models.datafile import PLEXOSDatafile
from r2x_plexos.models.generator import PLEXOSGenerator
from r2x_plexos.models.property import PLEXOSPropertyValue


@pytest.fixture
def xml_with_variables(tmp_path):
    """Create a test XML with a generator that has max capacity referencing a variable."""
    db: PlexosDB = PlexosDB.from_xml(Path("tests/data/5_bus_system_variables.xml"))
    datafile_path = tmp_path / "generator_rating.csv"
    csv_content = (
        "Name,M01,M02,M03,M04,M05,M06,M07,M08,M09,M10,M11,M12\n"
        "TestGen,25.87,62.48,30.42,42.26,39.34,36.25,32.79,44.44,35.34,21.86,20.77,20.95\n"
    )
    datafile_path.write_text(csv_content, encoding="utf-8")

    datafile_name = "Ratings"
    datafile_id = db.add_object(ClassEnum.DataFile, datafile_name)
    db.add_property(
        ClassEnum.DataFile,
        datafile_name,
        "Filename",
        value=0,
        text={ClassEnum.DataFile: str(datafile_path)},
    )
    variable_name = "Rating Multiplier"
    variable_id = db.add_object(ClassEnum.Variable, variable_name)
    variable_prop_id = db.add_property(
        ClassEnum.Variable,
        variable_name,
        "Profile",
        value=1,
    )
    db._db.execute(
        "INSERT INTO t_tag(object_id,data_id,action_id) VALUES (?,?,?)", (datafile_id, variable_prop_id, 2)
    )
    db._db.execute(
        "INSERT INTO t_band(band_id,data_id) VALUES (?,?)",
        (
            1,
            variable_prop_id,
        ),
    )

    generator = "TestGen"
    db.add_object(ClassEnum.Generator, generator, collection_enum=CollectionEnum.Generators)
    generator_rating_id = db.add_property(
        ClassEnum.Generator,
        generator,
        "Rating",
        value=0.0,  # Placeholder when using datafile+variable
        text={ClassEnum.DataFile: datafile_name},
        collection_enum=CollectionEnum.Generators,
    )
    db._db.execute("INSERT INTO t_band(band_id,data_id) VALUES (?,?)", (1, generator_rating_id))
    db._db.execute(
        "INSERT INTO t_tag(object_id,data_id, action_id) VALUES (?,?,?)",
        (datafile_id, generator_rating_id, 1),
    )
    db._db.execute(
        "INSERT INTO t_tag(object_id,data_id, action_id) VALUES (?,?,?)",
        (variable_id, generator_rating_id, 1),
    )

    xml_path = tmp_path / "variable_timeseries.xml"
    db.to_xml(xml_path)

    return xml_path


def test_variable_timeseries(xml_with_variables, tmp_path, caplog):
    config = PLEXOSConfig(model_name="Base", reference_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_with_variables)
    store = DataStore(folder=tmp_path)
    store.add_data_file(data_file)

    parser = PLEXOSParser(config, store)
    sys = parser.build_system()
    generator_component = sys.get_component(PLEXOSGenerator, "TestGen")
    datafile_component = sys.get_component(PLEXOSDatafile, "Ratings")

    rating_value = generator_component.get_property_value("rating")
    assert isinstance(rating_value, PLEXOSPropertyValue)
    assert rating_value.get_entry().datafile_name == datafile_component.name
    assert rating_value.has_datafile()
    assert generator_component.rating == 62.48

    assert sys.has_time_series(generator_component)
    assert len(sys.list_time_series(generator_component)) == 1
    ts = sys.get_time_series(generator_component)
    assert all(ts.data[:100] == 25.87)
    assert all(ts.data[-100:] == 20.95)


# def test_battery_capacity_with_constant_variable(xml_with_variables, tmp_path, caplog):
#     """Test generator max_capacity computed as base_value * variable_value."""
#     config = PLEXOSConfig(model_name="Base", reference_year=2024)
#     data_file = DataFile(name="xml_file", fpath=xml_with_variables)
#     store = DataStore(folder=tmp_path)
#     store.add_data_file(data_file)

#     parser = PLEXOSParser(config, store)
#     sys = parser.build_system()

#     battery_component = sys.get_component(PLEXOSBattery, "TestBattery")
#     datafile_component = sys.get_component(PLEXOSDatafile, "BatteryCapacities")
#     variable_component = sys.get_component(PLEXOSVariable, "CapacityMultiplier")

#     assert isinstance(battery_component, PLEXOSObject)
#     assert isinstance(battery_component, PLEXOSBattery)

#     max_power_property_value = battery_component.get_property_value("max_power")
#     assert isinstance(max_power_property_value, PLEXOSPropertyValue)
#     assert max_power_property_value.get_entry().datafile_name == datafile_component.name
#     assert max_power_property_value.has_datafile()
#     assert battery_component.max_power == 100.0

#     capacity_property_value = battery_component.get_property_value("capacity")
#     assert isinstance(capacity_property_value, PLEXOSPropertyValue)
#     assert capacity_property_value.get_entry().variable_name == variable_component.name
#     assert capacity_property_value.has_variable()
#     assert battery_component.capacity == 100.0 * 3
#     assert not sys.has_timeseries(battery_component)
