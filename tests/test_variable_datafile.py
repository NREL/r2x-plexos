"""Test variable resolution with constant values."""

from r2x_core import DataFile, DataStore
from r2x_plexos import PLEXOSConfig, PLEXOSParser
from r2x_plexos.models.datafile import PLEXOSDatafile
from r2x_plexos.models.generator import PLEXOSGenerator
from r2x_plexos.models.property import PLEXOSPropertyValue


def test_variable_timeseries(db_with_variable_monthly, tmp_path):
    db = db_with_variable_monthly

    xml_path = tmp_path / "variable_ts.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", reference_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data(data_file)

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
#     store = DataStore(path=tmp_path)
#     store.add_data(data_file)

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
