"""Test variable resolution with constant values."""

import datetime
import random
from pathlib import Path

import pytest
from plexosdb import ClassEnum, CollectionEnum, PlexosDB

from r2x_core import DataFile, DataStore, System
from r2x_plexos.config import PLEXOSConfig
from r2x_plexos.models import PLEXOSRegion
from r2x_plexos.models.datafile import PLEXOSDatafile
from r2x_plexos.parser import PLEXOSParser


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + datetime.timedelta(n)


def datetime_to_ole_date(dt: datetime) -> float:
    """
    Converts a Python datetime object to an OLE Automation Date (float).

    :param dt: The datetime object to convert.
    :return: The OLE Automation Date as a float.
    """
    # The OLE Automation Date epoch is midnight, December 30, 1899
    ole_epoch = datetime.datetime(1899, 12, 30, 0, 0, 0)

    time_difference = dt - ole_epoch

    ole_date = time_difference.total_seconds() / (24.0 * 60.0 * 60.0)

    return ole_date


@pytest.fixture
def year_daily_hour(tmp_path: Path):
    start_year = 2026
    start_date = datetime.date(start_year, 1, 1)
    end_date = datetime.date(2030, 12, 31)

    hourly_columns = [f"{i + 1}" for i in range(24)]
    header = "Year,Month,Day," + ",".join(hourly_columns) + "\n"

    data_lines = [header]

    # Simplified loop using the daterange generator
    for date in daterange(start_date, end_date):
        row_elements = [str(date.year), str(date.month), str(date.day)]
        hourly_data = [random.randint(100, 50000) for _ in range(24)]
        row_elements.extend(list(map(str, hourly_data)))
        data_lines.append(",".join(row_elements) + "\n")

    output_fpath = tmp_path / "year_daily_hour.csv"
    output_fpath.write_text("".join(data_lines))
    return output_fpath


@pytest.fixture
def xml_with_multi_weather_chrono(tmp_path, year_daily_hour):
    """Create a test XML with a generator that has max capacity referencing a variable."""
    db: PlexosDB = PlexosDB.from_xml(Path("tests/data/5_bus_system_variables.xml"))
    datafile_name = "LoadProfiles"
    datafile_id = db.add_object(ClassEnum.DataFile, datafile_name)
    scenarios = ["scenario_1", "scenario_2", "scenario_3"]

    for scenario in scenarios:
        db.add_property(
            ClassEnum.DataFile,
            datafile_name,
            "Filename",
            value=0,
            datafile_text=str(year_daily_hour),
            band=1,
            scenario=scenario,
        )

    db.add_membership(ClassEnum.Model, ClassEnum.Scenario, "Base", "scenario_2", CollectionEnum.Scenarios)

    regions = ["r1", "r2"]
    db.add_objects(ClassEnum.Region, regions)
    for region in regions:
        region_prop_id = db.add_property(ClassEnum.Region, region, "Load", 0.0, band=1)
        db._db.execute(
            "INSERT INTO t_tag(object_id,data_id, action_id) VALUES (?,?,?)",
            (datafile_id, region_prop_id, 0),
        )

    db.add_object(ClassEnum.Model, "TestModel")
    db.add_object(ClassEnum.Horizon, "horizon")
    db.add_membership(
        ClassEnum.Model, ClassEnum.Scenario, "TestModel", "scenario_2", CollectionEnum.Scenarios
    )
    db.add_membership(ClassEnum.Model, ClassEnum.Horizon, "TestModel", "horizon", CollectionEnum.Horizon)
    db.add_attribute(
        ClassEnum.Horizon,
        "horizon",
        attribute_name="Chrono Date From",
        attribute_value=datetime_to_ole_date(datetime.datetime(2030, 1, 1)),
    )
    db.add_attribute(
        ClassEnum.Horizon, "horizon", attribute_name="Chrono Step Type", attribute_value=2
    )  # "-1;\"Second\";0;\"Minute\";1;\"Hour\";2;\"Day\";3;\"Week\""
    db.add_attribute(
        ClassEnum.Horizon, "horizon", attribute_name="Chrono Step Count", attribute_value=5
    )  # Total count of steps

    xml_path = tmp_path / "year_daily_hour.xml"
    db.to_xml(xml_path)
    return xml_path


def test_multi_band_datafile(tmp_path, xml_with_multi_weather_chrono, caplog):
    config = PLEXOSConfig(model_name="TestModel", reference_year=2026)
    data_file = DataFile(name="xml_file", fpath=xml_with_multi_weather_chrono)
    store = DataStore(path=tmp_path)
    store.add_data(data_file)

    parser = PLEXOSParser(config, store)
    sys: System = parser.build_system()

    # Variable inspection
    variable_component = sys.get_component(PLEXOSDatafile, "LoadProfiles")
    prop_value = variable_component.get_property_value("filename")

    assert prop_value.get_entry().scenario_name == "scenario_2"
    assert "year_daily_hour" in prop_value.get_entry().text
    assert prop_value.has_datafile()
    assert prop_value.has_scenarios()

    regions_to_inspect = ["r1", "r2"]
    for region in regions_to_inspect:
        region_component = sys.get_component(PLEXOSRegion, region)
        assert isinstance(region_component, PLEXOSRegion)
        prop_value = region_component.get_property_value("load")
        assert prop_value.has_datafile()
        assert region_component.load != 0.0

        assert sys.has_time_series(region_component)
        assert len(sys.list_time_series(region_component)) == 1

        ts_list = sys.list_time_series(region_component)
        ts = ts_list[0]
        assert len(ts.data) == 120  # 5 days from the chronoology
        assert all(val != 0 for val in ts.data[:120])  # Check first 5 days hours are non-zero
