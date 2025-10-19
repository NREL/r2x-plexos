"""Test variable resolution with constant values."""

from pathlib import Path

import pytest
from plexosdb import ClassEnum, CollectionEnum, PlexosDB

from r2x_core import DataFile, DataStore, System
from r2x_plexos.config import PLEXOSConfig
from r2x_plexos.models import PLEXOSRegion
from r2x_plexos.models.variable import PLEXOSVariable
from r2x_plexos.parser import PLEXOSParser


@pytest.fixture
def multi_year_load(tmp_path):
    weather_years = [2020, 2021, 2022]
    fpaths = []
    for year in weather_years:
        csv_content = "Month,Day,Period,r1,r2\n"
        for period in range(1, 25):
            region_1_value = 1000 * period + year
            region_2_value = 1200 * period + year
            line = f"1,1,{period},{region_1_value},{region_2_value}\n"
            csv_content += line
        datafile_path = tmp_path / f"Load_{year}.csv"
        fpaths.append(str(datafile_path))
        datafile_path.write_text(csv_content)

    return fpaths


@pytest.fixture
def xml_with_multi_weather(tmp_path, multi_year_load):
    """Create a test XML with a generator that has max capacity referencing a variable."""
    db: PlexosDB = PlexosDB.from_xml(Path("tests/data/5_bus_system_variables.xml"))
    variable_name = "LoadProfiles"
    variable_id = db.add_object(ClassEnum.Variable, variable_name)
    scenarios = ["scenario_1", "scenario_2"]

    for band, fpath in enumerate(multi_year_load, start=1):
        for scenario in scenarios:
            db.add_property(
                ClassEnum.Variable,
                variable_name,
                "Profile",
                value=0,
                text={ClassEnum.DataFile: str(fpath)},
                band=band,
                scenario=scenario,
            )

    # Only add scenario 2 to the base for property resolution
    db.add_membership(ClassEnum.Model, ClassEnum.Scenario, "Base", "scenario_2", CollectionEnum.Scenarios)

    regions = ["r1", "r2"]
    db.add_objects(ClassEnum.Region, regions)
    for region in regions:
        region_prop_id = db.add_property(ClassEnum.Region, region, "Load", 0.0, band=1)
        db._db.execute(
            "INSERT INTO t_tag(object_id,data_id, action_id) VALUES (?,?,?)",
            (variable_id, region_prop_id, 0),
        )
    xml_path = tmp_path / "multi_weather.xml"
    db.to_xml(xml_path)
    return xml_path


def test_multi_band_datafile(tmp_path, xml_with_multi_weather, caplog):
    config = PLEXOSConfig(model_name="Base", reference_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_with_multi_weather)
    store = DataStore(folder=tmp_path)
    store.add_data_file(data_file)

    parser = PLEXOSParser(config, store)
    sys: System = parser.build_system()

    # Variable inspection
    variable_component = sys.get_component(PLEXOSVariable, "LoadProfiles")
    prop_value = variable_component.get_property_value("profile")

    assert prop_value.get_entry().scenario_name == "scenario_2"
    assert "Load_2020" in prop_value.get_entry().text
    assert prop_value.has_datafile()
    assert prop_value.has_bands()
    assert prop_value.has_scenarios()

    regions_to_inspect = ["r1", "r2"]
    for region in regions_to_inspect:
        region_component = sys.get_component(PLEXOSRegion, region)
        assert isinstance(region_component, PLEXOSRegion)
        prop_value = region_component.get_property_value("load")
        assert prop_value.has_variable()
        assert region_component.load != 0.0

        assert sys.has_time_series(region_component)
        assert len(sys.list_time_series(region_component)) == 3

        assert sys.has_time_series(region_component, band=1)
        band_1 = sys.get_time_series(region_component, band=1)
        assert len(band_1.data) == 8784
        assert all(band_1.data[:24] != 0)
