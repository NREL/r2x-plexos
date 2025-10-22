"""Test variable resolution with constant values."""

from r2x_core import DataFile, DataStore, System
from r2x_plexos.config import PLEXOSConfig
from r2x_plexos.models import PLEXOSRegion
from r2x_plexos.models.variable import PLEXOSVariable
from r2x_plexos.parser import PLEXOSParser


def test_multi_band_datafile(tmp_path, db_with_multiband_variable):
    db = db_with_multiband_variable

    xml_path = tmp_path / "multiband_var.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", reference_year=2020)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(folder=tmp_path)
    store.add_data_file(data_file)

    parser = PLEXOSParser(config, store)
    sys: System = parser.build_system()

    variable_component = sys.get_component(PLEXOSVariable, "LoadProfiles")
    prop_value = variable_component.get_property_value("profile")

    assert prop_value.has_datafile()
    assert prop_value.has_bands()
    assert prop_value.has_scenarios()

    entry = prop_value.get_entry()
    assert entry.scenario_name == "scenario_2"
    assert entry.text is not None
    assert "Load_" in entry.text

    regions_to_inspect = ["r1", "r2"]
    for region in regions_to_inspect:
        region_component = sys.get_component(PLEXOSRegion, region)
        assert isinstance(region_component, PLEXOSRegion)
        prop_value = region_component.get_property_value("load")
        assert prop_value.has_variable()

        assert sys.has_time_series(region_component)
        assert len(sys.list_time_series(region_component)) == 3

        assert sys.has_time_series(region_component, band=1)
        band_1 = sys.get_time_series(region_component, band=1)
        assert len(band_1.data) == 8784
        assert all(band_1.data[:24] != 0)
