"""Test variable resolution with constant values."""

from r2x_core import DataStore, System
from r2x_plexos.config import PLEXOSConfig
from r2x_plexos.models import PLEXOSRegion
from r2x_plexos.models.datafile import PLEXOSDatafile
from r2x_plexos.parser import PLEXOSParser


def test_multi_band_datafile(tmp_path, db_with_multiband_variable, caplog):
    db = db_with_multiband_variable
    config = PLEXOSConfig(model_name="Base")
    store = DataStore(path=tmp_path)

    parser = PLEXOSParser(config, store, db=db)
    sys: System = parser.build_system()

    # Variable inspection
    variable_component = sys.get_component(PLEXOSDatafile, "LoadProfiles")
    prop_value = variable_component.get_property_value("filename")

    assert prop_value.get_entry().scenario_name == "scenario_2"
    assert "Load_" in prop_value.get_entry().text
    assert prop_value.has_datafile()
    assert prop_value.has_scenarios()

    regions_to_inspect = ["r1", "r2"]
    for region in regions_to_inspect:
        region_component = sys.get_component(PLEXOSRegion, region)
        assert isinstance(region_component, PLEXOSRegion)
        prop_value = region_component.get_property_value("load")
        assert prop_value.has_variable()

        assert sys.has_time_series(region_component)
        assert len(sys.list_time_series(region_component)) == 3

        band_1_ts = sys.get_time_series(region_component, band=1)
        assert len(band_1_ts.data) == 8784
        assert all(val != 0 for val in band_1_ts.data[:24])
