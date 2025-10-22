"""Tests for multi-band property support."""

from r2x_core import DataFile, DataStore
from r2x_plexos import PLEXOSConfig, PLEXOSParser, PLEXOSPropertyValue, scenario_priority
from r2x_plexos.models.generator import PLEXOSGenerator


def test_heat_rate_multi_band_no_priority():
    """Test that multi-banded Heat Rate returns dict of all bands without scenario priority."""
    heat_rate_prop = PLEXOSPropertyValue.from_records(
        [
            {"band": 1, "value": 10.0},
            {"band": 2, "value": 11.0},
            {"band": 3, "value": 12.0},
            {"band": 4, "value": 13.0},
        ],
        units="GJ/MWh",
    )

    gen = PLEXOSGenerator(name="ThermalGen1", heat_rate=heat_rate_prop)

    result = gen.heat_rate

    assert isinstance(result, dict)
    assert result == {1: 10.0, 2: 11.0, 3: 12.0, 4: 13.0}

    assert isinstance(gen.__dict__["heat_rate"], PLEXOSPropertyValue)
    assert gen.get_property_value("heat_rate").get_bands() == [1, 2, 3, 4]


def test_heat_rate_single_band_returns_value():
    """Test that single-band Heat Rate returns float value, not dict."""
    heat_rate_prop = PLEXOSPropertyValue.from_records(
        [{"band": 1, "value": 10.5}],
        units="GJ/MWh",
    )

    gen = PLEXOSGenerator(name="ThermalGen2", heat_rate=heat_rate_prop)

    result = gen.heat_rate

    assert isinstance(result, float)
    assert result == 10.5


def test_heat_rate_multi_band_with_priority():
    """Test that multi-band property with scenario priority returns single value."""
    heat_rate_prop = PLEXOSPropertyValue.from_records(
        [
            {"scenario": "Base", "band": 1, "value": 10.0},
            {"scenario": "Base", "band": 2, "value": 11.0},
            {"scenario": "High", "band": 1, "value": 12.0},
            {"scenario": "High", "band": 2, "value": 13.0},
        ],
        units="GJ/MWh",
    )

    gen = PLEXOSGenerator(name="ThermalGen3", heat_rate=heat_rate_prop)

    with scenario_priority({"Base": 1, "High": 2}):
        result = gen.heat_rate
        assert isinstance(result, float)
        assert result == 12.0  # High scenario, band 1 (default band)


def test_has_bands_method():
    """Test that has_bands() correctly identifies multi-band properties."""
    multi_band_prop = PLEXOSPropertyValue.from_records(
        [
            {"band": 1, "value": 10.0},
            {"band": 2, "value": 11.0},
            {"band": 3, "value": 12.0},
        ],
        units="GJ/MWh",
    )
    assert multi_band_prop.has_bands() is True

    single_band_prop = PLEXOSPropertyValue.from_records(
        [{"band": 1, "value": 10.0}],
        units="GJ/MWh",
    )
    assert single_band_prop.has_bands() is False

    default_prop = PLEXOSPropertyValue.from_records(
        [{"value": 10.0}],
        units="GJ/MWh",
    )
    assert default_prop.has_bands() is False


def test_parser_multiband_heat_rate(db_thermal_gen_multiband, tmp_path):
    db = db_thermal_gen_multiband

    xml_path = tmp_path / "multiband.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", reference_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(folder=tmp_path)
    store.add_data_file(data_file)

    parser = PLEXOSParser(config, store)
    system = parser.build_system()

    gen = system.get_component(PLEXOSGenerator, "thermal-01")
    assert gen is not None

    heat_rate_prop = gen.get_property_value("heat_rate")
    assert heat_rate_prop is not None
    assert heat_rate_prop.has_bands() is True
    assert heat_rate_prop.get_bands() == [1, 2, 3]

    result = gen.heat_rate
    assert isinstance(result, dict)
    assert result == {1: 10.5, 2: 11.5, 3: 12.5}


def test_multiband_with_scenario_priority_no_scenarios():
    """Test that multi-band properties without scenarios return dict even with priority context."""
    # Create multi-band property without scenarios
    heat_rate_prop = PLEXOSPropertyValue.from_records(
        [
            {"band": 1, "value": 50.0},
            {"band": 2, "value": 100.0},
            {"band": 3, "value": 200.0},
            {"band": 4, "value": 400.0},
            {"band": 5, "value": 800.0},
        ],
        units="GJ/MWh",
    )

    gen = PLEXOSGenerator(name="TestGen", heat_rate=heat_rate_prop)

    # Set scenario priority context (simulates parser.build_system() behavior)
    with scenario_priority({"Scenario1": 1, "Scenario2": 2}):
        # Even with priority context, multi-band property without scenarios
        # should return dict of all bands, not just band 1
        result = gen.heat_rate
        assert isinstance(result, dict), f"Expected dict, got {type(result)}"
        assert result == {1: 50.0, 2: 100.0, 3: 200.0, 4: 400.0, 5: 800.0}, (
            f"Multi-band property without scenarios should return all bands even with priority context, got {result}"
        )
