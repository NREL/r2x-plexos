"""Tests for PlexosProperty factory methods."""

from r2x_plexos.models.property import PLEXOSPropertyValue


def test_from_dict_scenarios():
    prop = PLEXOSPropertyValue.from_records([{"scenario": "Base", "value": 100}])
    assert prop.get_scenarios() == ["Base"]


def test_from_dict_timeslices():
    prop = PLEXOSPropertyValue.from_records(
        [
            {"timeslice": "Peak", "value": 150},
            {"timeslice": "OffPeak", "value": 100},
        ]
    )
    assert prop.get_timeslices() == ["OffPeak", "Peak"]


def test_from_dict_bands():
    prop = PLEXOSPropertyValue.from_records(
        [
            {"band": 1, "value": 100},
            {"band": 2, "value": 50},
        ]
    )
    assert prop.get_bands() == [1, 2]
