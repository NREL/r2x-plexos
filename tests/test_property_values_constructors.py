"""Tests for PlexosProperty constructor methods."""

from r2x_plexos import PLEXOSPropertyValue


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


def test_get_bands_and_dates():
    from r2x_plexos import PLEXOSPropertyValue

    prop = PLEXOSPropertyValue.from_records(
        [{"band": 1, "value": 10, "date_from": "2024-01-01", "date_to": "2024-01-31"}]
    )
    assert prop.get_bands() == [1]
    assert prop.get_dates() == [("2024-01-01", "2024-01-31")]


def test_property_comparisons():
    from r2x_plexos import PLEXOSPropertyValue

    a = PLEXOSPropertyValue.from_records([{"value": 1}])
    b = PLEXOSPropertyValue.from_records([{"value": 2}])
    assert a < b or b < a  # __lt__
    assert a <= b or b <= a  # __le__
    assert a != b  # __eq__ and __ne__
    assert a > b or b > a  # __gt__
