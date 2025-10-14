import pytest
from infrasys import Component

from r2x_plexos import horizon, scenario_and_horizon, scenario_priority
from r2x_plexos.models.generator import PLEXOSGenerator
from r2x_plexos.models.property import PLEXOSPropertyValue


@pytest.mark.parametrize(
    "max_capacity,expected,priority",
    [
        (10, 10, None),
        (10.0, 10.0, None),
        (PLEXOSPropertyValue.from_dict({"value": 10.0}), 10.0, None),
        (PLEXOSPropertyValue.from_records([{"scenario": "test", "value": 10.0}]), 10.0, {"test": 1}),
        (
            PLEXOSPropertyValue.from_records([{"scenario": "test", "value": 10.0}, {"value": 11.0}]),
            11.0,
            None,
        ),
        (
            PLEXOSPropertyValue.from_records(
                [{"scenario": "test", "value": 10.0}, {"scenario": "test2", "value": 11.0}]
            ),
            10.0,
            {"test": 1, "test2": 2},
        ),
        (
            PLEXOSPropertyValue.from_records(
                [{"scenario": "test", "value": 10.0}, {"scenario": "test2", "value": 11.0}, {"value": 15}]
            ),
            10.0,
            {"test": 1, "test2": 2},
        ),
        (PLEXOSPropertyValue.from_dict({"time_slice": "M1", "value": 10.0}), 10.0, None),
        (
            PLEXOSPropertyValue.from_records(
                [{"time_slice": "M1", "value": 10.0}, {"time_slice": "M1", "value": 15.0, "scenario": "test"}]
            ),
            {"M1": 10.0},  # Should return scenario dict when there's a scenario without priority
            None,
        ),
        (
            PLEXOSPropertyValue.from_records(
                [{"time_slice": "M1", "value": 10.0}, {"time_slice": "M2", "value": 15.0}]
            ),
            {"M1": 10.0, "M2": 15.0},  # Should return timeslice dict when there are multiple timeslices
            None,
        ),
        (
            PLEXOSPropertyValue.from_records([{"band": 1, "value": 10.0}, {"band": 2, "value": 15.0}]),
            {1: 10.0, 2: 15.0},
            None,
        ),
        (
            PLEXOSPropertyValue.from_records(
                [{"band": 1, "value": 10.0}, {"band": 1, "value": 15.0, "scenario": "test"}]
            ),
            10.0,
            None,
        ),
        (
            PLEXOSPropertyValue.from_records(
                [{"band": 1, "value": 10.0}, {"band": 1, "value": 15.0, "scenario": "test"}]
            ),
            15.0,
            {"test": 1},
        ),
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"scenario": "s1", "time_slice": "M1", "value": 10.0},
                    {"scenario": "s2", "time_slice": "M2", "value": 20.0},
                ]
            ),
            {"s1": 10.0, "s2": 20.0},
            None,
        ),
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"scenario": "s1", "time_slice": "M1", "value": 10.0},
                    {"scenario": "s2", "time_slice": "M2", "value": 20.0},
                ]
            ),
            20.0,
            {"s2": 1, "s1": 2},
        ),
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"band": 1, "scenario": "s1", "value": 10.0},
                    {"band": 2, "scenario": "s1", "value": 20.0},
                ]
            ),
            {"s1": 10.0},
            None,
        ),
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"band": 1, "scenario": "s1", "value": 10.0},
                    {"band": 2, "scenario": "s1", "value": 20.0},
                ]
            ),
            10.0,
            {"s1": 1},
        ),
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"value": 5.0},
                    {"scenario": "s1", "value": 10.0},
                    {"time_slice": "M1", "value": 15.0},
                ]
            ),
            5.0,
            None,
        ),
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"scenario": "s1", "time_slice": "M1", "value": 10.0},
                    {"scenario": "s1", "time_slice": "M2", "value": 20.0},
                ]
            ),
            10.0,
            None,
        ),
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"scenario": "s1", "time_slice": "M1", "value": 10.0},
                    {"scenario": "s1", "time_slice": "M2", "value": 20.0},
                ]
            ),
            10.0,
            {"s1": 1},
        ),
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"scenario": "s1", "value": 10.0},
                    {"scenario": "s2", "time_slice": "M1", "value": 20.0},
                ]
            ),
            {"s1": 10.0, "s2": 20.0},
            None,
        ),
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"time_slice": "M1", "value": 10.0},
                    {"time_slice": "M2", "value": 15.0},
                    {"time_slice": "M3", "value": 20.0},
                ]
            ),
            {"M1": 10.0, "M2": 15.0, "M3": 20.0},
            None,
        ),
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"band": 1, "value": 10.0},
                    {"band": 2, "value": 15.0},
                    {"band": 3, "value": 20.0},
                ]
            ),
            {1: 10.0, 2: 15.0, 3: 20.0},
            None,
        ),
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"band": 1, "value": 10.0},
                    {"band": 2, "scenario": "s1", "value": 20.0},
                ]
            ),
            10.0,
            None,
        ),
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"scenario": "s1", "time_slice": "M1", "value": 10.0},
                    {"scenario": "s2", "time_slice": "M1", "value": 20.0},
                    {"scenario": "s3", "time_slice": "M1", "value": 30.0},
                ]
            ),
            {"s1": 10.0, "s2": 20.0, "s3": 30.0},
            None,
        ),
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"scenario": "s1", "value": 10.0},
                    {"scenario": "s2", "value": 20.0},
                    {"scenario": "s3", "value": 30.0},
                ]
            ),
            20.0,
            {"s2": 1, "s1": 2, "s3": 3},
        ),
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"time_slice": "M1", "band": 1, "value": 10.0},
                    {"time_slice": "M2", "band": 2, "value": 20.0},
                ]
            ),
            {"M1": 10.0, "M2": 20.0},
            None,
        ),
        # Edge case: Single date range
        (
            PLEXOSPropertyValue.from_records(
                [{"date_from": "2024-01-01", "date_to": "2024-12-31", "value": 10.0}]
            ),
            10.0,
            None,
        ),
        # Edge case: Multiple date ranges, no scenario
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"date_from": "2024-01-01", "date_to": "2024-06-30", "value": 10.0},
                    {"date_from": "2024-07-01", "date_to": "2024-12-31", "value": 20.0},
                ]
            ),
            10.0,
            None,
        ),
        # Edge case: Dates with scenario, no priority
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"scenario": "s1", "date_from": "2024-01-01", "date_to": "2024-12-31", "value": 10.0},
                    {"scenario": "s2", "date_from": "2024-01-01", "date_to": "2024-12-31", "value": 20.0},
                ]
            ),
            {"s1": 10.0, "s2": 20.0},
            None,
        ),
        # Edge case: Dates with scenario, with priority
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"scenario": "s1", "date_from": "2024-01-01", "date_to": "2024-12-31", "value": 10.0},
                    {"scenario": "s2", "date_from": "2024-01-01", "date_to": "2024-12-31", "value": 20.0},
                ]
            ),
            20.0,
            {"s2": 1, "s1": 2},
        ),
        # Edge case: Dates with timeslices
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"time_slice": "M1", "date_from": "2024-01-01", "date_to": "2024-06-30", "value": 10.0},
                    {"time_slice": "M2", "date_from": "2024-07-01", "date_to": "2024-12-31", "value": 20.0},
                ]
            ),
            {"M1": 10.0, "M2": 20.0},
            None,
        ),
        # Edge case: Default value with dated scenario
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"value": 5.0},
                    {"scenario": "s1", "date_from": "2024-01-01", "date_to": "2024-12-31", "value": 10.0},
                ]
            ),
            5.0,
            None,
        ),
        # Edge case: Scenario with dates and timeslices, no priority
        (
            PLEXOSPropertyValue.from_records(
                [
                    {
                        "scenario": "s1",
                        "time_slice": "M1",
                        "date_from": "2024-01-01",
                        "date_to": "2024-12-31",
                        "value": 10.0,
                    },
                    {
                        "scenario": "s2",
                        "time_slice": "M2",
                        "date_from": "2024-01-01",
                        "date_to": "2024-12-31",
                        "value": 20.0,
                    },
                ]
            ),
            {"s1": 10.0, "s2": 20.0},
            None,
        ),
        # Edge case: Scenario with dates and timeslices, with priority
        (
            PLEXOSPropertyValue.from_records(
                [
                    {
                        "scenario": "s1",
                        "time_slice": "M1",
                        "date_from": "2024-01-01",
                        "date_to": "2024-12-31",
                        "value": 10.0,
                    },
                    {
                        "scenario": "s2",
                        "time_slice": "M2",
                        "date_from": "2024-01-01",
                        "date_to": "2024-12-31",
                        "value": 20.0,
                    },
                ]
            ),
            10.0,
            {"s1": 1, "s2": 2},
        ),
    ],
    ids=[
        "scalar_int",
        "scalar_float",
        "property_simple_value",
        "1_scenario_with_priority",
        "1_scenario_with_default_no_priority",
        "2_scenarios_with_priority",
        "2_scenarios_with_default_with_priority",
        "1_timeslice_no_priority",
        "1_timeslice_with_scenario_no_priority",
        "2_timeslices_no_priority",
        "2_bands_no_priority",
        "same_bands_with_scenario_no_priority",
        "same_bands_with_scenario_priority",
        "multi_scenario_multi_timeslice_no_priority",
        "multi_scenario_multi_timeslice_with_priority",
        "multi_band_with_scenario_no_priority",
        "multi_band_with_scenario_with_priority",
        "mixed_default_scenario_timeslice_no_priority",
        "same_scenario_multi_timeslice_no_priority",
        "same_scenario_multi_timeslice_with_priority",
        "multi_scenario_one_with_timeslice_no_priority",
        "default_with_3_timeslices",
        "default_with_3_bands",
        "scenario_with_bands_prefer_default",
        "3_scenarios_same_timeslice_no_priority",
        "3_scenarios_middle_priority_wins",
        "timeslices_with_different_bands",
        "single_date_range",
        "multiple_date_ranges_no_scenario",
        "dates_with_scenario_no_priority",
        "dates_with_scenario_with_priority",
        "dates_with_timeslices",
        "default_with_dated_scenario",
        "scenario_dates_timeslices_no_priority",
        "scenario_dates_timeslices_with_priority",
    ],
)
def test_generator(max_capacity, expected, priority):
    component = PLEXOSGenerator(name="test", max_capacity=max_capacity)
    with scenario_priority(priority):
        assert isinstance(component, Component)
        assert component.max_capacity == expected


@pytest.mark.parametrize(
    "max_capacity,horizon_range,expected,priority",
    [
        # Horizon + Scenarios
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"scenario": "s1", "date_from": "2024-01-01", "date_to": "2024-06-30", "value": 10.0},
                    {"scenario": "s1", "date_from": "2024-07-01", "date_to": "2024-12-31", "value": 20.0},
                ]
            ),
            ("2024-01-01", "2024-06-30"),
            10.0,
            None,
        ),
        # Horizon + Scenarios + Priority
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"scenario": "s1", "date_from": "2024-01-01", "date_to": "2024-12-31", "value": 10.0},
                    {"scenario": "s2", "date_from": "2024-01-01", "date_to": "2024-12-31", "value": 20.0},
                ]
            ),
            ("2024-01-01", "2024-12-31"),
            20.0,
            {"s2": 1, "s1": 2},
        ),
        # Horizon + Timeslices
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"time_slice": "M1", "date_from": "2024-01-01", "date_to": "2024-06-30", "value": 10.0},
                    {"time_slice": "M2", "date_from": "2024-07-01", "date_to": "2024-12-31", "value": 20.0},
                ]
            ),
            ("2024-01-01", "2024-06-30"),
            10.0,
            None,
        ),
        # Horizon + Bands
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"band": 1, "date_from": "2024-01-01", "date_to": "2024-06-30", "value": 10.0},
                    {"band": 2, "date_from": "2024-07-01", "date_to": "2024-12-31", "value": 20.0},
                ]
            ),
            ("2024-01-01", "2024-06-30"),
            10.0,
            None,
        ),
        # Horizon + Scenarios + Timeslices
        (
            PLEXOSPropertyValue.from_records(
                [
                    {
                        "scenario": "s1",
                        "time_slice": "M1",
                        "date_from": "2024-01-01",
                        "date_to": "2024-06-30",
                        "value": 10.0,
                    },
                    {
                        "scenario": "s2",
                        "time_slice": "M2",
                        "date_from": "2024-07-01",
                        "date_to": "2024-12-31",
                        "value": 20.0,
                    },
                ]
            ),
            ("2024-01-01", "2024-06-30"),
            10.0,
            None,
        ),
        # Horizon + Scenarios + Timeslices + Priority
        (
            PLEXOSPropertyValue.from_records(
                [
                    {
                        "scenario": "s1",
                        "time_slice": "M1",
                        "date_from": "2024-01-01",
                        "date_to": "2024-12-31",
                        "value": 10.0,
                    },
                    {
                        "scenario": "s2",
                        "time_slice": "M1",
                        "date_from": "2024-01-01",
                        "date_to": "2024-12-31",
                        "value": 20.0,
                    },
                ]
            ),
            ("2024-01-01", "2024-12-31"),
            20.0,
            {"s2": 1, "s1": 2},
        ),
        # Horizon + Scenarios + Bands
        (
            PLEXOSPropertyValue.from_records(
                [
                    {
                        "scenario": "s1",
                        "band": 1,
                        "date_from": "2024-01-01",
                        "date_to": "2024-12-31",
                        "value": 10.0,
                    },
                    {
                        "scenario": "s2",
                        "band": 2,
                        "date_from": "2024-01-01",
                        "date_to": "2024-12-31",
                        "value": 20.0,
                    },
                ]
            ),
            ("2024-01-01", "2024-12-31"),
            {"s1": 10.0, "s2": 20.0},
            None,
        ),
        # Horizon filters out entries
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"scenario": "s1", "date_from": "2024-01-01", "date_to": "2024-06-30", "value": 10.0},
                    {"scenario": "s2", "date_from": "2024-07-01", "date_to": "2024-12-31", "value": 20.0},
                ]
            ),
            ("2024-01-01", "2024-06-30"),
            10.0,
            None,
        ),
        # Horizon with default value
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"value": 5.0},
                    {"scenario": "s1", "date_from": "2024-01-01", "date_to": "2024-12-31", "value": 10.0},
                ]
            ),
            ("2024-01-01", "2024-12-31"),
            5.0,
            None,
        ),
        # Horizon excludes all dated entries, returns default
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"value": 5.0},
                    {"scenario": "s1", "date_from": "2025-01-01", "date_to": "2025-12-31", "value": 10.0},
                ]
            ),
            ("2024-01-01", "2024-12-31"),
            5.0,
            None,
        ),
        # Horizon + multiple timeslices same period
        (
            PLEXOSPropertyValue.from_records(
                [
                    {"time_slice": "M1", "date_from": "2024-01-01", "date_to": "2024-12-31", "value": 10.0},
                    {"time_slice": "M2", "date_from": "2024-01-01", "date_to": "2024-12-31", "value": 20.0},
                ]
            ),
            ("2024-01-01", "2024-12-31"),
            {"M1": 10.0, "M2": 20.0},
            None,
        ),
        # Horizon + Scenarios + Timeslices + Bands (full combination)
        (
            PLEXOSPropertyValue.from_records(
                [
                    {
                        "scenario": "s1",
                        "time_slice": "M1",
                        "band": 1,
                        "date_from": "2024-01-01",
                        "date_to": "2024-06-30",
                        "value": 10.0,
                    },
                    {
                        "scenario": "s2",
                        "time_slice": "M2",
                        "band": 2,
                        "date_from": "2024-07-01",
                        "date_to": "2024-12-31",
                        "value": 20.0,
                    },
                ]
            ),
            ("2024-01-01", "2024-06-30"),
            10.0,
            None,
        ),
    ],
    ids=[
        "horizon_scenario_filter",
        "horizon_scenario_priority",
        "horizon_timeslices",
        "horizon_bands",
        "horizon_scenario_timeslice",
        "horizon_scenario_timeslice_priority",
        "horizon_scenario_bands",
        "horizon_filters_scenarios",
        "horizon_with_default",
        "horizon_excludes_all_dated",
        "horizon_multiple_timeslices",
        "horizon_full_combination",
    ],
)
def test_generator_with_horizon(max_capacity, horizon_range, expected, priority):
    """Test generator with horizon context manager for date filtering."""
    component = PLEXOSGenerator(name="test", max_capacity=max_capacity)
    date_from, date_to = horizon_range

    if priority:
        with scenario_and_horizon(priority, date_from, date_to):
            assert isinstance(component, Component)
            assert component.max_capacity == expected
    else:
        with horizon(date_from, date_to):
            assert isinstance(component, Component)
            assert component.max_capacity == expected
