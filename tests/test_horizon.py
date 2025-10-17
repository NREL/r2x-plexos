"""Tests for horizon context manager and date filtering."""

from r2x_plexos import horizon, scenario_and_horizon, scenario_priority
from r2x_plexos.models.generator import PLEXOSGenerator
from r2x_plexos.models.property import PLEXOSPropertyValue


def test_horizon_filters_by_date_range():
    """Test that horizon filters entries to those within the date range."""
    prop = PLEXOSPropertyValue.from_records(
        [
            {"date_from": "2024-01-01", "date_to": "2024-06-30", "value": 10.0},
            {"date_from": "2024-07-01", "date_to": "2024-12-31", "value": 20.0},
        ]
    )
    gen = PLEXOSGenerator(name="test", max_capacity=prop)

    assert gen.max_capacity == 10.0

    with horizon("2024-01-01", "2024-06-30"):
        assert gen.max_capacity == 10.0

    with horizon("2024-07-01", "2024-12-31"):
        assert gen.max_capacity == 20.0

    with horizon("2024-05-01", "2024-06-15"):
        assert gen.max_capacity == 10.0


def test_horizon_includes_entries_without_dates():
    """Test that entries without dates are included regardless of horizon."""
    prop = PLEXOSPropertyValue.from_records(
        [
            {"value": 5.0},  # No dates - applies to all periods
            {"date_from": "2024-07-01", "date_to": "2024-12-31", "value": 20.0},
        ]
    )
    gen = PLEXOSGenerator(name="test", max_capacity=prop)

    assert gen.max_capacity == 5.0

    with horizon("2024-07-01", "2024-12-31"):
        assert gen.max_capacity == 5.0

    with horizon("2025-01-01", "2025-12-31"):
        assert gen.max_capacity == 5.0


def test_horizon_with_scenarios_no_priority():
    """Test horizon with multiple scenarios without priority."""
    prop = PLEXOSPropertyValue.from_records(
        [
            {"scenario": "s1", "date_from": "2024-01-01", "date_to": "2024-12-31", "value": 10.0},
            {"scenario": "s2", "date_from": "2024-01-01", "date_to": "2024-12-31", "value": 20.0},
        ]
    )
    gen = PLEXOSGenerator(name="test", max_capacity=prop)

    assert gen.max_capacity == {"s1": 10.0, "s2": 20.0}

    with horizon("2024-01-01", "2024-12-31"):
        assert gen.max_capacity == {"s1": 10.0, "s2": 20.0}

    with horizon("2025-01-01", "2025-12-31"):
        assert gen.max_capacity is None


def test_horizon_with_timeslices():
    """Test horizon filtering with timeslices."""
    prop = PLEXOSPropertyValue.from_records(
        [
            {"time_slice": "M1", "date_from": "2024-01-01", "date_to": "2024-06-30", "value": 10.0},
            {"time_slice": "M2", "date_from": "2024-07-01", "date_to": "2024-12-31", "value": 20.0},
        ]
    )
    gen = PLEXOSGenerator(name="test", max_capacity=prop)

    assert gen.max_capacity == {"M1": 10.0, "M2": 20.0}

    with horizon("2024-01-01", "2024-06-30"):
        assert gen.max_capacity == 10.0

    with horizon("2024-07-01", "2024-12-31"):
        assert gen.max_capacity == 20.0

    with horizon("2024-01-01", "2024-12-31"):
        assert gen.max_capacity == {"M1": 10.0, "M2": 20.0}


def test_scenario_and_horizon_combined():
    """Test combined scenario priority and horizon filtering."""
    prop = PLEXOSPropertyValue.from_records(
        [
            {"scenario": "s1", "date_from": "2024-01-01", "date_to": "2024-12-31", "value": 10.0},
            {"scenario": "s2", "date_from": "2024-01-01", "date_to": "2024-12-31", "value": 20.0},
            {"scenario": "s3", "date_from": "2025-01-01", "date_to": "2025-12-31", "value": 30.0},
        ]
    )
    gen = PLEXOSGenerator(name="test", max_capacity=prop)

    assert gen.max_capacity == {"s1": 10.0, "s2": 20.0, "s3": 30.0}

    # PLEXOS: higher priority number = higher priority
    with scenario_priority({"s1": 1, "s3": 2, "s2": 3}):
        assert gen.max_capacity == 20.0

    with horizon("2024-01-01", "2024-12-31"):
        assert gen.max_capacity == {"s1": 10.0, "s2": 20.0}

    with scenario_and_horizon({"s1": 1, "s2": 2}, "2024-01-01", "2024-12-31"):
        assert gen.max_capacity == 20.0

    with scenario_and_horizon({"s3": 1}, "2025-01-01", "2025-12-31"):
        assert gen.max_capacity == 30.0


def test_horizon_partial_overlap():
    """Test horizon with partial date overlaps."""
    prop = PLEXOSPropertyValue.from_records(
        [
            {"date_from": "2024-01-01", "date_to": "2024-06-30", "value": 10.0},
            {"date_from": "2024-06-01", "date_to": "2024-12-31", "value": 20.0},
        ]
    )
    gen = PLEXOSGenerator(name="test", max_capacity=prop)

    with horizon("2024-06-01", "2024-06-30"):
        assert gen.max_capacity == 10.0

    with horizon("2024-03-01", "2024-03-31"):
        assert gen.max_capacity == 10.0

    with horizon("2024-09-01", "2024-09-30"):
        assert gen.max_capacity == 20.0


def test_horizon_with_open_ended_dates():
    """Test horizon with entries that have only date_from or date_to."""
    prop = PLEXOSPropertyValue.from_records(
        [
            {"date_from": "2024-01-01", "value": 10.0},  # Open-ended end date
            {"date_to": "2023-12-31", "value": 5.0},  # Open-ended start date
        ]
    )
    gen = PLEXOSGenerator(name="test", max_capacity=prop)

    with horizon("2024-06-01", "2024-12-31"):
        assert gen.max_capacity == 10.0

    with horizon("2023-01-01", "2023-12-31"):
        assert gen.max_capacity == 5.0

    with horizon("2025-01-01", "2025-12-31"):
        assert gen.max_capacity == 10.0


def test_horizon_nested_context_managers():
    """Test nested horizon context managers."""
    prop = PLEXOSPropertyValue.from_records(
        [
            {"date_from": "2024-01-01", "date_to": "2024-06-30", "value": 10.0},
            {"date_from": "2024-07-01", "date_to": "2024-12-31", "value": 20.0},
        ]
    )
    gen = PLEXOSGenerator(name="test", max_capacity=prop)

    with horizon("2024-01-01", "2024-12-31"):
        assert gen.max_capacity == 10.0  # Both entries match, first returned

        with horizon("2024-07-01", "2024-12-31"):
            assert gen.max_capacity == 20.0

        assert gen.max_capacity == 10.0

    assert gen.max_capacity == 10.0


def test_horizon_with_scenarios_and_timeslices():
    """Test complex case with scenarios, timeslices, and dates."""
    prop = PLEXOSPropertyValue.from_records(
        [
            {
                "scenario": "s1",
                "time_slice": "M1",
                "date_from": "2024-01-01",
                "date_to": "2024-06-30",
                "value": 10.0,
            },
            {
                "scenario": "s1",
                "time_slice": "M2",
                "date_from": "2024-07-01",
                "date_to": "2024-12-31",
                "value": 15.0,
            },
            {
                "scenario": "s2",
                "time_slice": "M1",
                "date_from": "2024-01-01",
                "date_to": "2024-06-30",
                "value": 20.0,
            },
            {
                "scenario": "s2",
                "time_slice": "M2",
                "date_from": "2024-07-01",
                "date_to": "2024-12-31",
                "value": 25.0,
            },
        ]
    )
    gen = PLEXOSGenerator(name="test", max_capacity=prop)

    with horizon("2024-01-01", "2024-06-30"):
        assert gen.max_capacity == {"s1": 10.0, "s2": 20.0}

    with horizon("2024-07-01", "2024-12-31"):
        assert gen.max_capacity == {"s1": 15.0, "s2": 25.0}

    # PLEXOS: higher priority number = higher priority
    with scenario_and_horizon({"s1": 1, "s2": 2}, "2024-01-01", "2024-06-30"):
        assert gen.max_capacity == 20.0

    with scenario_and_horizon({"s2": 1, "s1": 2}, "2024-07-01", "2024-12-31"):
        assert gen.max_capacity == 15.0
