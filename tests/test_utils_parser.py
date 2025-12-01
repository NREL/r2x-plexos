"""Tests for parser utility functions."""

from datetime import datetime, timedelta

import pytest
from infrasys.time_series_models import SingleTimeSeries

from r2x_plexos.models.base import PLEXOSRow
from r2x_plexos.utils_parser import (
    apply_action,
    apply_action_to_timeseries,
    create_plexos_row,
    to_snake_case,
    trim_timeseries_to_horizon,
)


@pytest.fixture
def sample_ts():
    """Reusable time series for action tests."""
    data = [1.0, 2.0, 3.0, 4.0, 5.0]
    initial_time = datetime(2024, 1, 1)
    return SingleTimeSeries.from_array(data, "test_ts", initial_time, resolution=3600)


@pytest.fixture
def trim_ts():
    """Time series trimmed for horizon tests."""
    data = [1.0, 2.0, 3.0]
    initial_time = datetime(2024, 1, 1)
    return SingleTimeSeries.from_array(data, "trim_ts", initial_time, resolution=3600)


def test_to_snake_case_basic():
    assert to_snake_case("SimpleTest") == "simple_test"


def test_to_snake_case_with_spaces():
    assert to_snake_case("Test With Spaces") == "test_with_spaces"


def test_to_snake_case_camel_case():
    assert to_snake_case("testCamelCase") == "test_camel_case"


def test_to_snake_case_already_snake():
    assert to_snake_case("already_snake_case") == "already_snake_case"


def test_to_snake_case_with_numbers():
    assert to_snake_case("test123Value") == "test123_value"


def test_apply_action_to_timeseries_multiply(sample_ts):
    result = apply_action_to_timeseries(sample_ts, "*", 2.0)
    assert list(result.data) == [2.0, 4.0, 6.0, 8.0, 10.0]


def test_apply_action_to_timeseries_add(sample_ts):
    result = apply_action_to_timeseries(sample_ts, "+", 10.0)
    assert list(result.data) == [11.0, 12.0, 13.0, 14.0, 15.0]


def test_apply_action_to_timeseries_subtract(sample_ts):
    result = apply_action_to_timeseries(sample_ts, "-", 1.0)
    assert list(result.data) == [0.0, 1.0, 2.0, 3.0, 4.0]


def test_apply_action_to_timeseries_divide(sample_ts):
    result = apply_action_to_timeseries(sample_ts, "/", 2.0)
    assert list(result.data) == [0.5, 1.0, 1.5, 2.0, 2.5]


def test_apply_action_to_timeseries_divide_by_zero(sample_ts):
    with pytest.raises(ValueError, match="Cannot divide by zero"):
        apply_action_to_timeseries(sample_ts, "/", 0.0)


def test_apply_action_to_timeseries_equals_returns_same(sample_ts):
    result = apply_action_to_timeseries(sample_ts, "=", 100.0)
    assert list(result.data) == list(sample_ts.data)
    assert result is sample_ts


def test_apply_action_to_timeseries_invalid_action(sample_ts):
    with pytest.raises(ValueError, match="Unsupported action"):
        apply_action_to_timeseries(sample_ts, "invalid", 1.0)


def test_apply_action_to_timeseries_preserves_metadata(sample_ts):
    result = apply_action_to_timeseries(sample_ts, "*", 2.0)
    assert result.name == sample_ts.name
    assert result.initial_timestamp == sample_ts.initial_timestamp
    assert result.resolution == sample_ts.resolution


def test_create_plexos_row_updates_value():
    template_row = PLEXOSRow(
        value=100.0,
        units="MW",
        action="*",
        scenario_name="Base",
        band=1,
        timeslice_name="Summer",
        date_from="2024-01-01",
        date_to="2024-12-31",
        datafile_name="test.csv",
        datafile_id=123,
        column_name="TestColumn",
        variable_name="TestVar",
        variable_id=456,
        text="Test text",
    )
    new_value = 200.0
    result = create_plexos_row(new_value, template_row)

    assert result.value == new_value
    assert result.units == template_row.units
    assert result.action == template_row.action
    assert result.scenario_name == template_row.scenario_name
    assert result.band == template_row.band
    assert result.timeslice_name == template_row.timeslice_name
    assert result.date_from == template_row.date_from
    assert result.date_to == template_row.date_to
    assert result.datafile_name == template_row.datafile_name
    assert result.datafile_id == template_row.datafile_id
    assert result.column_name == template_row.column_name
    assert result.variable_name == template_row.variable_name
    assert result.variable_id == template_row.variable_id
    assert result.text == template_row.text


def test_create_plexos_row_with_zero():
    template_row = PLEXOSRow(
        value=100.0,
        units="MW",
        action="*",
        scenario_name="Base",
        band=1,
        timeslice_name="Summer",
        date_from="2024-01-01",
        date_to="2024-12-31",
        datafile_name="test.csv",
        datafile_id=123,
        column_name="TestColumn",
        variable_name="TestVar",
        variable_id=456,
        text="Test text",
    )
    result = create_plexos_row(0.0, template_row)
    assert result.value == 0.0


def test_create_plexos_row_with_negative():
    template_row = PLEXOSRow(
        value=100.0,
        units="MW",
        action="*",
        scenario_name="Base",
        band=1,
        timeslice_name="Summer",
        date_from="2024-01-01",
        date_to="2024-12-31",
        datafile_name="test.csv",
        datafile_id=123,
        column_name="TestColumn",
        variable_name="TestVar",
        variable_id=456,
        text="Test text",
    )
    result = create_plexos_row(-50.0, template_row)
    assert result.value == -50.0


def test_trim_timeseries_start_before_series(trim_ts):
    start = trim_ts.initial_timestamp - timedelta(hours=1)
    end = trim_ts.initial_timestamp + timedelta(hours=1)
    with pytest.raises(ValueError, match="Horizon start"):
        trim_timeseries_to_horizon(trim_ts, start, end)


def test_trim_timeseries_end_after_series(trim_ts):
    start = trim_ts.initial_timestamp
    end = trim_ts.initial_timestamp + timedelta(hours=10)
    with pytest.raises(ValueError, match="Horizon end"):
        trim_timeseries_to_horizon(trim_ts, start, end)


@pytest.mark.parametrize(
    ("base", "new", "action", "expected"),
    [
        (10.0, 5.0, "*", 50.0),
        (10.0, 5.0, "+", 15.0),
        (10.0, 5.0, "-", 5.0),
        (10.0, 5.0, "/", 2.0),
        (10.0, 5.0, "=", 5.0),
        (10.0, 5.0, None, 5.0),
        (10.0, 5.0, "invalid", 5.0),
    ],
)
def test_apply_action_various(base, new, action, expected):
    assert apply_action(base, new, action) == expected


def test_apply_action_divide_by_zero_returns_new():
    assert apply_action(10.0, 0.0, "/") == 0.0
