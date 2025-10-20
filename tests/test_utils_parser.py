"""Tests for parser utility functions."""

from datetime import datetime

import pytest
from infrasys.time_series_models import SingleTimeSeries

from r2x_plexos.models.base import PLEXOSRow
from r2x_plexos.utils_parser import apply_action_to_timeseries, create_plexos_row, to_snake_case


class TestToSnakeCase:
    """Tests for to_snake_case function."""

    def test_to_snake_case_basic(self):
        """Test basic snake_case conversion."""
        assert to_snake_case("SimpleTest") == "simple_test"

    def test_to_snake_case_with_spaces(self):
        """Test space handling."""
        assert to_snake_case("Test With Spaces") == "test_with_spaces"

    def test_to_snake_case_camel_case(self):
        """Test camelCase conversion."""
        assert to_snake_case("testCamelCase") == "test_camel_case"

    def test_to_snake_case_already_snake(self):
        """Test already snake_case string."""
        assert to_snake_case("already_snake_case") == "already_snake_case"

    def test_to_snake_case_with_numbers(self):
        """Test conversion with numbers."""
        assert to_snake_case("test123Value") == "test123_value"


class TestApplyActionToTimeseries:
    """Tests for apply_action_to_timeseries function."""

    @pytest.fixture
    def sample_ts(self):
        """Create a sample time series for testing."""
        data = [1.0, 2.0, 3.0, 4.0, 5.0]
        initial_time = datetime(2024, 1, 1)
        return SingleTimeSeries.from_array(data, "test_ts", initial_time, resolution=3600)

    def test_apply_action_multiply(self, sample_ts):
        """Test multiplication action."""
        result = apply_action_to_timeseries(sample_ts, "*", 2.0)
        assert list(result.data) == [2.0, 4.0, 6.0, 8.0, 10.0]

    def test_apply_action_multiply_unicode(self, sample_ts):
        """Test multiplication with unicode x symbol."""
        result = apply_action_to_timeseries(sample_ts, "\u00d7", 2.0)
        assert list(result.data) == [2.0, 4.0, 6.0, 8.0, 10.0]

    def test_apply_action_multiply_x(self, sample_ts):
        """Test multiplication with 'x' symbol."""
        result = apply_action_to_timeseries(sample_ts, "x", 2.0)
        assert list(result.data) == [2.0, 4.0, 6.0, 8.0, 10.0]

    def test_apply_action_add(self, sample_ts):
        """Test addition action."""
        result = apply_action_to_timeseries(sample_ts, "+", 10.0)
        assert list(result.data) == [11.0, 12.0, 13.0, 14.0, 15.0]

    def test_apply_action_subtract(self, sample_ts):
        """Test subtraction action."""
        result = apply_action_to_timeseries(sample_ts, "-", 1.0)
        assert list(result.data) == [0.0, 1.0, 2.0, 3.0, 4.0]

    def test_apply_action_divide(self, sample_ts):
        """Test division action."""
        result = apply_action_to_timeseries(sample_ts, "/", 2.0)
        assert list(result.data) == [0.5, 1.0, 1.5, 2.0, 2.5]

    def test_apply_action_divide_by_zero(self, sample_ts):
        """Test division by zero raises error."""
        with pytest.raises(ValueError, match="Cannot divide by zero"):
            apply_action_to_timeseries(sample_ts, "/", 0.0)

    def test_apply_action_equals(self, sample_ts):
        """Test equals action (no-op)."""
        result = apply_action_to_timeseries(sample_ts, "=", 100.0)
        assert list(result.data) == list(sample_ts.data)
        assert result is sample_ts

    def test_apply_action_invalid(self, sample_ts):
        """Test invalid action raises error."""
        with pytest.raises(ValueError, match="Unsupported action"):
            apply_action_to_timeseries(sample_ts, "invalid", 1.0)

    def test_apply_action_preserves_metadata(self, sample_ts):
        """Test that action preserves time series metadata."""
        result = apply_action_to_timeseries(sample_ts, "*", 2.0)
        assert result.name == sample_ts.name
        assert result.initial_timestamp == sample_ts.initial_timestamp
        assert result.resolution == sample_ts.resolution


class TestCreatePlexosRow:
    """Tests for create_plexos_row function."""

    @pytest.fixture
    def template_row(self):
        """Create a template PLEXOSRow for testing."""
        return PLEXOSRow(
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

    def test_create_plexos_row_preserves_template(self, template_row):
        """Test that all fields except value are preserved from template."""
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

    def test_create_plexos_row_updates_value(self, template_row):
        """Test that value is updated correctly."""
        original_value = template_row.value
        new_value = 500.5
        result = create_plexos_row(new_value, template_row)

        assert result.value == new_value
        assert result.value != original_value

    def test_create_plexos_row_with_zero(self, template_row):
        """Test creating row with zero value."""
        result = create_plexos_row(0.0, template_row)
        assert result.value == 0.0

    def test_create_plexos_row_with_negative(self, template_row):
        """Test creating row with negative value."""
        result = create_plexos_row(-50.0, template_row)
        assert result.value == -50.0
