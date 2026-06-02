"""Tests for exporter utility helpers."""

from datetime import datetime, timedelta
from pathlib import Path

import pytest
from infrasys import System
from infrasys.time_series_models import SingleTimeSeries

from r2x_plexos.models.generator import PLEXOSGenerator
from r2x_plexos.plugin_config import PLEXOSConfig
from r2x_plexos.utils_exporter import (
    build_metadata_suffix,
    export_time_series_csv,
    format_datetime,
    generate_csv_filename,
    get_component_category,
    get_hydro_budget_property_name,
    get_output_directory,
)


@pytest.fixture
def sample_time_series():
    data = [1.0, 2.0, 3.0]
    initial_time = datetime(2024, 1, 1)
    return SingleTimeSeries.from_array(data, "test_ts", initial_time, resolution=3600)


@pytest.fixture
def sample_config():
    return PLEXOSConfig(model_name="TestModel", horizon_year=2024)


@pytest.fixture
def sample_system():
    return System(name="TestSystem")


def test_export_time_series_csv_requires_data(tmp_path: Path):
    filepath = tmp_path / "empty.csv"
    with pytest.raises(ValueError, match="No time series data provided"):
        export_time_series_csv(filepath, [])


def test_export_time_series_csv_mismatched_lengths(tmp_path: Path, sample_time_series: SingleTimeSeries):
    extra = SingleTimeSeries.from_array(
        [1.0, 2.0, 3.0, 4.0], "other_ts", sample_time_series.initial_timestamp, resolution=3600
    )
    filepath = tmp_path / "mismatch.csv"
    with pytest.raises(ValueError, match="Time series length mismatch"):
        export_time_series_csv(filepath, [("first", sample_time_series), ("second", extra)])


def test_export_time_series_csv_success(tmp_path: Path, sample_time_series: SingleTimeSeries):
    """Test successful CSV export with matching time series."""
    filepath = tmp_path / "output.csv"

    # Create second time series with same length
    ts2 = SingleTimeSeries.from_array(
        [4.0, 5.0, 6.0], "test_ts2", sample_time_series.initial_timestamp, resolution=3600
    )

    result = export_time_series_csv(filepath, [("comp1", sample_time_series), ("comp2", ts2)])

    assert result.is_ok()
    assert filepath.exists()

    # Verify CSV content
    content = filepath.read_text()
    lines = content.strip().split("\n")
    assert len(lines) == 4  # header + 3 data rows
    assert "DateTime,comp1,comp2" in lines[0]
    assert "2024-01-01" in lines[1]


def test_get_component_category_with_category():
    """Test getting category from component that has category attribute."""
    gen = PLEXOSGenerator(name="TestGen", category="Thermal")
    assert get_component_category(gen) == "Thermal"


def test_get_component_category_without_category():
    """Test getting category from component without category attribute."""

    class ComponentWithoutCategory:
        name = "Test"

    comp = ComponentWithoutCategory()
    assert get_component_category(comp) == "-"


def test_get_component_category_none_category():
    """Test getting category when category is None."""
    gen = PLEXOSGenerator(name="TestGen", category=None)
    # When category is None, it should return None, not "-"
    assert get_component_category(gen) is None


def test_get_output_directory_with_output_path(sample_config, sample_system, tmp_path):
    """Test output directory creation with explicit output_path."""
    output_path = tmp_path / "custom_output"
    result = get_output_directory(sample_config, sample_system, output_path=str(output_path))

    assert result == output_path / "Data"
    assert result.exists()


def test_get_output_directory_with_config_timeseries_dir(sample_system, tmp_path):
    """Test output directory using config timeseries_dir."""
    config = PLEXOSConfig(model_name="Test", horizon_year=2024, timeseries_dir=tmp_path)
    result = get_output_directory(config, sample_system)

    assert result == tmp_path / "Data"
    assert result.exists()


def test_get_output_directory_defaults_to_cwd(sample_config, sample_system):
    """Test output directory defaults to current working directory."""
    result = get_output_directory(sample_config, sample_system)

    expected = Path.cwd() / "Data"
    assert result == expected


def test_generate_csv_filename_basic():
    """Test basic CSV filename generation."""
    metadata = {"model_name": "Base", "weather_year": 2024, "horizon_year": 2024}
    result = generate_csv_filename("max_capacity", "PLEXOSGenerator", metadata)

    assert result == "PLEXOSGenerator_max_capacity_Base_2024.csv"


def test_generate_csv_filename_different_years():
    """Test filename generation when weather_year and horizon_year differ."""
    metadata = {"model_name": "Base", "weather_year": 2012, "horizon_year": 2023}
    result = generate_csv_filename("max_capacity", "PLEXOSGenerator", metadata)

    assert result == "PLEXOSGenerator_max_capacity_Base_2012_2023.csv"


def test_generate_csv_filename_with_spaces():
    """Test filename generation with spaces in field name."""
    metadata = {"model_name": "Test"}
    result = generate_csv_filename("Max Capacity", "PLEXOSGenerator", metadata)

    assert "Max_Capacity" in result
    assert result == "PLEXOSGenerator_Max_Capacity_Test.csv"


def test_generate_csv_filename_with_slashes():
    """Test filename generation with slashes in field name."""
    metadata = {"model_name": "Test"}
    result = generate_csv_filename("Max/Min", "PLEXOSGenerator", metadata)

    assert "Max_Min" in result
    assert result == "PLEXOSGenerator_Max_Min_Test.csv"


def test_generate_csv_filename_no_metadata():
    """Test filename generation with no metadata."""
    result = generate_csv_filename("max_capacity", "PLEXOSGenerator", {})

    assert result == "PLEXOSGenerator_max_capacity_default.csv"


def test_generate_csv_filename_special_fields():
    """Test filename generation with different field names."""
    metadata = {"model_name": "Base"}

    result = generate_csv_filename("hydro_budget", "PLEXOSGenerator", metadata)
    assert result == "PLEXOSGenerator_hydro_budget_Base.csv"

    result = generate_csv_filename("max_active_power", "PLEXOSGenerator", metadata)
    assert result == "PLEXOSGenerator_max_active_power_Base.csv"

    result = generate_csv_filename("requirement", "PLEXOSReserve", metadata)
    assert result == "PLEXOSReserve_requirement_Base.csv"

    result = generate_csv_filename("natural_inflow", "PLEXOSStorage", metadata)
    assert result == "PLEXOSStorage_natural_inflow_Base.csv"


def test_generate_csv_filename_partial_metadata():
    """Test filename generation with partial metadata."""
    metadata = {"model_name": "Base", "weather_year": 2024}
    result = generate_csv_filename("load", "PLEXOSDemand", metadata)

    assert result == "PLEXOSDemand_load_Base_2024.csv"


def test_generate_csv_filename_ignores_none_weather_year():
    """None-valued metadata keys should not appear in the filename suffix."""
    metadata = {"model_name": "Base", "weather_year": None, "horizon_year": 2023}
    result = generate_csv_filename("load", "PLEXOSDemand", metadata)

    assert result == "PLEXOSDemand_load_Base_2023.csv"


def test_build_metadata_suffix_skips_none_values():
    """Suffix helper should skip None values even if key is present."""
    suffix = build_metadata_suffix({"model_name": "EI_PCM_2023", "weather_year": None, "horizon_year": 2023})

    assert suffix == "EI_PCM_2023_2023"


def test_format_datetime():
    """Test datetime formatting to ISO 8601."""
    dt = datetime(2024, 1, 15, 13, 30, 45)
    result = format_datetime(dt)

    assert result == "2024-01-15T13:30:45"


def test_format_datetime_with_microseconds():
    """Test datetime formatting with microseconds."""
    dt = datetime(2024, 1, 15, 13, 30, 45, 123456)
    result = format_datetime(dt)

    assert result == "2024-01-15T13:30:45.123456"


def test_export_time_series_csv_creates_parent_dirs(tmp_path: Path, sample_time_series: SingleTimeSeries):
    """Test that CSV export creates parent directories if needed."""
    nested_path = tmp_path / "nested" / "directory" / "output.csv"

    nested_path.parent.mkdir(parents=True, exist_ok=True)

    result = export_time_series_csv(nested_path, [("comp1", sample_time_series)])

    assert result.is_ok()
    assert nested_path.exists()
    assert nested_path.parent.exists()


def test_export_time_series_csv_multiple_components(tmp_path: Path):
    """Test CSV export with multiple components."""
    initial_time = datetime(2024, 1, 1)
    ts1 = SingleTimeSeries.from_array([1.0, 2.0], "ts1", initial_time, resolution=3600)
    ts2 = SingleTimeSeries.from_array([3.0, 4.0], "ts2", initial_time, resolution=3600)
    ts3 = SingleTimeSeries.from_array([5.0, 6.0], "ts3", initial_time, resolution=3600)

    filepath = tmp_path / "multi.csv"
    result = export_time_series_csv(filepath, [("comp1", ts1), ("comp2", ts2), ("comp3", ts3)])

    assert result.is_ok()

    # Verify CSV has all components
    content = filepath.read_text()
    lines = content.strip().split("\n")
    header = lines[0]
    assert "comp1" in header
    assert "comp2" in header
    assert "comp3" in header


def test_get_hydro_budget_property_name_hour_resolution():
    assert get_hydro_budget_property_name(timedelta(hours=1)) == "Max Energy Hour"


def test_get_hydro_budget_property_name_day_resolution():
    assert get_hydro_budget_property_name(timedelta(days=1)) == "Max Energy Day"


def test_get_hydro_budget_property_name_week_resolution():
    assert get_hydro_budget_property_name(timedelta(days=7)) == "Max Energy Week"


def test_get_hydro_budget_property_name_month_resolution():
    assert get_hydro_budget_property_name(timedelta(days=31)) == "Max Energy Month"


def test_get_hydro_budget_property_name_year_resolution():
    assert get_hydro_budget_property_name(timedelta(days=32)) == "Max Energy Year"
