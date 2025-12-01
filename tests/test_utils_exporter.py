"""Tests for exporter utility helpers."""

from datetime import datetime
from pathlib import Path

import pytest
from infrasys.time_series_models import SingleTimeSeries

from r2x_plexos.utils_exporter import export_time_series_csv


@pytest.fixture
def sample_time_series():
    data = [1.0, 2.0, 3.0]
    initial_time = datetime(2024, 1, 1)
    return SingleTimeSeries.from_array(data, "test_ts", initial_time, resolution=3600)


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
