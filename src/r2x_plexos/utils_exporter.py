"""Utility functions for PLEXOS exporter."""

import csv
from datetime import datetime
from pathlib import Path
from typing import Any

from infrasys import System
from infrasys.time_series_models import SingleTimeSeries

from r2x_core import Ok, Result
from r2x_plexos.config import PLEXOSConfig
from r2x_plexos.models.component import PLEXOSObject


def get_component_category(component: PLEXOSObject) -> str | None:
    """Get the category of a component if it has one."""
    return component.category if hasattr(component, "category") else "-"


def get_output_directory(config: PLEXOSConfig, system: System) -> Path:
    """Get the output directory for time series CSV files."""
    base_folder = Path(config.timeseries_dir) if config.timeseries_dir else Path.cwd()
    datafiles_dir = base_folder / "datafiles"
    datafiles_dir.mkdir(parents=True, exist_ok=True)
    return datafiles_dir


def generate_csv_filename(field_name: str, component_class: str, metadata: dict[str, Any]) -> str:
    """Generate a CSV filename for time series export."""
    safe_field = field_name.replace(" ", "_").replace("/", "_")

    metadata_parts = []
    for key in sorted(metadata.keys()):
        value = metadata[key]
        if value is not None:
            safe_value = str(value).replace(" ", "_").replace("/", "_")
            metadata_parts.append(f"{key}{safe_value}")

    metadata_suffix = "_".join(metadata_parts) if metadata_parts else "default"

    return f"{component_class}_{safe_field}_{metadata_suffix}.csv"


def format_datetime(dt: datetime) -> str:
    """Format datetime for CSV export in ISO 8601 format."""
    return dt.isoformat()


def export_time_series_csv(
    filepath: Path,
    time_series_data: list[tuple[str, SingleTimeSeries]],
) -> Result[None, Exception]:
    """Export time series to CSV in DateTime,Component format."""
    if not time_series_data:
        raise ValueError("No time series data provided")

    _first_name, first_ts = time_series_data[0]
    initial_timestamp = first_ts.initial_timestamp
    resolution = first_ts.resolution
    data_length = len(first_ts.data)

    for comp_name, ts in time_series_data:
        if len(ts.data) != data_length:
            raise ValueError(
                f"Time series length mismatch: {comp_name} has {len(ts.data)} points, expected {data_length}"
            )

    datetime_values = [initial_timestamp + (i * resolution) for i in range(data_length)]

    with open(filepath, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        header = ["DateTime"] + [name for name, _ in time_series_data]
        writer.writerow(header)

        for i, dt in enumerate(datetime_values):
            row = [format_datetime(dt)] + [ts.data[i] for _, ts in time_series_data]
            writer.writerow(row)

    return Ok(None)
