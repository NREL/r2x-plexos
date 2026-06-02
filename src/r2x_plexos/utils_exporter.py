"""Utility functions for PLEXOS exporter."""

import csv
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from infrasys import System
from infrasys.time_series_models import SingleTimeSeries

from r2x_core import Ok, Result
from r2x_plexos.plugin_config import PLEXOSConfig


def get_component_category(component: Any) -> str | None:
    """Get the category of a component if it has one."""
    return component.category if hasattr(component, "category") else "-"


def get_output_directory(
    config: PLEXOSConfig,
    system: System,
    output_path: str | None = None,
) -> Path:
    """Get the output directory for time series CSV files."""
    if output_path:
        base_folder = Path(output_path)
        if not base_folder.exists():
            base_folder.mkdir(parents=True, exist_ok=True)
    else:
        base_folder = Path(config.timeseries_dir) if config.timeseries_dir else Path.cwd()
    datafiles_dir = base_folder / "Data"
    datafiles_dir.mkdir(parents=True, exist_ok=True)
    return datafiles_dir


def build_metadata_suffix(
    metadata: dict[str, Any],
    ordered_keys: tuple[str, ...] = ("model_name", "weather_year", "horizon_year"),
) -> str:
    """Build a deterministic suffix from metadata values using key priority order."""
    parts: list[str] = []
    seen: set[str] = set()
    for key in ordered_keys:
        raw_value = metadata.get(key)
        if raw_value is None:
            continue
        value = str(raw_value)
        if value and value not in seen:
            parts.append(value)
            seen.add(value)
    return "_".join(parts) if parts else "default"


def generate_csv_filename(field_name: str, component_class: str, metadata: dict[str, Any]) -> str:
    """Generate a CSV filename for time series export."""
    safe_field = field_name.replace(" ", "_").replace("/", "_")

    metadata_suffix = build_metadata_suffix(metadata)

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

    _, first_ts = time_series_data[0]
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


def get_hydro_budget_property_name(resolution: timedelta) -> str:
    """Return the PLEXOS Max Energy property name matching the given time series resolution.

    Parameters
    ----------
    resolution : timedelta
        The resolution of the hydro_budget time series.

    Returns
    -------
    str
        One of "Max Energy Hour", "Max Energy Day", "Max Energy Week",
        "Max Energy Month", or "Max Energy Year".
    """
    total_seconds = resolution.total_seconds()
    if total_seconds <= 3600:
        return "Max Energy Hour"
    elif total_seconds <= 86400:
        return "Max Energy Day"
    elif total_seconds <= 7 * 86400:
        return "Max Energy Week"
    elif total_seconds <= 31 * 86400:
        return "Max Energy Month"
    else:
        return "Max Energy Year"
