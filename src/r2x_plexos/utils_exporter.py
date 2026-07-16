"""Utility functions for PLEXOS exporter."""

import csv
from datetime import datetime
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
    target_year: int | None = None,
) -> Result[None, Exception]:
    """Export time series to CSV in DateTime,Component format.

    Parameters
    ----------
    filepath : Path
        Destination CSV file path.
    time_series_data : list[tuple[str, SingleTimeSeries]]
        Pairs of (component_name, time_series) to export as columns.
    target_year : int | None, optional
        When provided, replaces the year component of every timestamp so that
        the exported CSV reflects the simulation horizon year instead of the
        underlying weather/source year stored in the time series.
    """
    if not time_series_data:
        raise ValueError("No time series data provided")

    _, first_ts = time_series_data[0]
    initial_timestamp = first_ts.initial_timestamp
    if target_year is not None:
        try:
            initial_timestamp = initial_timestamp.replace(year=target_year)
        except ValueError:
            # e.g. Feb 29 does not exist in target_year — clamp to Feb 28
            initial_timestamp = initial_timestamp.replace(year=target_year, day=28)
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


def get_hydro_budget_property_name(ts: Any) -> str:
    """Return the PLEXOS Max Energy property name inferred from time series length.

    Because all time series in r2x-plexos are stored with a fixed 1-hour
    resolution by the CSV parser regardless of the original data frequency,
    the resolution field cannot be used to distinguish between hourly, daily,
    weekly, monthly, or annual constraints.  Instead, the number of data
    points is used as a proxy for the original sampling period.

    **Constant series (all values identical)** are treated as ``Max Energy Day``
    regardless of length.  A constant 8760-row series is a scalar ``Max Energy Day``
    property that was expanded to an hourly profile during export.  Using it as
    ``Max Energy Hour`` would be non-binding (budget >> generator capacity) and
    therefore meaningless in PLEXOS.

    For non-constant series the row-count heuristic applies:

    ============  =============  ==================
    Row count     Frequency      PLEXOS property
    ============  =============  ==================
    > 366         hourly         Max Energy Hour
    > 52 - 366    daily          Max Energy Day
    > 12 - 52     weekly         Max Energy Week
    > 1 - 12      monthly        Max Energy Month
    1             annual         Max Energy Year
    ============  =============  ==================

    Parameters
    ----------
    ts : SingleTimeSeries
        The resolved hydro_budget time series.

    Returns
    -------
    str
        One of "Max Energy Hour", "Max Energy Day", "Max Energy Week",
        "Max Energy Month", or "Max Energy Year".
    """
    data = ts.data
    # Constant series: scalar budget expanded to an hourly profile → treat as daily cap
    if len(set(data)) == 1:
        return "Max Energy Day"

    n = len(data)
    if n > 366:
        return "Max Energy Hour"
    elif n > 52:
        return "Max Energy Day"
    elif n > 12:
        return "Max Energy Week"
    elif n > 1:
        return "Max Energy Month"
    else:
        return "Max Energy Year"
