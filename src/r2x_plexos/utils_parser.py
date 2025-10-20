"""Utils for parsing plexos XMLs."""

import re
from datetime import datetime

from infrasys.time_series_models import SingleTimeSeries

from .models.base import PLEXOSRow


def to_snake_case(name: str) -> str:
    """Convert name to snake_case."""
    name = name.replace(" ", "_")
    name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower()


def trim_timeseries_to_horizon(
    ts: SingleTimeSeries, horizon_start: datetime, horizon_end: datetime
) -> SingleTimeSeries:
    """Trim a time series to match a specified horizon (date range).

    Parameters
    ----------
    ts : SingleTimeSeries
        The time series to trim
    horizon_start : datetime
        The start of the horizon period
    horizon_end : datetime
        The end of the horizon period (inclusive)

    Returns
    -------
    SingleTimeSeries
        A new time series trimmed to the horizon range

    Raises
    ------
    ValueError
        If the horizon range is not within the time series bounds
    """
    # Calculate offset in terms of resolution steps
    resolution_seconds = ts.resolution.total_seconds()

    # Calculate start index
    start_offset = (horizon_start - ts.initial_timestamp).total_seconds()
    start_index = int(start_offset / resolution_seconds)

    # Calculate end index (exclusive - slice notation is [start:end))
    end_offset = (horizon_end - ts.initial_timestamp).total_seconds()
    end_index = int(end_offset / resolution_seconds)

    # Validate bounds
    if start_index < 0:
        raise ValueError(f"Horizon start {horizon_start} is before time series start {ts.initial_timestamp}")
    if end_index > len(ts.data):
        raise ValueError(
            f"Horizon end {horizon_end} is after time series end "
            f"(ts starts at {ts.initial_timestamp} with {len(ts.data)} points)"
        )

    # Slice the data
    trimmed_data = ts.data[start_index:end_index]

    # Create new time series with trimmed data
    return SingleTimeSeries.from_array(
        data=trimmed_data,
        name=ts.name,
        initial_timestamp=horizon_start,
        resolution=ts.resolution,
    )


def apply_action_to_timeseries(ts: SingleTimeSeries, action: str, value: float) -> SingleTimeSeries:
    """Apply an action operator to a time series."""
    action_map = {"\u00d7": "*", "x": "*", "*": "*", "+": "+", "-": "-", "/": "/", "=": "="}
    normalized_action = action_map.get(action, action)

    if normalized_action not in action_map.values():
        raise ValueError(f"Unsupported action: {action}")

    if normalized_action == "=" or normalized_action is None:
        return ts

    if normalized_action == "*":
        new_data = [x * value for x in ts.data]
    elif normalized_action == "+":
        new_data = [x + value for x in ts.data]
    elif normalized_action == "-":
        new_data = [x - value for x in ts.data]
    elif normalized_action == "/":
        if value == 0:
            raise ValueError("Cannot divide by zero")
        new_data = [x / value for x in ts.data]
    else:
        return ts

    return SingleTimeSeries.from_array(new_data, ts.name, ts.initial_timestamp, ts.resolution)


def create_plexos_row(value: float, template: PLEXOSRow) -> PLEXOSRow:
    """Create a new PLEXOSRow with updated value, preserving all other fields from template."""
    return PLEXOSRow(
        value=value,
        units=template.units,
        action=template.action,
        scenario_name=template.scenario_name,
        band=template.band,
        timeslice_name=template.timeslice_name,
        date_from=template.date_from,
        date_to=template.date_to,
        datafile_name=template.datafile_name,
        datafile_id=template.datafile_id,
        column_name=template.column_name,
        variable_name=template.variable_name,
        variable_id=template.variable_id,
        text=template.text,
    )


def apply_action(base_value: float, new_value: float, action: str | None) -> float:
    """Apply a PLEXOS action operation to combine values.

    Parameters
    ----------
    base_value : float
        The current/base value
    new_value : float
        The new value to apply
    action : str | None
        The action to perform: "=", "*", "+", "-", "/"

    Returns
    -------
    float
        The result of applying the action
    """
    if action == "*" or action == "\u00d7":
        return base_value * new_value
    elif action == "+":
        return base_value + new_value
    elif action == "-":
        return base_value - new_value
    elif action == "/" and new_value != 0:
        return base_value / new_value
    else:  # "=" or unknown - just return new value
        return new_value
