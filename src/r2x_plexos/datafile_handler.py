"""Handler for PLEXOS datafiles referenced in components."""

import calendar
import re
from datetime import datetime, timedelta
from functools import lru_cache, singledispatch
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import polars as pl
from infrasys import SingleTimeSeries

if TYPE_CHECKING:
    from r2x_plexos.models.timeslice import PLEXOSTimeslice

# Type alias for parsed file data - can be time series or constant float values
ParsedFileData = dict[str, SingleTimeSeries] | dict[str, float]


class FileType:
    """Base class for file type identification."""


class PatternFile(FileType):
    """File with Name and Pattern columns."""


class MonthlyFile(FileType):
    """File with monthly data (M01, M02, etc.)."""


class YearlyFile(FileType):
    """File with yearly data (Year column)."""


class HourlyComponentsFile(FileType):
    """File with Month, Day, Period format."""


class DatetimeComponentsFile(FileType):
    """File with DateTime column."""


class TimesliceFile(FileType):
    """File with timeslice columns."""

    def __init__(self, timeslices: list["PLEXOSTimeslice"]) -> None:
        """Assign timeslices."""
        self.timeslices = timeslices


class HourlyDailyFile(FileType):
    """File with Year, Month, Day columns and hourly columns (1-24)."""


class ValueFile(FileType):
    """Simple file with Name and Value columns."""


@lru_cache(maxsize=64)
def load_csv_cached(path: str) -> pl.LazyFrame:
    """Load a CSV file and return it as a LazyFrame."""
    return pl.scan_csv(Path(path), infer_schema_length=100000)


def is_leap_year(year: int) -> bool:
    """Check if the given year is a leap year."""
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)


def hours_in_year(year: int) -> int:
    """Return the number of hours in a year."""
    return 8784 if is_leap_year(year) else 8760


def get_month_hour_ranges(year: int) -> dict[int, range]:
    """Create a mapping of month number to hours in that month."""
    month_ranges = {}
    year_start = datetime(year=year, month=1, day=1)

    for month in range(1, 13):
        month_start = datetime(year=year, month=month, day=1)

        month_end = compute_month_end(year, month)

        start_hour = int((month_start - year_start).total_seconds() / 3600)
        end_hour = int((month_end - year_start).total_seconds() / 3600) + 23

        month_ranges[month] = range(start_hour, end_hour + 1)

    return month_ranges


def compute_month_end(year: int, month: int) -> datetime:
    """Compute the last day of a given month."""
    if month == 12:
        return datetime(year=year + 1, month=1, day=1) - timedelta(days=1)
    else:
        return datetime(year=year, month=month + 1, day=1) - timedelta(days=1)


def create_time_series(
    values: list[float],
    name: str,
    initial_time: datetime,
    resolution: timedelta = timedelta(hours=1),
) -> SingleTimeSeries:
    """Create a SingleTimeSeries with the given values and metadata."""
    return SingleTimeSeries.from_array(values, name, initial_time, resolution)


def validate_and_adjust_date(year: int, month: int, day: int, hour: int = 0) -> datetime:
    """Validate and adjust date to ensure it's valid for the given year.

    If the day is invalid for the month (e.g., Feb 29 in non-leap year),
    it will be clamped to the maximum valid day for that month.
    """
    from loguru import logger

    # Validate month
    if not 1 <= month <= 12:
        logger.warning(f"Invalid month {month}, defaulting to January")
        month = 1

    # Get maximum valid day for this month/year
    max_day = calendar.monthrange(year, month)[1]

    # Adjust day if it exceeds the maximum
    if day > max_day:
        logger.warning(f"Adjusted invalid date M{month:02d},D{day:02d} for year {year} to D{max_day:02d}")
        day = max_day
    elif day < 1:
        logger.warning(f"Invalid day {day}, defaulting to 1")
        day = 1

    return datetime(year, month, day, hour)


def parse_date_pattern(pattern: str, year: int) -> datetime:
    """Parse a date pattern like 'M1,D1,H0' into a datetime object."""
    if not pattern:
        raise ValueError("Empty pattern string")

    parts = {}
    for token in pattern.split(","):
        token = token.strip()
        if token.startswith("M"):
            parts["month"] = int(token[1:])
        elif token.startswith("D"):
            parts["day"] = int(token[1:])
        elif token.startswith("H"):
            parts["hour"] = int(token[1:])

    return validate_and_adjust_date(year, parts.get("month", 1), parts.get("day", 1), parts.get("hour", 0))


def get_hours_for_timeslice(pattern: str, year: int) -> set[int]:
    """Convert a timeslice pattern to a set of hour indices."""
    if not pattern:
        return set()

    normalized_pattern = pattern.replace(";", ",")
    parts = [p.strip() for p in normalized_pattern.split(",") if p.strip()]
    hours: set[int] = set()

    for part in parts:
        month_match = re.search(r"M(\d+)-(\d+)", part)
        if not month_match:
            continue

        start_month = int(month_match.group(1))
        end_month = int(month_match.group(2))
        month_ranges = get_month_hour_ranges(year)

        for month in range(start_month, end_month + 1):
            if month in month_ranges:
                hours.update(month_ranges[month])

    return hours


def detect_file_type(df: pl.LazyFrame, timeslices: list["PLEXOSTimeslice"] | None = None) -> FileType:
    """Detect the type of file based on its structure."""
    columns = df.collect_schema().names()
    column_lower_map = {col.lower().strip(): col for col in columns}

    if all(col.lower().strip() in column_lower_map for col in ["name", "pattern"]):
        return PatternFile()

    if all(col.lower().strip() in column_lower_map for col in ["name", "value"]):
        return ValueFile()

    if "name".lower().strip() in column_lower_map and any(
        f"m{i:02d}".lower().strip() in column_lower_map for i in range(1, 13)
    ):
        return MonthlyFile()

    if all(col.lower().strip() in column_lower_map for col in ["month", "day", "period"]):
        return HourlyComponentsFile()

    if timeslices and "name".lower().strip() in column_lower_map:
        timeslice_names_lower = {ts.name.lower().strip() for ts in timeslices}
        for col_lower in column_lower_map:
            if col_lower in timeslice_names_lower:
                return TimesliceFile(timeslices)

    if all(col.lower().strip() in column_lower_map for col in ["year", "month", "day"]):
        hour_cols = sum(1 for col in columns if col.strip().isdigit() and 1 <= int(col.strip()) <= 24)
        if hour_cols >= 20 and "name" not in {col.lower().strip() for col in columns}:
            return HourlyDailyFile()

    if all(col.lower().strip() in column_lower_map for col in ["year"]):
        return YearlyFile()

    if "name" in {col.lower().strip() for col in columns} and any(
        col.lower().strip().startswith("yr-") for col in columns
    ):
        return YearlyFile()

    if all(col.lower().strip() in column_lower_map for col in ["datetime"]):
        return DatetimeComponentsFile()

    raise ValueError(f"Unknown file type with columns: {columns}")


def extract_file_data(
    path: str,
    default_initial_time: datetime | None = None,
    year: int | None = None,
    timeslices: list["PLEXOSTimeslice"] | None = None,
) -> ParsedFileData:
    """Extract all time series from a CSV file."""
    df = load_csv_cached(path)
    file_type = detect_file_type(df, timeslices)
    return parse_file(file_type, df, default_initial_time, year)


def extract_one_time_series(
    path: str,
    component: str,
    default_initial_time: datetime | None = None,
    year: int | None = None,
) -> SingleTimeSeries | float:
    """Extract a single time series from a CSV file."""
    ts_map = extract_file_data(path, default_initial_time, year)

    if component not in ts_map:
        if len(ts_map) == 1:
            result: SingleTimeSeries | float = next(iter(ts_map.values()))  # type: ignore[assignment]
            return result
        raise ValueError(f"Component '{component}' not found in file: {path}")

    return cast(SingleTimeSeries | float, ts_map[component])  # type: ignore[redundant-cast]


@singledispatch
def parse_file(
    file_type: FileType,
    df: pl.LazyFrame,
    default_initial_time: datetime | None = None,
    year: int | None = None,
) -> ParsedFileData:
    """Parse a file based on its type."""
    raise ValueError(f"Unsupported file type: {type(file_type).__name__}")


@parse_file.register
def _(
    file_type: PatternFile,
    df: pl.LazyFrame,
    default_initial_time: datetime | None = None,
    year: int | None = None,
) -> dict[str, SingleTimeSeries]:
    """Parse a pattern file with daily profiles and band columns."""
    if year is None:
        raise ValueError("Year must be provided for pattern-based files.")

    columns = df.collect_schema().names()

    band_columns = []
    for col in columns:
        try:
            _ = int(col)
            band_columns.append(col)
        except ValueError:
            continue

    has_band_columns = len(band_columns) > 0
    total_hours = hours_in_year(year)
    ts_map: dict[str, SingleTimeSeries] = {}

    collected_df = df.collect()

    name_column = find_column_case_insensitive(collected_df[0].to_dict(), "name")
    if name_column is None:
        raise ValueError("No 'Name' column found in pattern file")
    unique_components = collected_df.select(name_column).unique().to_series().to_list()

    for component_name in unique_components:
        component_rows = collected_df.filter(pl.col(name_column) == component_name)

        if has_band_columns:
            for band_col in band_columns:
                hourly_values = [0.0] * total_hours  # Default to 0

                for row in component_rows.iter_rows(named=True):
                    pattern_column = find_column_case_insensitive(row, "pattern")
                    pattern = row.get(pattern_column) if pattern_column else None

                    if not pattern:
                        if default_initial_time is None:
                            raise ValueError(
                                f"No Pattern provided and no default_initial_time specified for {component_name}"
                            )
                        continue

                    try:
                        pattern_date = parse_date_pattern(pattern, year)
                        day_of_year = (pattern_date - datetime(year, 1, 1)).days

                        if band_col in row and row[band_col] is not None:
                            try:
                                band = int(band_col)
                                value = safe_float_conversion(row[band_col])

                                start_hour = day_of_year * 24

                                for hour in range(24):
                                    if start_hour + hour < total_hours:
                                        hourly_values[start_hour + hour] = value
                            except (ValueError, TypeError):
                                pass
                    except ValueError:
                        continue

                band = int(band_col)
                ts_name = f"{component_name}_band_{band}"
                ts_map[ts_name] = create_time_series(hourly_values, f"band_{band}", datetime(year, 1, 1))
        else:
            hourly_values = [0.0] * total_hours  # Default to 0

            for row in component_rows.iter_rows(named=True):
                pattern_column = find_column_case_insensitive(row, "pattern")
                value_column = find_column_case_insensitive(row, "value")

                pattern = row.get(pattern_column) if pattern_column else None
                raw_value = row.get(value_column) if value_column else None

                if not pattern or raw_value is None:
                    continue

                try:
                    pattern_date = parse_date_pattern(pattern, year)
                    day_of_year = (pattern_date - datetime(year, 1, 1)).days
                    start_hour = day_of_year * 24

                    for hour in range(24):
                        if start_hour + hour < total_hours:
                            hourly_values[start_hour + hour] = safe_float_conversion(raw_value)
                except ValueError:
                    continue

            ts_map[component_name] = create_time_series(hourly_values, "value", datetime(year, 1, 1))

    return ts_map


@parse_file.register
def _(
    file_type: MonthlyFile,
    df: pl.LazyFrame,
    default_initial_time: datetime | None = None,
    year: int | None = None,
) -> dict[str, SingleTimeSeries]:
    """Parse a file with monthly data."""
    if year is None:
        raise ValueError("Year must be provided for monthly data files.")

    initial_time = default_initial_time or datetime(year, 1, 1)
    total_hours = hours_in_year(year)
    month_ranges = get_month_hour_ranges(year)

    collected_df = df.collect()
    ts_map: dict[str, SingleTimeSeries] = {}

    for row in collected_df.iter_rows(named=True):
        if "Name" not in row:
            continue

        name = row["Name"]
        hourly_values = [0.0] * total_hours

        for month in range(1, 13):
            month_col = f"M{month:02d}"
            if month_col not in row or row[month_col] is None:
                continue

            monthly_value = safe_float_conversion(row[month_col])
            for hour_idx in month_ranges[month]:
                if hour_idx < total_hours:
                    hourly_values[hour_idx] = monthly_value

        ts_map[name] = create_time_series(hourly_values, "value", initial_time)

    return ts_map


@parse_file.register
def _(
    file_type: HourlyComponentsFile,
    df: pl.LazyFrame,
    default_initial_time: datetime | None = None,
    year: int | None = None,
) -> dict[str, SingleTimeSeries]:
    """Parse a file with hourly component data."""
    if year is None:
        raise ValueError("Year must be provided for Month/Day/Period files.")

    initial_time = default_initial_time or datetime(year, 1, 1)
    total_hours = hours_in_year(year)

    collected_df = df.collect()

    if "Year" in collected_df.columns:
        collected_df = collected_df.filter(pl.col("Year") == year)

    excluded_cols_lower = {"year", "month", "day", "period"}
    component_columns = [
        col for col in collected_df.columns if col.lower().strip() not in excluded_cols_lower
    ]
    ts_map: dict[str, SingleTimeSeries] = {}
    year_start = datetime(year=year, month=1, day=1)

    for component in component_columns:
        hourly_values = [0.0] * total_hours

        for row in collected_df.iter_rows(named=True):
            month_col = find_column_case_insensitive(row, "month")
            day_col = find_column_case_insensitive(row, "day")
            period_col = find_column_case_insensitive(row, "period")

            if not all([month_col, day_col, period_col]) or component not in row or row[component] is None:
                continue

            # Type narrowing: at this point we know these are not None
            assert month_col is not None
            assert day_col is not None
            assert period_col is not None

            if any(row[col] is None for col in [month_col, day_col, period_col]):
                continue

            month = int(row[month_col])
            day = int(row[day_col])
            if not is_valid_date(month, day):
                continue

            period = int(row[period_col])
            if not is_valid_period(period):
                continue

            hour = period - 1  # Convert 1-24 to 0-23
            component_value = safe_float_conversion(row[component])
            date_obj = validate_and_adjust_date(year, month, day, hour)
            hour_index = int((date_obj - year_start).total_seconds() / 3600)

            if 0 <= hour_index < total_hours:
                hourly_values[hour_index] = component_value

        ts_map[component] = create_time_series(hourly_values, "value", initial_time)

    return ts_map


@parse_file.register
def _(
    file_type: DatetimeComponentsFile,
    df: pl.LazyFrame,
    default_initial_time: datetime | None = None,
    year: int | None = None,
) -> dict[str, SingleTimeSeries]:
    """Parse a file with DateTime column."""
    if year is None:
        raise ValueError("Year must be provided for Datetime files.")

    initial_time = default_initial_time or datetime(year, 1, 1)
    total_hours = hours_in_year(year)

    collected_df = df.collect()

    datetime_col = next((col for col in collected_df.columns if col.lower() == "datetime"), None)
    if datetime_col is None:
        raise ValueError("Datetime column not found in file")

    filtered_rows = []
    for row in collected_df.iter_rows(named=True):
        date_obj = parse_datetime_string(row[datetime_col])
        if date_obj is not None and date_obj.year == year:
            row_with_parsed_date = dict(row)
            row_with_parsed_date["_parsed_datetime"] = date_obj
            filtered_rows.append(row_with_parsed_date)

    component_columns = [col for col in collected_df.columns if col.lower() != "datetime"]
    ts_map: dict[str, SingleTimeSeries] = {}
    year_start = datetime(year=year, month=1, day=1)
    month_ranges = get_month_hour_ranges(year)

    month_counts = {}
    for row in filtered_rows:
        date_obj = row["_parsed_datetime"]
        month = date_obj.month
        if month not in month_counts:
            month_counts[month] = 0
        month_counts[month] += 1

    is_monthly_data = sum(1 for count in month_counts.values() if count == 1) >= len(month_counts) / 2

    for component in component_columns:
        hourly_values = [0.0] * total_hours

        if is_monthly_data:
            monthly_values = {}

            for row in filtered_rows:
                if component not in row or row[component] is None:
                    continue

                date_obj = row["_parsed_datetime"]
                month = date_obj.month
                component_value = safe_float_conversion(row[component])

                if month not in monthly_values:
                    monthly_values[month] = component_value

            for month, value in monthly_values.items():
                for hour_idx in month_ranges[month]:
                    if hour_idx < total_hours:
                        hourly_values[hour_idx] = value
        else:
            for row in filtered_rows:
                if component not in row or row[component] is None:
                    continue

                date_obj = row["_parsed_datetime"]
                component_value = safe_float_conversion(row[component])
                hour_index = int((date_obj - year_start).total_seconds() / 3600)

                if 0 <= hour_index < total_hours:
                    hourly_values[hour_index] = component_value

        ts_map[component] = create_time_series(hourly_values, "value", initial_time)

    return ts_map


def is_valid_date(month: int, day: int) -> bool:
    """Check if month and day are within valid ranges."""
    return 1 <= month <= 12 and 1 <= day <= 31


def is_valid_period(period: int) -> bool:
    """Check if period is within valid range (1-24)."""
    return 1 <= period <= 24


@parse_file.register
def _(
    file_type: TimesliceFile,
    df: pl.LazyFrame,
    default_initial_time: datetime | None = None,
    year: int | None = None,
) -> dict[str, SingleTimeSeries]:
    """Parse a timeslice file."""
    timeslices = file_type.timeslices

    if year is None:
        raise ValueError("Year must be provided for timeslice files.")

    initial_time = default_initial_time or datetime(year, 1, 1)
    total_hours = hours_in_year(year)
    timeslice_hours = extract_timeslice_hours(timeslices, year)
    collected_df = df.collect()

    timeslice_map = {ts.name.lower(): ts.name for ts in timeslices}
    column_mapping = {
        col: timeslice_map[col.lower()] for col in collected_df.columns if col.lower() in timeslice_map
    }

    ts_map: dict[str, SingleTimeSeries] = {}
    for row in collected_df.iter_rows(named=True):
        if "Name" not in row:
            continue

        name = row["Name"]
        hourly_values = [0.0] * total_hours

        for col, ts_name in column_mapping.items():
            if col not in row or ts_name not in timeslice_hours or row[col] is None:
                continue

            if col.startswith("YR-"):
                col_year = int(col.split("-")[1])
                if col_year != year:
                    continue

            timeslice_value = safe_float_conversion(row[col])
            for hour_idx in timeslice_hours[ts_name]:
                if hour_idx < total_hours:
                    hourly_values[hour_idx] = timeslice_value

        ts_map[name] = create_time_series(hourly_values, "value", initial_time)

    return ts_map


def extract_timeslice_hours(timeslices: list["PLEXOSTimeslice"], year: int) -> dict[str, set[int]]:
    """Extract hours for each timeslice."""
    timeslice_hours: dict[str, set[int]] = {}
    total_hours = hours_in_year(year)

    for ts in timeslices:
        if ts.name.lower() == "summer":
            summer_hours = get_timeslice_patterns_hours(ts, year)
            if summer_hours:
                timeslice_hours[ts.name] = summer_hours
            break

    for ts in timeslices:
        if ts.name.lower() == "summer":
            continue  # Already processed

        ts_hours = get_timeslice_patterns_hours(ts, year)
        if ts_hours:
            timeslice_hours[ts.name] = ts_hours

    if any(ts.name.lower() == "winter" for ts in timeslices):
        winter_name = next(ts.name for ts in timeslices if ts.name.lower() == "winter")
        if "Summer" in timeslice_hours and winter_name not in timeslice_hours:
            all_hours = set(range(total_hours))
            timeslice_hours[winter_name] = all_hours - timeslice_hours["Summer"]

    return timeslice_hours


def get_timeslice_patterns_hours(timeslice: "PLEXOSTimeslice", year: int) -> set[int]:
    """Extract hours from patterns in a timeslice object."""
    if not hasattr(timeslice, "include") or not timeslice.include:
        return set()

    patterns = extract_patterns_from_timeslice(timeslice)

    hour_set: set[int] = set()
    for pattern in patterns:
        if pattern:
            hour_set.update(get_hours_for_timeslice(pattern, year))

    return hour_set


def extract_patterns_from_timeslice(timeslice: "PLEXOSTimeslice") -> list[str]:
    """Extract pattern strings from a timeslice object."""
    # Get the include property value
    include_prop = timeslice.get_property_value("include")
    if not include_prop:
        return []

    # Extract text patterns from property entries
    return [entry.text for entry in include_prop.entries.values() if entry.text]


@parse_file.register
def _(
    file_type: ValueFile,
    df: pl.LazyFrame,
    default_initial_time: datetime | None = None,
    year: int | None = None,
) -> dict[str, float]:
    """Parse a simple Name-Value file."""
    collected_df = df.collect()
    output_map: dict[str, float] = {}

    for row in collected_df.iter_rows(named=True):
        name_col = find_column_case_insensitive(row, "name")
        value_col = find_column_case_insensitive(row, "value")

        if not name_col or not value_col or row[value_col] is None:
            continue

        component_name = row[name_col]
        constant_value = safe_float_conversion(row[value_col])
        output_map[component_name] = constant_value

    return output_map


@parse_file.register
def _(
    file_type: YearlyFile,
    df: pl.LazyFrame,
    default_initial_time: datetime | None = None,
    year: int | None = None,
) -> dict[str, SingleTimeSeries]:
    """Parse a file with yearly data. Should be parsed as a single value file."""
    if year is None:
        raise ValueError("Year must be provided for yearly data files.")

    initial_time = default_initial_time or datetime(year, 1, 1)
    total_hours = hours_in_year(year)

    collected_df = df.collect()

    year_column = next((col for col in collected_df.columns if col.lower() == "year"), None)
    has_name_column = "Name" in collected_df.columns or "name" in collected_df.columns

    ts_map: dict[str, SingleTimeSeries] = {}
    if year_column and not has_name_column:
        year_row_df = collected_df.filter(pl.col(year_column) == year)

        if year_row_df.height == 0:
            return {}  # No data for this year

        year_row = year_row_df.row(0, named=True)

        for col_name, value in year_row.items():
            if col_name.lower() == "year":
                continue  # Skip the year column itself

            if value is not None:
                yearly_value = safe_float_conversion(value)
                hourly_values = [yearly_value] * total_hours
                ts_map[col_name] = create_time_series(hourly_values, "value", initial_time)

        return ts_map

    wide_year_cols = [col for col in collected_df.columns if col.lower().startswith("yr-")]
    if wide_year_cols:
        collected_df = collected_df.melt(
            id_vars=["Name"], value_vars=wide_year_cols, variable_name="year", value_name="Value"
        )
        collected_df = collected_df.with_columns(pl.col("year").str.replace(r"^YR-", "").cast(pl.Int32))

    if year_column:
        collected_df = collected_df.filter(pl.col(year_column) == year)

    for row in collected_df.iter_rows(named=True):
        if "Name" not in row:
            continue

        if "Value" in row and row["Value"] is not None:
            yearly_value = safe_float_conversion(row["Value"])
        hourly_values = [yearly_value] * total_hours

        name = row["Name"]
        ts_map[name] = create_time_series(hourly_values, "value", initial_time)

    return ts_map


@parse_file.register
def _(
    file_type: HourlyDailyFile,
    df: pl.LazyFrame,
    default_initial_time: datetime | None = None,
    year: int | None = None,
) -> dict[str, SingleTimeSeries]:
    """Parse Year,Month,Day,1-24 hourly format files."""
    if year is None:
        raise ValueError("Year must be provided for HourlyDailyFile.")

    initial_time = default_initial_time or datetime(year, 1, 1)
    collected_df = df.collect()

    if collected_df.height == 0:
        return {}

    first_row = collected_df[0].to_dict()
    year_col = find_column_case_insensitive(first_row, "year")
    month_col = find_column_case_insensitive(first_row, "month")
    day_col = find_column_case_insensitive(first_row, "day")

    if not all([year_col, month_col, day_col]):
        raise ValueError("Year, Month, and Day columns are required for HourlyDailyFile")

    assert year_col is not None
    assert month_col is not None
    assert day_col is not None

    year_df = collected_df.filter(pl.col(year_col) == year).sort([month_col, day_col])

    if year_df.height == 0:
        return {}

    hourly_values = []
    for row in year_df.iter_rows(named=True):
        day_values = [
            safe_float_conversion(row[str(hour)])
            for hour in range(1, 25)
            if str(hour) in row and row[str(hour)] is not None
        ]
        if len(day_values) != 24:
            raise ValueError(f"Missing hourly data for {row[year_col]}-{row[month_col]}-{row[day_col]}")
        hourly_values.extend(day_values)

    ts = create_time_series(hourly_values, "hourly_data", initial_time)
    return {"hourly_data": ts}


def find_column_case_insensitive(row: dict[str, Any], target_name: str) -> str | None:
    """Find a column name in a row dict ignoring case and whitespace."""
    return next((col for col in row if col.lower().strip() == target_name.lower()), None)


def parse_datetime_string(date_str: str) -> datetime | None:
    """
    Parse a datetime string into a datetime object.

    Args:
        date_str: The date string to parse

    Returns
    -------
        datetime: The parsed datetime object or None if parsing fails
    """
    if not isinstance(date_str, str):
        return date_str

    date_formats = [
        "%m/%d/%Y",  # 1/1/2023
        "%Y-%m-%d",  # 2023-01-01
        "%d-%m-%Y",  # 01-01-2023
        "%m/%d/%Y %H:%M",  # 1/1/2023 00:00
        "%m/%d/%Y %H:%M:%S",  # 1/1/2023 00:00:00
        "%Y-%m-%d %H:%M:%S",  # 2023-01-01 00:00:00
        "%Y-%m-%dT%H:%M:%S",  # 2023-01-01T00:00:00
    ]

    for date_format in date_formats:
        try:
            return datetime.strptime(date_str, date_format)
        except ValueError:
            continue
    return None


def safe_float_conversion(value: Any) -> float:
    """
    Safely convert a value to float, handling common formatting issues.

    Args:
        value: The value to convert to float

    Returns
    -------
        float: The converted value

    Raises
    ------
        ValueError: If the value cannot be converted to float
    """
    if isinstance(value, int | float):
        return float(value)

    if isinstance(value, str):
        clean_value = value.replace(",", "")
        return float(clean_value)

    return float(value)
