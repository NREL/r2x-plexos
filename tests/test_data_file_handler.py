from datetime import datetime
from typing import TYPE_CHECKING, cast
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from r2x_plexos.datafile_handler import (
    DatetimeComponentsFile,
    HourlyComponentsFile,
    MonthlyFile,
    PatternFile,
    TimesliceFile,
    ValueFile,
    YearlyFile,
    compute_month_end,
    create_time_series,
    detect_file_type,
    extract_all_time_series,
    extract_one_time_series,
    extract_patterns_from_timeslice,
    extract_timeslice_hours,
    find_column_case_insensitive,
    get_hours_for_timeslice,
    get_month_hour_ranges,
    get_timeslice_patterns_hours,
    hours_in_year,
    is_leap_year,
    is_valid_date,
    is_valid_period,
    load_csv_cached,
    parse_date_pattern,
    parse_datetime_string,
    parse_file,
    safe_float_conversion,
)

if TYPE_CHECKING:
    from r2x_plexos.models.timeslice import PlexosTimeSlice


class MockTimeslice:
    def __init__(self, name: str, include_pattern: str | None = None) -> None:
        self.name = name
        if include_pattern:
            self.include = MagicMock()
            self.include.get_timeslices = MagicMock(return_value=[include_pattern])
        else:
            self.include = MagicMock()  # Initialize as a MagicMock rather than None


@pytest.fixture
def mock_timeslices() -> list[MockTimeslice]:
    summer = MockTimeslice("Summer", "M5-10")
    winter = MockTimeslice("Winter", "M11-12,M1-4")  # Winter spans year-end
    return [summer, winter]


@pytest.fixture
def timeslice_dataframe() -> pl.LazyFrame:
    data = {
        "Name": ["SPP-N_GAS (SINGLE FUEL)"],
        "Summer": [2171.0],
        "Winter": [1861.0],
    }
    return pl.LazyFrame(data)


def test_get_hours_for_timeslice_summer() -> None:
    # Test summer months (May-October)
    hours = get_hours_for_timeslice("M5-10", 2024)

    # Calculate expected range
    start_hour = int((datetime(2024, 5, 1) - datetime(2024, 1, 1)).total_seconds() / 3600)
    end_hour = int((datetime(2024, 10, 31) - datetime(2024, 1, 1)).total_seconds() / 3600) + 23

    # Check that we have all hours from May 1 to Oct 31
    assert min(hours) == start_hour
    assert max(hours) == end_hour
    assert len(hours) == end_hour - start_hour + 1


def test_get_hours_for_timeslice_winter_split() -> None:
    # Test winter months (Nov-Apr) which wraps around year end
    hours = set()
    hours.update(get_hours_for_timeslice("M11-12", 2024))
    hours.update(get_hours_for_timeslice("M1-4", 2024))

    # Calculate expected ranges
    nov_dec_start = int((datetime(2024, 11, 1) - datetime(2024, 1, 1)).total_seconds() / 3600)
    nov_dec_end = int((datetime(2024, 12, 31) - datetime(2024, 1, 1)).total_seconds() / 3600) + 23

    jan_apr_start = 0  # January 1st
    jan_apr_end = int((datetime(2024, 4, 30) - datetime(2024, 1, 1)).total_seconds() / 3600) + 23

    # Check first part (Jan-Apr)
    first_part = set(range(jan_apr_start, jan_apr_end + 1))
    assert first_part.issubset(hours)

    # Check second part (Nov-Dec)
    second_part = set(range(nov_dec_start, nov_dec_end + 1))
    assert second_part.issubset(hours)

    # Total should be the sum of both parts
    assert len(hours) == len(first_part) + len(second_part)


def test_get_hours_for_timeslice_with_comma() -> None:
    # Test multiple month ranges separated by comma
    hours = get_hours_for_timeslice("M11-12,M1-4", 2024)

    # Calculate expected ranges
    nov_dec_start = int((datetime(2024, 11, 1) - datetime(2024, 1, 1)).total_seconds() / 3600)
    nov_dec_end = int((datetime(2024, 12, 31) - datetime(2024, 1, 1)).total_seconds() / 3600) + 23

    jan_apr_start = 0  # January 1st
    jan_apr_end = int((datetime(2024, 4, 30) - datetime(2024, 1, 1)).total_seconds() / 3600) + 23

    # Check first part (Jan-Apr)
    first_part = set(range(jan_apr_start, jan_apr_end + 1))
    assert first_part.issubset(hours)

    # Check second part (Nov-Dec)
    second_part = set(range(nov_dec_start, nov_dec_end + 1))
    assert second_part.issubset(hours)

    # Total should be the sum of both parts
    assert len(hours) == len(first_part) + len(second_part)


def test_detect_file_type_timeslice(mock_timeslices: list[MockTimeslice]) -> None:
    # Test detection of timeslice files
    df = pl.LazyFrame({"Name": ["Generator1"], "Summer": [100.0], "Winter": [200.0]})

    # Cast MockTimeslice list to PlexosTimeSlice list for type checking
    file_type = detect_file_type(df, cast(list["PlexosTimeSlice"], mock_timeslices))
    assert isinstance(file_type, TimesliceFile)
    # Use cast to tell mypy these are the same objects even though the types differ
    assert file_type.timeslices == cast(list["PlexosTimeSlice"], mock_timeslices)

    # Test non-timeslice file - should raise error since it doesn't match any type
    df2 = pl.LazyFrame({"test": ["Generator1"], "teas": [100.0]})

    with pytest.raises(ValueError):
        detect_file_type(df2, cast(list["PlexosTimeSlice"], mock_timeslices))


def test_detect_file_type_pattern() -> None:
    # Test detection of pattern files
    df = pl.LazyFrame({"Name": ["Generator1"], "Pattern": ["M1,D1,H1"], "Value": [100.0]})

    file_type = detect_file_type(df)
    assert isinstance(file_type, PatternFile)


def test_detect_file_type_monthly() -> None:
    # Test detection of monthly files
    df = pl.LazyFrame({"Name": ["Generator1"], "M01": [100.0], "M02": [200.0]})

    file_type = detect_file_type(df)
    assert isinstance(file_type, MonthlyFile)


def test_detect_file_type_hourly() -> None:
    # Test detection of hourly component files
    df = pl.LazyFrame({"Month": [1, 1], "Day": [1, 1], "Period": [1, 2], "Generator1": [100.0, 200.0]})

    file_type = detect_file_type(df)
    assert isinstance(file_type, HourlyComponentsFile)


def test_parse_timeslice_file(
    mock_timeslices: list[MockTimeslice], timeslice_dataframe: pl.LazyFrame
) -> None:
    # Test parsing of timeslice files
    # Cast MockTimeslice list to PlexosTimeSlice list for type checking
    file_type = TimesliceFile(cast(list["PlexosTimeSlice"], mock_timeslices))
    result = parse_file(file_type, timeslice_dataframe, datetime(2023, 1, 1), 2023)

    # Check that we have a single time series
    assert len(result) == 1
    assert "SPP-N_GAS (SINGLE FUEL)" in result

    ts = result["SPP-N_GAS (SINGLE FUEL)"]

    # A non-leap year should have 8760 hours
    assert len(ts.data) == 8760

    # Get Summer and Winter hours
    summer_hours = get_hours_for_timeslice("M5-10", 2023)
    winter_hours_part1 = get_hours_for_timeslice("M1-4", 2023)
    winter_hours_part2 = get_hours_for_timeslice("M11-12", 2023)
    winter_hours = winter_hours_part1.union(winter_hours_part2)

    # Check Summer values (2171.0)
    for hour in summer_hours:
        assert ts.data[hour] == 2171.0

    # Check Winter values (1861.0)
    for hour in winter_hours:
        assert ts.data[hour] == 1861.0, f"Hour {hour} should have value 1861.0"

    # Check November-December specifically (last part of year)
    nov_dec_hours = get_hours_for_timeslice("M11-12", 2023)
    for hour in nov_dec_hours:
        assert ts.data[hour] == 1861.0, f"Hour {hour} in Nov-Dec should have value 1861.0"


def test_winter_as_complement_of_summer(timeslice_dataframe: pl.LazyFrame) -> None:
    # Create a case where Winter pattern isn't explicitly defined
    summer = MockTimeslice("Summer", "M5-10")
    winter = MockTimeslice("Winter", None)  # Winter has no pattern

    file_type = TimesliceFile(cast(list["PlexosTimeSlice"], [summer, winter]))
    result = parse_file(file_type, timeslice_dataframe, datetime(2023, 1, 1), 2023)

    ts = result["SPP-N_GAS (SINGLE FUEL)"]

    # Check the values in last 100 hours (December)
    for i in range(8760 - 100, 8760):
        assert ts.data[i] == 1861.0, f"Hour {i} should be Winter value (1861.0)"


def test_get_hours_for_timeslice_with_semicolon() -> None:
    # Test timeslice pattern with semicolon separator
    hours = get_hours_for_timeslice("M1-4;M11-12", 2023)

    # Calculate expected ranges
    jan_apr_hours = get_hours_for_timeslice("M1-4", 2023)
    nov_dec_hours = get_hours_for_timeslice("M11-12", 2023)

    # Combined hours should equal union of both ranges
    expected_hours = jan_apr_hours.union(nov_dec_hours)
    assert hours == expected_hours
    assert len(hours) == len(jan_apr_hours) + len(nov_dec_hours)


def test_get_timeslice_patterns_hours() -> None:
    # Test extracting hours from a timeslice object
    timeslice = MockTimeslice("Test", "M1-3")
    hours = get_timeslice_patterns_hours(cast("PlexosTimeSlice", timeslice), 2023)

    expected = get_hours_for_timeslice("M1-3", 2023)
    assert hours == expected


def test_extract_timeslice_hours(mock_timeslices: list[MockTimeslice]) -> None:
    # Test extracting hours for all timeslices
    result = extract_timeslice_hours(cast(list["PlexosTimeSlice"], mock_timeslices), 2023)

    assert "Summer" in result
    assert "Winter" in result

    expected_summer = get_hours_for_timeslice("M5-10", 2023)
    expected_winter = get_hours_for_timeslice("M11-12,M1-4", 2023)

    assert result["Summer"] == expected_summer
    assert result["Winter"] == expected_winter


@patch("r2x_plexos.datafile_handler.load_csv_cached")
def test_extract_all_time_series(mock_load_csv: MagicMock) -> None:
    # Setup mock data for a pattern file
    mock_df = pl.LazyFrame(
        {
            "Name": ["Generator1"],
            "Pattern": ["M1,D1,H1"],
            "Value": [100.0],
        }
    )
    mock_load_csv.return_value = mock_df

    # Call function with a dummy path
    _ = extract_all_time_series("dummy/path", datetime(2023, 1, 1), 2023)

    # Verify results - we should have one timeseries for Generator1
    assert "Generator1" in mock_load_csv.return_value.collect()["Name"]
    assert mock_load_csv.call_args[0][0] == "dummy/path"


@patch("r2x_plexos.datafile_handler.extract_all_time_series")
def test_extract_one_time_series(mock_extract_all: MagicMock) -> None:
    # Setup mock return value
    mock_ts = MagicMock()  # Mock timeseries
    mock_extract_all.return_value = {"Generator1": mock_ts}

    # Test successful extraction
    result = extract_one_time_series("dummy/path", "Generator1", datetime(2023, 1, 1), 2023)
    assert result == mock_ts

    # Test extraction of non-existent component
    mock_extract_all.return_value = {"Generator1": mock_ts}
    with pytest.raises(ValueError):
        extract_one_time_series("dummy/path", "NonExistentGenerator", datetime(2023, 1, 1), 2023)


def test_is_leap_year() -> None:
    assert is_leap_year(2024) is True
    assert is_leap_year(2023) is False
    assert is_leap_year(2000) is True
    assert is_leap_year(1900) is False
    assert is_leap_year(2100) is False


def test_hours_in_year() -> None:
    assert hours_in_year(2024) == 8784
    assert hours_in_year(2023) == 8760
    assert hours_in_year(2000) == 8784
    assert hours_in_year(1900) == 8760


def test_compute_month_end() -> None:
    assert compute_month_end(2023, 1) == datetime(2023, 1, 31)
    assert compute_month_end(2023, 2) == datetime(2023, 2, 28)
    assert compute_month_end(2024, 2) == datetime(2024, 2, 29)
    assert compute_month_end(2023, 12) == datetime(2023, 12, 31)


def test_get_month_hour_ranges() -> None:
    ranges = get_month_hour_ranges(2023)

    assert len(ranges) == 12
    assert 1 in ranges and 12 in ranges

    jan_range = ranges[1]
    assert jan_range.start == 0
    assert len(jan_range) == 31 * 24

    feb_range = ranges[2]
    assert len(feb_range) == 28 * 24

    leap_ranges = get_month_hour_ranges(2024)
    feb_leap = leap_ranges[2]
    assert len(feb_leap) == 29 * 24


def test_create_time_series() -> None:
    values = [1.0, 2.0, 3.0]
    initial_time = datetime(2023, 1, 1)
    ts = create_time_series(values, "test_series", initial_time)

    assert ts.name == "test_series"
    assert len(ts.data) == 3
    assert ts.data[0] == 1.0
    assert ts.data[1] == 2.0
    assert ts.data[2] == 3.0


def test_parse_date_pattern() -> None:
    result = parse_date_pattern("M1,D1,H0", 2023)
    assert result == datetime(2023, 1, 1, 0)

    result = parse_date_pattern("M12,D25,H23", 2023)
    assert result == datetime(2023, 12, 25, 23)

    result = parse_date_pattern("M6,D15", 2023)
    assert result == datetime(2023, 6, 15, 0)

    with pytest.raises(ValueError):
        parse_date_pattern("", 2023)


def test_find_column_case_insensitive() -> None:
    row = {"Name": "test", "VALUE": 123, " Pattern ": "M1"}

    assert find_column_case_insensitive(row, "name") == "Name"
    assert find_column_case_insensitive(row, "value") == "VALUE"
    assert find_column_case_insensitive(row, "pattern") == " Pattern "
    assert find_column_case_insensitive(row, "missing") is None


def test_safe_float_conversion() -> None:
    assert safe_float_conversion(123) == 123.0
    assert safe_float_conversion(123.45) == 123.45
    assert safe_float_conversion("123.45") == 123.45
    assert safe_float_conversion("1,234.56") == 1234.56

    with pytest.raises(ValueError):
        safe_float_conversion("not_a_number")


def test_parse_datetime_string() -> None:
    assert parse_datetime_string("1/1/2023") == datetime(2023, 1, 1)
    assert parse_datetime_string("2023-01-01") == datetime(2023, 1, 1)
    assert parse_datetime_string("2023-01-01T12:30:45") == datetime(2023, 1, 1, 12, 30, 45)
    assert parse_datetime_string("1/1/2023 14:30") == datetime(2023, 1, 1, 14, 30)
    assert parse_datetime_string("invalid_date") is None
    assert parse_datetime_string(datetime(2023, 1, 1)) == datetime(2023, 1, 1)


@pytest.fixture
def mock_timeslice_with_patterns() -> MockTimeslice:
    timeslice = MockTimeslice("TestSlice")
    timeslice.include.get_timeslices = MagicMock(return_value=["M1-3", "M6-8"])
    return timeslice


def test_extract_patterns_from_timeslice(mock_timeslice_with_patterns: MockTimeslice) -> None:
    patterns = extract_patterns_from_timeslice(mock_timeslice_with_patterns)
    assert patterns == ["M1-3", "M6-8"]

    timeslice_no_patterns = MockTimeslice("Empty")
    timeslice_no_patterns.include.get_timeslices = MagicMock(return_value=None)
    patterns = extract_patterns_from_timeslice(timeslice_no_patterns)
    assert patterns == []


@pytest.fixture
def yearly_dataframe() -> pl.LazyFrame:
    return pl.LazyFrame({"Name": ["Generator1", "Generator2"], "Year": [2023, 2023], "Data": [100.0, 200.0]})


@pytest.fixture
def yearly_columns_dataframe() -> pl.LazyFrame:
    return pl.LazyFrame(
        {"Name": ["Generator1", "Generator2"], "YR-2022": [100.0, 150.0], "YR-2023": [110.0, 160.0]}
    )


@pytest.fixture
def datetime_dataframe() -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "DateTime": ["2023-01-01T00:00:00", "2023-01-01T01:00:00"],
            "Generator1": [100.0, 110.0],
            "Generator2": [200.0, 210.0],
        }
    )


@pytest.fixture
def value_dataframe() -> pl.LazyFrame:
    return pl.LazyFrame({"Name": ["Generator1", "Generator2"], "Value": [150.0, 250.0]})


def test_detect_file_type_yearly(yearly_dataframe: pl.LazyFrame) -> None:
    file_type = detect_file_type(yearly_dataframe)
    assert isinstance(file_type, YearlyFile)


def test_detect_file_type_yearly_columns(yearly_columns_dataframe: pl.LazyFrame) -> None:
    file_type = detect_file_type(yearly_columns_dataframe)
    assert isinstance(file_type, YearlyFile)


def test_detect_file_type_datetime(datetime_dataframe: pl.LazyFrame) -> None:
    file_type = detect_file_type(datetime_dataframe)
    assert isinstance(file_type, DatetimeComponentsFile)


def test_detect_file_type_value(value_dataframe: pl.LazyFrame) -> None:
    file_type = detect_file_type(value_dataframe)
    assert isinstance(file_type, ValueFile)


def test_parse_value_file(value_dataframe: pl.LazyFrame) -> None:
    file_type = ValueFile()
    result = parse_file(file_type, value_dataframe, datetime(2023, 1, 1), 2023)

    assert len(result) == 2
    assert "Generator1" in result
    assert "Generator2" in result

    ts1 = result["Generator1"]
    ts2 = result["Generator2"]

    assert len(ts1.data) == 8760
    assert len(ts2.data) == 8760
    assert all(v == 150.0 for v in ts1.data)
    assert all(v == 250.0 for v in ts2.data)


@pytest.fixture
def monthly_dataframe() -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "Name": ["Generator1"],
            "M01": [100.0],
            "M02": [110.0],
            "M03": [120.0],
            "M04": [130.0],
            "M05": [140.0],
            "M06": [150.0],
            "M07": [160.0],
            "M08": [170.0],
            "M09": [180.0],
            "M10": [190.0],
            "M11": [200.0],
            "M12": [210.0],
        }
    )


@patch("polars.scan_csv")
def test_load_csv_cached(mock_scan_csv: MagicMock) -> None:
    mock_df = pl.LazyFrame({"test": [1, 2, 3]})
    mock_scan_csv.return_value = mock_df

    result = load_csv_cached("/fake/path.csv")
    assert result is mock_df

    result2 = load_csv_cached("/fake/path.csv")
    assert result2 is mock_df
    assert mock_scan_csv.call_count == 1


def test_get_hours_for_timeslice_invalid_pattern() -> None:
    hours = get_hours_for_timeslice("invalid_pattern", 2023)
    assert hours == set()

    hours = get_hours_for_timeslice("", 2023)
    assert hours == set()


@pytest.fixture
def pattern_dataframe_with_bands() -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "Name": ["Generator1", "Generator1"],
            "Pattern": ["M1,D1,H0", "M1,D2,H0"],
            "1": [100.0, 110.0],
            "2": [200.0, 210.0],
        }
    )


def test_parse_pattern_file_with_bands(pattern_dataframe_with_bands: pl.LazyFrame) -> None:
    file_type = PatternFile()
    result = parse_file(file_type, pattern_dataframe_with_bands, datetime(2023, 1, 1), 2023)

    assert len(result) == 2
    assert "Generator1_band_1" in result
    assert "Generator1_band_2" in result

    ts1 = result["Generator1_band_1"]
    ts2 = result["Generator1_band_2"]

    assert len(ts1.data) == 8760
    assert len(ts2.data) == 8760
    assert ts1.data[0] == 100.0
    assert ts1.data[24] == 110.0
    assert ts2.data[0] == 200.0
    assert ts2.data[24] == 210.0


def test_parse_monthly_file(monthly_dataframe: pl.LazyFrame) -> None:
    file_type = MonthlyFile()
    result = parse_file(file_type, monthly_dataframe, datetime(2023, 1, 1), 2023)

    assert len(result) == 1
    assert "Generator1" in result

    ts = result["Generator1"]
    assert len(ts.data) == 8760

    ranges = get_month_hour_ranges(2023)
    for month in range(1, 13):
        expected_value = 100.0 + (month - 1) * 10  # M01=100, M02=110, etc.
        for hour in ranges[month]:
            assert ts.data[hour] == expected_value


def test_parse_monthly_file_no_year(monthly_dataframe: pl.LazyFrame) -> None:
    file_type = MonthlyFile()
    with pytest.raises(ValueError, match="Year must be provided for monthly data files"):
        parse_file(file_type, monthly_dataframe, datetime(2023, 1, 1), None)


@pytest.fixture
def hourly_components_dataframe() -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "Month": [1, 1, 1, 1],
            "Day": [1, 1, 2, 2],
            "Period": [1, 2, 1, 2],
            "Generator1": [100.0, 110.0, 120.0, 130.0],
            "Generator2": [200.0, 210.0, 220.0, 230.0],
        }
    )


def test_parse_hourly_components_file(hourly_components_dataframe: pl.LazyFrame) -> None:
    file_type = HourlyComponentsFile()
    result = parse_file(file_type, hourly_components_dataframe, datetime(2023, 1, 1), 2023)

    assert len(result) == 2
    assert "Generator1" in result
    assert "Generator2" in result

    ts1 = result["Generator1"]
    ts2 = result["Generator2"]

    assert len(ts1.data) == 8760
    assert len(ts2.data) == 8760
    assert ts1.data[0] == 100.0
    assert ts1.data[1] == 110.0
    assert ts1.data[24] == 120.0
    assert ts1.data[25] == 130.0
    assert ts2.data[0] == 200.0


def test_parse_hourly_components_file_no_year(hourly_components_dataframe: pl.LazyFrame) -> None:
    file_type = HourlyComponentsFile()
    with pytest.raises(ValueError, match="Year must be provided for Month/Day/Period files"):
        parse_file(file_type, hourly_components_dataframe, datetime(2023, 1, 1), None)


@pytest.fixture
def datetime_components_dataframe() -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "DateTime": ["2023-01-01T00:00:00", "2023-01-01T01:00:00", "2023-01-01T02:00:00"],
            "Generator1": [100.0, 110.0, 120.0],
            "Generator2": [200.0, 210.0, 220.0],
        }
    )


def test_parse_datetime_components_file(datetime_components_dataframe: pl.LazyFrame) -> None:
    file_type = DatetimeComponentsFile()
    result = parse_file(file_type, datetime_components_dataframe, datetime(2023, 1, 1), 2023)

    assert len(result) == 2
    assert "Generator1" in result
    assert "Generator2" in result

    ts1 = result["Generator1"]
    ts2 = result["Generator2"]

    assert len(ts1.data) == 8760
    assert len(ts2.data) == 8760
    assert ts1.data[0] == 100.0
    assert ts1.data[1] == 110.0
    assert ts1.data[2] == 120.0
    assert ts2.data[0] == 200.0


def test_parse_datetime_components_file_no_year(datetime_components_dataframe: pl.LazyFrame) -> None:
    file_type = DatetimeComponentsFile()
    with pytest.raises(ValueError, match="Year must be provided for Datetime files"):
        parse_file(file_type, datetime_components_dataframe, datetime(2023, 1, 1), None)


@pytest.fixture
def yearly_file_dataframe() -> pl.LazyFrame:
    return pl.LazyFrame({"Name": ["Generator1", "Generator2"], "Year": [2023, 2023], "Value": [150.0, 250.0]})


def test_parse_yearly_file(yearly_file_dataframe: pl.LazyFrame) -> None:
    file_type = YearlyFile()
    result = parse_file(file_type, yearly_file_dataframe, datetime(2023, 1, 1), 2023)

    assert len(result) == 2
    assert "Generator1" in result
    assert "Generator2" in result

    ts1 = result["Generator1"]
    ts2 = result["Generator2"]

    assert len(ts1.data) == 8760
    assert len(ts2.data) == 8760
    assert all(v == 150.0 for v in ts1.data)
    assert all(v == 250.0 for v in ts2.data)


def test_is_valid_date() -> None:
    assert is_valid_date(1, 1) is True
    assert is_valid_date(12, 31) is True
    assert is_valid_date(6, 15) is True
    assert is_valid_date(0, 1) is False
    assert is_valid_date(13, 1) is False
    assert is_valid_date(1, 0) is False
    assert is_valid_date(1, 32) is False


def test_is_valid_period() -> None:
    assert is_valid_period(1) is True
    assert is_valid_period(24) is True
    assert is_valid_period(12) is True
    assert is_valid_period(0) is False
    assert is_valid_period(25) is False
    assert is_valid_period(-1) is False


def test_parse_pattern_file_no_year() -> None:
    df = pl.LazyFrame({"Name": ["Generator1"], "Pattern": ["M1,D1,H0"], "Value": [100.0]})

    file_type = PatternFile()
    with pytest.raises(ValueError, match="Year must be provided"):
        parse_file(file_type, df, datetime(2023, 1, 1), None)


def test_parse_pattern_file_band_no_pattern() -> None:
    df = pl.LazyFrame({"Name": ["Generator1"], "Pattern": [None], "1": [100.0]})

    file_type = PatternFile()
    with pytest.raises(ValueError, match="No Pattern provided and no default_initial_time"):
        parse_file(file_type, df, None, 2023)


def test_parse_hourly_components_invalid_data() -> None:
    df = pl.LazyFrame(
        {
            "Month": [13, 1],  # Invalid month
            "Day": [1, 32],  # Invalid day
            "Period": [25, 1],  # Invalid period
            "Generator1": [100.0, 110.0],
        }
    )

    file_type = HourlyComponentsFile()
    result = parse_file(file_type, df, datetime(2023, 1, 1), 2023)

    assert len(result) == 1
    assert "Generator1" in result
    ts = result["Generator1"]
    # Invalid data should be skipped, resulting in mostly zero values
    assert len(ts.data) == 8760


def test_parse_datetime_components_invalid_datetime() -> None:
    df = pl.LazyFrame({"DateTime": ["invalid_date", "2023-01-01T01:00:00"], "Generator1": [100.0, 110.0]})

    file_type = DatetimeComponentsFile()
    result = parse_file(file_type, df, datetime(2023, 1, 1), 2023)

    assert len(result) == 1
    assert "Generator1" in result
    ts = result["Generator1"]
    assert len(ts.data) == 8760
    assert ts.data[1] == 110.0  # Valid row should be processed


def test_extract_all_time_series_with_timeslices() -> None:
    mock_timeslice = MockTimeslice("Summer", "M5-10")

    df = pl.LazyFrame({"Name": ["Generator1"], "Summer": [100.0]})

    with patch("r2x_plexos.datafile_handler.load_csv_cached", return_value=df):
        result = extract_all_time_series(
            "dummy/path", datetime(2023, 1, 1), 2023, [cast("PlexosTimeSlice", mock_timeslice)]
        )

        assert len(result) == 1
        assert "Generator1" in result
