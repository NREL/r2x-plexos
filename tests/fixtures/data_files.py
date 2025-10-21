import datetime
from collections.abc import Iterable

import pytest


def daterange(start_date: datetime.date, days: int) -> Iterable[datetime.date]:
    for n in range(days):
        yield start_date + datetime.timedelta(days=n)


@pytest.fixture
def datetime_single_component_data(tmp_path):
    def _datetime_component_data(
        *component_names: str, start_date: datetime.date, days: int, profile: list[float]
    ):
        if not component_names:
            raise ValueError("At least one component name must be provided.")

        header = "Datetime," + ",".join(component_names) + "\n"
        csv_lines = [header]

        for date in daterange(start_date, days):
            for hour, value in enumerate(profile):
                dt = datetime.datetime.combine(date, datetime.time(hour=hour))
                row = f"{dt.isoformat()}," + ",".join(str(value) for _ in component_names)
                csv_lines.append(row + "\n")

        output_fpath = tmp_path / "datetime_series.csv"
        output_fpath.write_text("".join(csv_lines))
        return output_fpath

    return _datetime_component_data


@pytest.fixture
def monthly_component_data(tmp_path):
    def _monthly_component_data(component_values: dict[str, dict[str, float]]):
        component_names = list(component_values.keys())
        header = "Name," + ",".join(f"M{i:02d}" for i in range(1, 13)) + "\n"
        csv_lines = [header]

        for comp_name in component_names:
            values = component_values[comp_name]
            row_values = [str(values.get(f"M{i:02d}", 0.0)) for i in range(1, 13)]
            csv_lines.append(f"{comp_name}," + ",".join(row_values) + "\n")

        output_fpath = tmp_path / "monthly_data.csv"
        output_fpath.write_text("".join(csv_lines))
        return output_fpath

    return _monthly_component_data


@pytest.fixture
def value_component_data(tmp_path):
    def _value_component_data(component_values: dict[str, float]):
        header = "Name,Value\n"
        csv_lines = [header]

        for comp_name, value in component_values.items():
            csv_lines.append(f"{comp_name},{value}\n")

        output_fpath = tmp_path / "value_data.csv"
        output_fpath.write_text("".join(csv_lines))
        return output_fpath

    return _value_component_data


@pytest.fixture
def multi_year_data_file(tmp_path):
    weather_years = [2020, 2021, 2022]
    fpaths = []
    for year in weather_years:
        csv_content = "Month,Day,Period,r1,r2\n"
        for period in range(1, 25):
            region_1_value = 1000 * period + year
            region_2_value = 1200 * period + year
            line = f"1,1,{period},{region_1_value},{region_2_value}\n"
            csv_content += line
        datafile_path = tmp_path / f"Load_{year}.csv"
        fpaths.append(str(datafile_path))
        datafile_path.write_text(csv_content)

    return fpaths


@pytest.fixture
def year_daily_hour(tmp_path):
    import random

    start_year = 2026
    start_date = datetime.date(start_year, 1, 1)
    end_date = datetime.date(2030, 12, 31)

    total_days = (end_date - start_date).days + 1

    hourly_columns = [f"{i + 1}" for i in range(24)]
    header = "Year,Month,Day," + ",".join(hourly_columns) + ",r1,r2\n"

    data_lines = [header]

    for date in daterange(start_date, total_days):
        row_elements = [str(date.year), str(date.month), str(date.day)]
        hourly_data = [random.randint(100, 50000) for _ in range(24)]
        row_elements.extend(list(map(str, hourly_data)))
        r1_value = random.randint(100, 50000)
        r2_value = random.randint(100, 50000)
        row_elements.extend([str(r1_value), str(r2_value)])
        data_lines.append(",".join(row_elements) + "\n")

    output_fpath = tmp_path / "year_daily_hour.csv"
    output_fpath.write_text("".join(data_lines))
    return output_fpath
