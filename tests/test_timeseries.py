"""Tests for time series parsing functionality."""

from pathlib import Path
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from r2x_core import DataFile, DataStore
from r2x_plexos import PLEXOSParser
from r2x_plexos.config import PLEXOSConfig
from r2x_plexos.parser import TimeSeriesReference, TimeSeriesSourceType


@pytest.fixture
def parser_basic(data_folder) -> PLEXOSParser:
    """Create a basic parser instance for testing."""
    config = PLEXOSConfig(model_name="Base", timeseries_dir=None, reference_year=2023)
    data_file = DataFile(name="xml_file", glob="*.xml")
    store = DataStore(folder=data_folder)
    store.add_data_file(data_file)
    return PLEXOSParser(config, store)


@pytest.fixture
def parser_with_timeseries_dir(data_folder) -> PLEXOSParser:
    """Create a parser with timeseries_dir configured."""
    config = PLEXOSConfig(model_name="Base", timeseries_dir="timeseries", reference_year=2023)
    data_file = DataFile(name="xml_file", glob="*.xml")
    store = DataStore(folder=data_folder)
    store.add_data_file(data_file)
    return PLEXOSParser(config, store)


def test_resolve_datafile_path_basic(parser_basic: PLEXOSParser, data_folder: Path) -> None:
    """Test basic datafile path resolution."""
    result = parser_basic._resolve_datafile_path("test_file.csv")
    expected = data_folder / "test_file.csv"
    assert result == expected


def test_resolve_datafile_path_with_timeseries_dir(
    parser_with_timeseries_dir: PLEXOSParser, data_folder: Path
) -> None:
    """Test datafile path resolution with timeseries_dir."""
    result = parser_with_timeseries_dir._resolve_datafile_path("test_file.csv")
    expected = data_folder / "timeseries" / "test_file.csv"
    assert result == expected


def test_resolve_datafile_path_windows_style(parser_basic: PLEXOSParser, data_folder: Path) -> None:
    """Test Windows-style path normalization."""
    result = parser_basic._resolve_datafile_path("TimeSeries\\Annual\\test_file.csv")
    expected = data_folder / "TimeSeries" / "Annual" / "test_file.csv"
    assert result == expected


def test_resolve_datafile_path_subdirectory(parser_basic: PLEXOSParser, data_folder: Path) -> None:
    """Test path resolution with subdirectories."""
    result = parser_basic._resolve_datafile_path("data/annual/test.csv")
    expected = data_folder / "data" / "annual" / "test.csv"
    assert result == expected


def test_resolve_datafile_path_none(parser_basic: PLEXOSParser) -> None:
    """Test that None path raises ValueError."""
    with pytest.raises(ValueError, match="No datafile path provided"):
        parser_basic._resolve_datafile_path(None)


def test_resolve_datafile_path_empty(parser_basic: PLEXOSParser) -> None:
    """Test that empty path raises ValueError."""
    with pytest.raises(ValueError, match="No datafile path provided"):
        parser_basic._resolve_datafile_path("")


def test_get_or_parse_timeseries_value_file(parser_basic: PLEXOSParser, data_folder: Path) -> None:
    """Test parsing a simple value file."""
    csv_path = data_folder / "test_value.csv"
    csv_path.write_text("Name,Value\nGenerator1,150.0\nGenerator2,250.0")

    ts = parser_basic._get_or_parse_timeseries(
        file_path=str(csv_path), component_name="Generator1", reference_year=2023, timeslices=None
    )

    assert ts is not None
    assert len(ts.data) == 8760
    assert all(v == 150.0 for v in ts.data)


def test_get_or_parse_timeseries_caching(parser_basic: PLEXOSParser, data_folder: Path) -> None:
    """Test that parsed files are cached."""
    csv_path = data_folder / "test_cache.csv"
    csv_path.write_text("Name,Value\nGen1,100.0\nGen2,200.0")

    _ = parser_basic._get_or_parse_timeseries(
        file_path=str(csv_path), component_name="Gen1", reference_year=2023
    )
    assert str(csv_path) in parser_basic._parsed_files_cache

    ts2 = parser_basic._get_or_parse_timeseries(
        file_path=str(csv_path), component_name="Gen2", reference_year=2023
    )
    assert ts2 is not None
    assert all(v == 200.0 for v in ts2.data)


def test_get_or_parse_timeseries_component_not_found(parser_basic: PLEXOSParser, data_folder: Path) -> None:
    """Test error when component not in file."""
    csv_path = data_folder / "test_missing.csv"
    csv_path.write_text("Name,Value\nGenerator1,150.0")

    with pytest.raises(ValueError, match="Component NonExistent not found"):
        parser_basic._get_or_parse_timeseries(
            file_path=str(csv_path), component_name="NonExistent", reference_year=2023
        )


def test_attach_direct_datafile_timeseries_success(parser_basic: PLEXOSParser, data_folder: Path) -> None:
    """Test successful time series attachment."""
    csv_path = data_folder / "generator_data.csv"
    csv_path.write_text("Name,Value\nGen1,250.0\nGen2,350.0")

    mock_component = MagicMock()
    test_uuid = UUID("12345678-1234-5678-1234-567812345678")
    mock_component.uuid = test_uuid
    mock_component.name = "Gen1"

    parser_basic.system = MagicMock()
    parser_basic.system.get_component_by_uuid.return_value = mock_component

    ref = TimeSeriesReference(
        component_uuid=test_uuid,
        component_name="Gen1",
        field_name="max_capacity",
        source_type=TimeSeriesSourceType.DIRECT_DATAFILE,
        datafile_path="generator_data.csv",
        units="MW",
        property_name="Max Capacity",
    )

    parser_basic._attach_direct_datafile_timeseries(
        ref=ref, reference_year=2023, timeslices=None, horizon=("2023-01-01", "2023-12-31")
    )

    assert (test_uuid, "max_capacity") in parser_basic._attached_timeseries
    assert parser_basic._attached_timeseries[(test_uuid, "max_capacity")] is True

    parser_basic.system.add_time_series.assert_called_once()
    args, kwargs = parser_basic.system.add_time_series.call_args
    assert args[1] == mock_component  # Second arg is component
    assert "horizon" in kwargs
    assert kwargs["horizon"] == ("2023-01-01", "2023-12-31")


def test_attach_direct_datafile_timeseries_component_not_found_in_system(
    parser_basic: PLEXOSParser, data_folder: Path
) -> None:
    """Test error when component not in system."""
    csv_path = data_folder / "test.csv"
    csv_path.write_text("Name,Value\nGen1,100.0")

    parser_basic.system = MagicMock()
    parser_basic.system.get_component_by_uuid.return_value = None

    test_uuid = UUID("12345678-1234-5678-1234-567812345678")
    ref = TimeSeriesReference(
        component_uuid=test_uuid,
        component_name="MissingGen",
        field_name="capacity",
        source_type=TimeSeriesSourceType.DIRECT_DATAFILE,
        datafile_path="test.csv",
    )

    with pytest.raises(ValueError, match=r"Component MissingGen.*"):
        parser_basic._attach_direct_datafile_timeseries(
            ref=ref, reference_year=2023, timeslices=None, horizon=None
        )


def test_attach_direct_datafile_timeseries_already_attached(
    parser_basic: PLEXOSParser, data_folder: Path
) -> None:
    """Test that already attached time series are skipped."""
    test_uuid = UUID("12345678-1234-5678-1234-567812345678")

    parser_basic._attached_timeseries[(test_uuid, "capacity")] = True

    ref = TimeSeriesReference(
        component_uuid=test_uuid,
        component_name="Gen1",
        field_name="capacity",
        source_type=TimeSeriesSourceType.DIRECT_DATAFILE,
        datafile_path="test.csv",
    )

    parser_basic.system = MagicMock()

    parser_basic._attach_direct_datafile_timeseries(
        ref=ref, reference_year=2023, timeslices=None, horizon=None
    )

    parser_basic.system.get_component_by_uuid.assert_not_called()


def test_attach_direct_datafile_timeseries_file_not_found(
    parser_basic: PLEXOSParser, data_folder: Path
) -> None:
    """Test handling of missing datafile."""
    mock_component = MagicMock()
    test_uuid = UUID("12345678-1234-5678-1234-567812345678")
    mock_component.uuid = test_uuid

    parser_basic.system = MagicMock()
    parser_basic.system.get_component_by_uuid.return_value = mock_component

    ref = TimeSeriesReference(
        component_uuid=test_uuid,
        component_name="Gen1",
        field_name="capacity",
        source_type=TimeSeriesSourceType.DIRECT_DATAFILE,
        datafile_path="nonexistent.csv",
    )

    with pytest.raises(FileNotFoundError, match="Time series file not found"):
        parser_basic._attach_direct_datafile_timeseries(
            ref=ref, reference_year=2023, timeslices=None, horizon=None
        )


def test_build_time_series_integration(parser_basic: PLEXOSParser, data_folder: Path) -> None:
    """Test the full build_time_series workflow."""
    gen_csv = data_folder / "generators.csv"
    gen_csv.write_text("Name,Value\nGen1,500.0\nGen2,750.0\nGen3,1000.0")

    load_csv = data_folder / "loads.csv"
    load_csv.write_text("Name,Value\nLoad1,200.0\nLoad2,300.0")

    gen1 = MagicMock()
    gen1_uuid = UUID("11111111-1111-1111-1111-111111111111")
    gen1.uuid = gen1_uuid
    gen1.name = "Gen1"

    gen2 = MagicMock()
    gen2_uuid = UUID("22222222-2222-2222-2222-222222222222")
    gen2.uuid = gen2_uuid
    gen2.name = "Gen2"

    load1 = MagicMock()
    load1_uuid = UUID("33333333-3333-3333-3333-333333333333")
    load1.uuid = load1_uuid
    load1.name = "Load1"

    parser_basic.system = MagicMock()

    def get_component_by_uuid_side_effect(uuid):
        if uuid == gen1_uuid:
            return gen1
        elif uuid == gen2_uuid:
            return gen2
        elif uuid == load1_uuid:
            return load1
        return None

    parser_basic.system.get_component_by_uuid.side_effect = get_component_by_uuid_side_effect
    parser_basic.system.get_components.return_value = []  # No timeslices

    parser_basic.time_series_references = [
        TimeSeriesReference(
            component_uuid=gen1_uuid,
            component_name="Gen1",
            field_name="max_capacity",
            source_type=TimeSeriesSourceType.DIRECT_DATAFILE,
            datafile_path="generators.csv",
            units="MW",
        ),
        TimeSeriesReference(
            component_uuid=gen2_uuid,
            component_name="Gen2",
            field_name="max_capacity",
            source_type=TimeSeriesSourceType.DIRECT_DATAFILE,
            datafile_path="generators.csv",
            units="MW",
        ),
        TimeSeriesReference(
            component_uuid=load1_uuid,
            component_name="Load1",
            field_name="load",
            source_type=TimeSeriesSourceType.DIRECT_DATAFILE,
            datafile_path="loads.csv",
            units="MW",
        ),
        TimeSeriesReference(
            component_uuid=UUID("44444444-4444-4444-4444-444444444444"),
            component_name="MissingComponent",
            field_name="value",
            source_type=TimeSeriesSourceType.DIRECT_DATAFILE,
            datafile_path="generators.csv",
        ),
    ]

    parser_basic.build_time_series()

    assert len(parser_basic._attached_timeseries) == 3  # 3 successful attachments
    assert (gen1_uuid, "max_capacity") in parser_basic._attached_timeseries
    assert (gen2_uuid, "max_capacity") in parser_basic._attached_timeseries
    assert (load1_uuid, "load") in parser_basic._attached_timeseries

    assert parser_basic.system.add_time_series.call_count == 3

    assert len(parser_basic._failed_references) == 1
    failed_ref, error_msg = parser_basic._failed_references[0]
    assert failed_ref.component_name == "MissingComponent"
    assert "not found in system" in error_msg

    assert str(gen_csv) in parser_basic._parsed_files_cache
