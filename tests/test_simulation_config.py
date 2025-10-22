"""Tests for simulation configuration functionality."""

from pathlib import Path

import pytest
from plexosdb import ClassEnum, PlexosDB

from r2x_plexos.models.simulation_config import (
    PLEXOSPASA,
    PLEXOSDiagnostic,
    PLEXOSMTSchedule,
    PLEXOSPerformance,
    PLEXOSProduction,
    PLEXOSReport,
    PLEXOSSTSchedule,
    PLEXOSTransmission,
)
from r2x_plexos.utils_plexosdb import validate_simulation_attribute
from r2x_plexos.utils_simulation import (
    build_plexos_simulation,
    convert_simulation_config_to_attributes,
    get_default_simulation_config,
    ingest_simulation_config_to_plexosdb,
    validate_simulation_config,
)


@pytest.fixture
def plexos_db():
    """Create a PlexosDB instance with populated schema from test XML."""
    return PlexosDB.from_xml(Path("tests/data/5_bus_system_variables.xml"))


def test_validate_simulation_attribute_valid(plexos_db):
    """Test validation with valid attribute."""
    result = validate_simulation_attribute(plexos_db, ClassEnum.Performance, "SOLVER")
    assert result.is_ok()

    result = validate_simulation_attribute(plexos_db, ClassEnum.STSchedule, "Transmission Detail")
    assert result.is_ok()


def test_validate_simulation_attribute_invalid(plexos_db):
    """Test validation with invalid attribute."""
    result = validate_simulation_attribute(plexos_db, ClassEnum.Performance, "InvalidAttribute")
    assert result.is_err()
    assert "Invalid attribute" in result.error

    result = validate_simulation_attribute(plexos_db, ClassEnum.Production, "NotARealAttribute")
    assert result.is_err()


def test_get_default_simulation_config():
    """Test default template generation."""
    defaults = get_default_simulation_config()

    assert "mt_schedule" in defaults
    assert "st_schedule" in defaults
    assert "production" in defaults
    assert "pasa" in defaults
    assert "performance" in defaults
    assert "report" in defaults
    assert "transmission" in defaults
    assert "diagnostic" in defaults

    # Check that all defaults are proper instances
    assert isinstance(defaults["mt_schedule"], PLEXOSMTSchedule)
    assert isinstance(defaults["st_schedule"], PLEXOSSTSchedule)
    assert isinstance(defaults["production"], PLEXOSProduction)
    assert isinstance(defaults["pasa"], PLEXOSPASA)
    assert isinstance(defaults["performance"], PLEXOSPerformance)
    assert isinstance(defaults["report"], PLEXOSReport)
    assert isinstance(defaults["transmission"], PLEXOSTransmission)
    assert isinstance(defaults["diagnostic"], PLEXOSDiagnostic)

    # Check some default values
    assert defaults["performance"].solver == 4  # Gurobi
    assert defaults["st_schedule"].transmission_detail == 1  # Nodal


def test_convert_simulation_config_to_attributes():
    """Test Pydantic to dict conversion."""
    perf = PLEXOSPerformance(
        name="MyPerf",
        solver=4,
        mip_relative_gap=0.01,
        mip_maximum_threads=20,
    )

    result = convert_simulation_config_to_attributes(perf)
    assert result.is_ok()

    attrs = result.unwrap()
    assert "SOLVER" in attrs
    assert attrs["SOLVER"] == 4
    assert "MIP Relative Gap" in attrs
    assert attrs["MIP Relative Gap"] == 0.01
    assert "MIP Maximum Threads" in attrs
    assert attrs["MIP Maximum Threads"] == 20


def test_validate_simulation_config_success(plexos_db):
    """Test full config validation (valid)."""
    perf = PLEXOSPerformance(
        name="ValidPerf",
        solver=4,
        mip_relative_gap=0.01,
    )

    result = validate_simulation_config(plexos_db, ClassEnum.Performance, perf)
    assert result.is_ok()


def test_validate_simulation_config_failure():
    """Test full config validation (invalid)."""
    # Create a custom config with an invalid field
    # Note: This test is tricky because Pydantic validates field names at creation time
    # So we test the validation logic by using a valid object first
    perf = PLEXOSPerformance(name="TestPerf", solver=4)

    # Create an in-memory DB (won't have proper schema)
    db = PlexosDB()

    # This should fail because the in-memory DB doesn't have attribute schema
    result = validate_simulation_config(db, ClassEnum.Performance, perf)
    assert result.is_err()


def test_ingest_simulation_config_to_plexosdb(plexos_db):
    """Test single simulation config ingestion."""
    perf = PLEXOSPerformance(
        name="Test_Performance",
        solver=4,
        mip_relative_gap=0.01,
        mip_maximum_threads=20,
    )

    result = ingest_simulation_config_to_plexosdb(
        plexos_db,
        ClassEnum.Performance,
        perf,
        validate=True,
    )

    assert result.is_ok()
    info = result.unwrap()

    assert info["object_name"] == "Test_Performance"
    assert info["class"] == "Performance"
    assert info["attribute_count"] == 4  # solver, gap, threads, max_time
    assert "SOLVER" in info["attributes_added"]
    assert "MIP Relative Gap" in info["attributes_added"]


def test_build_with_simulation_config():
    """Test build_plexos_simulation with sim config."""
    # Create simulation configs
    sim_configs = {
        "performance": PLEXOSPerformance(name="MyPerf", solver=4),
        "report": PLEXOSReport(
            name="MyReport",
            output_results_by_day=-1,
            output_results_by_month=-1,
        ),
    }

    result = build_plexos_simulation(
        {"horizon_year": 2012, "resolution": "1D"},
        simulation_config=sim_configs,
    )

    assert result.is_ok()
    build_result = result.unwrap()

    assert len(build_result.models) == 1
    assert len(build_result.horizons) == 1
    assert build_result.simulation_configs is not None
    assert "performance" in build_result.simulation_configs
    assert "report" in build_result.simulation_configs


def test_ingest_full_simulation_with_config():
    """Test simulation config objects are included in build result."""
    # Create simulation configs
    sim_configs = {
        "performance": PLEXOSPerformance(
            name="Test_Perf",
            solver=4,
            mip_relative_gap=0.02,
        ),
        "st_schedule": PLEXOSSTSchedule(
            name="Test_ST",
            transmission_detail=1,
            heat_rate_detail=2,
        ),
        "diagnostic": None,  # Test that None values are skipped
    }

    # Build simulation
    build_result_res = build_plexos_simulation(
        {"horizon_year": 2012, "resolution": "1D"},
        simulation_config=sim_configs,
    )
    assert build_result_res.is_ok()
    build_result = build_result_res.unwrap()

    # Verify simulation configs are in the result
    assert build_result.simulation_configs is not None
    assert "performance" in build_result.simulation_configs
    assert "st_schedule" in build_result.simulation_configs
    assert "diagnostic" in build_result.simulation_configs

    # Verify the config objects
    assert build_result.simulation_configs["performance"].name == "Test_Perf"
    assert build_result.simulation_configs["st_schedule"].name == "Test_ST"
    assert build_result.simulation_configs["diagnostic"] is None

    # Verify models and horizons are also present
    assert len(build_result.models) == 1
    assert len(build_result.horizons) == 1


def test_simulation_config_examples():
    """Test all .example() methods work."""
    mt_schedule = PLEXOSMTSchedule.example()
    assert mt_schedule.name == "MT_Schedule_Example"
    assert mt_schedule.step_type == 4

    st_schedule = PLEXOSSTSchedule.example()
    assert st_schedule.name == "ST_Schedule_Example"
    assert st_schedule.transmission_detail == 1

    production = PLEXOSProduction.example()
    assert production.name == "Production_Example"
    assert production.unit_commitment_optimality == 2

    pasa = PLEXOSPASA.example()
    assert pasa.name == "PASA_Example"
    assert pasa.step_type == 1

    performance = PLEXOSPerformance.example()
    assert performance.name == "Performance_Example"
    assert performance.solver == 4

    report = PLEXOSReport.example()
    assert report.name == "Report_Example"
    assert report.output_results_by_day == -1

    transmission = PLEXOSTransmission.example()
    assert transmission.name == "Transmission_Example"
    assert transmission.of_method == 1

    diagnostic = PLEXOSDiagnostic.example()
    assert diagnostic.name == "Diagnostic_Example"
