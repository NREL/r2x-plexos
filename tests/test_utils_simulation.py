"""Tests for simulation configuration builder utilities."""

import typing
from datetime import datetime
from pathlib import Path

import plexosdb
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
from r2x_plexos.plugin_config import PLEXOSConfig
from r2x_plexos.utils_plexosdb import validate_simulation_attribute
from r2x_plexos.utils_simulation import (
    build_plexos_simulation,
    convert_simulation_config_to_attributes,
    datetime_to_ole_date,
    get_default_simulation_config,
    get_enum_from_string,
    ingest_simulation_config_to_plexosdb,
    ingest_simulation_to_plexosdb,
    validate_simulation_config,
)

FILE_NAME = "master_10.0R2_btu.xml"
_XML_TEMPLATE_DIR = Path(plexosdb.__file__).parent / "config"


def test_datetime_to_ole_date():
    """Test OLE date conversion."""
    # January 1, 2012 should be 40909
    dt = datetime(2012, 1, 1)
    ole_date = datetime_to_ole_date(dt)
    assert ole_date == 40909.0


def test_build_simple_daily_simulation():
    """Test building simple daily simulation for full year."""
    config = {
        "horizon_year": 2012,
        "resolution": "1D",
        "models": [
            {
                "name": "Model_2012",
                "category": "model_2012",
                "horizon": {
                    "name": "Horizon_2012",
                    "start": "2012-01-01",
                    "end": "2012-12-31",
                    "chrono_step_type": 2,
                    "chrono_step_count": 366,
                },
            }
        ],
    }
    result = build_plexos_simulation(config)

    assert result.is_ok()
    build_result = result.unwrap()

    assert len(build_result.models) == 1
    assert len(build_result.horizons) == 1
    assert len(build_result.memberships) == 1

    model = build_result.models[0]
    assert model.name == "Model_2012"
    assert model.category == "model_2012"

    horizon = build_result.horizons[0]
    assert horizon.name == "Horizon_2012"
    assert horizon.chrono_step_count == 366  # 2012 is leap year
    assert horizon.chrono_step_type == 2  # Daily

    assert build_result.memberships[0] == ("Model_2012", "Horizon_2012", "Horizon")


def test_build_monthly_template():
    """Test building monthly models from template."""
    config = {"horizon_year": 2012, "template": "monthly"}
    result = build_plexos_simulation(config)

    assert result.is_ok()
    build_result = result.unwrap()

    assert len(build_result.models) == 12
    assert len(build_result.horizons) == 12
    assert len(build_result.memberships) == 12

    # Check January
    jan_model = build_result.models[0]
    assert jan_model.name == "Model_2012_M01"

    jan_horizon = build_result.horizons[0]
    assert jan_horizon.name == "Horizon_2012_M01"
    assert jan_horizon.chrono_step_count == 31  # Days in January

    # Check February (leap year)
    feb_horizon = build_result.horizons[1]
    assert feb_horizon.chrono_step_count == 29  # Days in February 2012


def test_build_monthly_with_overrides():
    """Test monthly template with property overrides."""
    config = {
        "horizon_year": 2012,
        "template": "monthly",
        "model_properties": {"category": "custom_category"},
        "horizon_properties": {"periods_per_day": 48},
    }
    result = build_plexos_simulation(config)

    assert result.is_ok()
    build_result = result.unwrap()

    assert len(build_result.models) == 12

    # Check overrides applied
    model = build_result.models[0]
    assert model.category == "custom_category"

    horizon = build_result.horizons[0]
    assert horizon.periods_per_day == 48


def test_build_weekly_template():
    """Test building weekly models from template."""
    config = {"horizon_year": 2012, "template": "weekly"}
    result = build_plexos_simulation(config)

    assert result.is_ok()
    build_result = result.unwrap()

    assert len(build_result.models) == 52
    assert len(build_result.horizons) == 52

    # Check first week
    week1_horizon = build_result.horizons[0]
    assert week1_horizon.name == "Horizon_2012_W01"
    assert week1_horizon.chrono_step_count == 7


def test_build_quarterly_template():
    """Test building quarterly models from template."""
    config = {"horizon_year": 2012, "template": "quarterly"}
    result = build_plexos_simulation(config)

    assert result.is_ok()
    build_result = result.unwrap()

    assert len(build_result.models) == 4
    assert len(build_result.horizons) == 4

    # Check Q1
    q1_model = build_result.models[0]
    assert q1_model.name == "Model_2012_Q1"

    q1_horizon = build_result.horizons[0]
    assert q1_horizon.name == "Horizon_2012_Q1"
    # Q1 = Jan (31) + Feb (29 in 2012) + Mar (31) = 91 days
    assert q1_horizon.chrono_step_count == 91


def test_build_custom_simulation():
    """Test building fully custom simulation."""
    config = {
        "models": [
            {
                "name": "Summer_Peak",
                "category": "seasonal",
                "horizon": {
                    "name": "Summer_Horizon",
                    "start": "2012-06-01",
                    "end": "2012-08-31",
                    "chrono_step_type": 2,
                    "periods_per_day": 24,
                },
            },
            {
                "name": "Winter_Base",
                "category": "seasonal",
                "horizon": {
                    "start": "2012-12-01",
                    "end": "2012-12-31",
                },
            },
        ]
    }
    result = build_plexos_simulation(config)

    assert result.is_ok()
    build_result = result.unwrap()

    assert len(build_result.models) == 2
    assert len(build_result.horizons) == 2

    # Check first model
    summer_model = build_result.models[0]
    assert summer_model.name == "Summer_Peak"
    assert summer_model.category == "seasonal"

    summer_horizon = build_result.horizons[0]
    assert summer_horizon.name == "Summer_Horizon"
    assert summer_horizon.chrono_step_count == 92  # Jun-Aug: 30+31+31

    # Check second model (auto-generated horizon name)
    winter_horizon = build_result.horizons[1]
    assert winter_horizon.name == "Winter_Base_Horizon"
    assert winter_horizon.chrono_step_count == 31  # December


def test_missing_year_raises_error():
    """Test that missing year returns appropriate error."""
    result = build_plexos_simulation({"resolution": "1D"})
    assert result.is_err()
    assert result.error is not None
    assert "must specify 'horizon_year'" in result.error.lower()


def test_unknown_template_raises_error():
    """Test that unknown template returns appropriate error."""
    result = build_plexos_simulation({"horizon_year": 2012, "template": "unknown"})
    assert result.is_err()
    assert result.error is not None
    assert "Unknown template" in result.error


def test_unsupported_resolution_raises_error():
    """Test that unsupported resolution returns appropriate error."""
    result = build_plexos_simulation({"horizon_year": 2012, "resolution": "1W"})
    assert result.is_ok()
    build_result = result.unwrap()
    assert build_result.models == []
    assert build_result.horizons == []


def test_datetime_to_ole_date_with_time():
    """Test OLE date conversion with time component - line 147-148."""
    dt = datetime(2012, 1, 1, 12, 0, 0)  # Noon
    ole_date = datetime_to_ole_date(dt)
    # Should be 40909.5 (half day)
    assert ole_date == 40909.5


def test_build_simple_hourly_simulation():
    """Test building simple hourly simulation - line 184."""
    config = {"horizon_year": 2012, "resolution": "1H"}
    result = build_plexos_simulation(config)

    assert result.is_ok()
    build_result = result.unwrap()

    assert isinstance(build_result.horizons, list)
    if build_result.horizons:
        horizon = build_result.horizons[0]
        assert horizon.chrono_step_type == 1  # Hourly
        assert horizon.chrono_step_count == 366  # Days in 2012


def test_build_custom_missing_start_date():
    """Test custom simulation missing start date - line 240."""
    config = {
        "models": [
            {
                "name": "TestModel",
                "horizon": {
                    "end": "2012-12-31",
                },
            }
        ]
    }
    result = build_plexos_simulation(config)

    assert result.is_err()
    assert result.error is not None
    assert "missing required 'start' date" in result.error


def test_build_custom_missing_end_date():
    """Test custom simulation missing end date - line 246."""
    config = {
        "models": [
            {
                "name": "TestModel",
                "horizon": {
                    "start": "2012-01-01",
                },
            }
        ]
    }
    result = build_plexos_simulation(config)

    assert result.is_err()
    assert result.error is not None
    assert "missing required 'end' date" in result.error


def test_build_custom_invalid_date_range():
    """Test custom simulation with end before start - line 251."""
    config = {
        "models": [
            {
                "name": "TestModel",
                "horizon": {
                    "start": "2012-12-31",
                    "end": "2012-01-01",  # End before start
                },
            }
        ]
    }
    result = build_plexos_simulation(config)

    assert result.is_err()
    assert result.error is not None
    assert "invalid date range" in result.error


def test_build_custom_with_horizon_overrides():
    """Test custom simulation with horizon config overrides - lines 258-259."""
    config = {
        "models": [
            {
                "name": "CustomModel",
                "horizon": {
                    "start": "2012-01-01",
                    "end": "2012-01-31",
                    "chrono_step_count": 100,  # Override calculated days
                    "chrono_step_type": 1,  # Hourly
                    "step_count": 2,
                    "periods_per_day": 48,
                },
            }
        ]
    }
    result = build_plexos_simulation(config)

    assert result.is_ok()
    build_result = result.unwrap()

    horizon = build_result.horizons[0]
    assert horizon.chrono_step_count == 100  # Override applied
    assert horizon.chrono_step_type == 1
    assert horizon.step_count == 2
    assert horizon.periods_per_day == 48


def test_build_weekly_end_of_year_boundary():
    """Test weekly template handles year boundary correctly - lines 270-271."""
    config = {"horizon_year": 2012, "template": "weekly"}
    result = build_plexos_simulation(config)

    assert result.is_ok()
    build_result = result.unwrap()

    # Last week should not extend beyond the year
    last_horizon = build_result.horizons[-1]
    # The last week might be shorter if it hits year boundary
    assert last_horizon.chrono_step_count <= 7


def test_get_default_simulation_config():
    """Test getting default simulation configuration - lines 412-414."""
    defaults = get_default_simulation_config()

    assert "mt_schedule" in defaults
    assert "st_schedule" in defaults
    assert "production" in defaults
    assert "pasa" in defaults
    assert "performance" in defaults
    assert "report" in defaults
    assert "transmission" in defaults
    assert "diagnostic" in defaults

    # All should be instances of their respective classes
    assert isinstance(defaults["performance"], PLEXOSPerformance)
    assert isinstance(defaults["production"], PLEXOSProduction)
    assert isinstance(defaults["mt_schedule"], PLEXOSMTSchedule)
    assert isinstance(defaults["st_schedule"], PLEXOSSTSchedule)
    assert isinstance(defaults["pasa"], PLEXOSPASA)
    assert isinstance(defaults["report"], PLEXOSReport)
    assert isinstance(defaults["transmission"], PLEXOSTransmission)
    assert isinstance(defaults["diagnostic"], PLEXOSDiagnostic)


def test_convert_simulation_config_to_attributes():
    """Test converting simulation config to attributes - line 448."""
    perf = PLEXOSPerformance(name="TestPerf", solver=4, mip_relative_gap=0.01)
    result = convert_simulation_config_to_attributes(perf)

    assert result.is_ok()
    attrs = result.unwrap()

    # Should have attribute names as keys (using aliases)
    assert "SOLVER" in attrs
    assert attrs["SOLVER"] == 4
    assert "MIP Relative Gap" in attrs
    assert attrs["MIP Relative Gap"] == 0.01

    # Should not include base class fields
    assert "name" not in attrs
    assert "category" not in attrs


def test_convert_simulation_config_skips_none_values():
    """Test that None values are skipped in conversion."""
    perf = PLEXOSPerformance(name="TestPerf", solver=4)
    result = convert_simulation_config_to_attributes(perf)

    assert result.is_ok()
    attrs = result.unwrap()

    # Only non-None values should be included
    assert "SOLVER" in attrs
    # Fields with None values should not be in the dict
    assert len(attrs) > 0


def test_validate_simulation_config_success(tmp_path):
    """Test successful validation of simulation config - line 535."""

    template_path = _XML_TEMPLATE_DIR / FILE_NAME
    db = PlexosDB.from_xml(template_path)

    perf = PLEXOSPerformance(name="TestPerf", solver=4)
    result = validate_simulation_config(db, ClassEnum.Performance, perf)

    assert result.is_ok()


def test_validate_simulation_config_invalid_attribute(tmp_path):
    """Test validation fails with invalid attribute."""

    template_path = _XML_TEMPLATE_DIR / FILE_NAME
    db = PlexosDB.from_xml(template_path)

    # Create a config with an invalid attribute name
    perf = PLEXOSPerformance(name="TestPerf")
    # Manually add an invalid attribute
    perf.model_fields_set.add("invalid_attr")

    # This should still pass because we only validate existing attributes
    result = validate_simulation_config(db, ClassEnum.Performance, perf)
    # It should succeed because we skip None values
    assert result.is_ok()


def test_ingest_simulation_config_success(tmp_path):
    """Test ingesting simulation config to database - lines 655, 657."""

    template_path = _XML_TEMPLATE_DIR / FILE_NAME
    db = PlexosDB.from_xml(template_path)

    perf = PLEXOSPerformance(name="TestPerformance", solver=4, mip_relative_gap=0.01)
    result = ingest_simulation_config_to_plexosdb(db, ClassEnum.Performance, perf)

    assert result.is_ok()
    info = result.unwrap()

    assert info["object_name"] == "TestPerformance"
    assert info["class"] == "Performance"
    assert "SOLVER" in info["attributes_added"]
    assert info["attribute_count"] > 0

    # Verify object was created
    assert db.check_object_exists(ClassEnum.Performance, "TestPerformance")


def test_ingest_simulation_config_without_name():
    """Test ingesting config without name fails - line 664."""
    from unittest.mock import Mock

    template_path = _XML_TEMPLATE_DIR / FILE_NAME
    db = PlexosDB.from_xml(template_path)

    # Create a mock object that bypasses Pydantic validation
    mock_perf = Mock()
    mock_perf.name = None
    mock_perf.model_dump.return_value = {"solver": 4}
    mock_perf.model_fields_set = {"solver"}

    result = ingest_simulation_config_to_plexosdb(db, ClassEnum.Performance, mock_perf)

    assert result.is_err()
    assert result.error is not None
    assert "must have a name" in result.error


def test_ingest_simulation_to_plexosdb_success(tmp_path):
    """Test full simulation ingestion - lines 804, 822, 826-827."""

    template_path = _XML_TEMPLATE_DIR / FILE_NAME
    db = PlexosDB.from_xml(template_path)

    # Build simple simulation with a model
    sim_config = {
        "horizon_year": 2012,
        "resolution": "1D",
        "models": [
            {
                "name": "Model_2012",
                "category": "model_2012",
                "horizon": {
                    "name": "Horizon_2012",
                    "start": "2012-01-01",
                    "end": "2012-12-31",
                    "chrono_step_type": 2,
                    "chrono_step_count": 366,
                },
            }
        ],
    }
    build_result = build_plexos_simulation(sim_config)

    assert build_result.is_ok()
    simulation = build_result.unwrap()

    # Ingest to database
    result = ingest_simulation_to_plexosdb(db, simulation)

    assert result.is_ok()
    info = result.unwrap()

    assert "models" in info
    assert "horizons" in info
    assert len(info["models"]) == 1
    assert len(info["horizons"]) == 1

    # Verify objects exist
    assert db.check_object_exists(ClassEnum.Model, "Model_2012")
    assert db.check_object_exists(ClassEnum.Horizon, "Horizon_2012")


def test_ingest_simulation_with_configs(tmp_path):
    """Test ingestion with simulation configurations - lines 834-838."""

    template_path = _XML_TEMPLATE_DIR / FILE_NAME
    db = PlexosDB.from_xml(template_path)

    # Build simulation with configs
    sim_config = {"horizon_year": 2012, "resolution": "1D"}

    simulation_configs = {
        "performance": PLEXOSPerformance(name="MyPerformance", solver=4),
        "production": PLEXOSProduction(name="MyProduction"),
        "mt_schedule": None,  # Test None handling
        "st_schedule": PLEXOSSTSchedule(name="MySTSchedule"),
    }

    build_result = build_plexos_simulation(sim_config, simulation_config=simulation_configs)

    assert build_result.is_ok()
    simulation = build_result.unwrap()
    assert simulation.simulation_configs is not None

    # Ingest to database
    result = ingest_simulation_to_plexosdb(db, simulation, validate=True)

    assert result.is_ok()
    info = result.unwrap()

    assert "simulation_objects" in info
    # Should have created performance, production, and st_schedule (mt_schedule is None)
    assert len(info["simulation_objects"]) == 3

    # Verify objects exist
    assert db.check_object_exists(ClassEnum.Performance, "MyPerformance")
    assert db.check_object_exists(ClassEnum.Production, "MyProduction")
    assert db.check_object_exists(ClassEnum.STSchedule, "MySTSchedule")


def test_build_simulation_with_simulation_configs():
    """Test building simulation and attaching simulation configs."""
    sim_config = {"horizon_year": 2012, "resolution": "1D"}

    simulation_configs = {
        "performance": PLEXOSPerformance(name="MyPerformance", solver=4),
    }

    result = build_plexos_simulation(sim_config, simulation_config=simulation_configs)

    assert result.is_ok()
    build_result = result.unwrap()

    # Check that simulation_configs is attached
    assert build_result.simulation_configs is not None
    assert "performance" in build_result.simulation_configs
    perf = build_result.simulation_configs["performance"]
    assert perf is not None
    assert perf.name == "MyPerformance"


def test_ingest_with_unknown_config_type(tmp_path):
    """Test ingestion with unknown simulation config type - warning path."""

    template_path = _XML_TEMPLATE_DIR / FILE_NAME
    db = PlexosDB.from_xml(template_path)

    sim_config = {"horizon_year": 2012, "resolution": "1D"}

    # Add unknown config type
    simulation_configs = {
        "unknown_type": PLEXOSPerformance(name="TestPerf", solver=4),
    }

    build_result = build_plexos_simulation(sim_config, simulation_config=simulation_configs)
    simulation = build_result.unwrap()

    # Should log warning but not fail
    result = ingest_simulation_to_plexosdb(db, simulation)

    assert result.is_ok()
    info = result.unwrap()
    # Unknown type should be skipped
    assert len(info["simulation_objects"]) == 0


def test_ingest_simulation_without_configs(tmp_path):
    """Test ingestion without any simulation configs."""

    template_path = _XML_TEMPLATE_DIR / FILE_NAME
    db = PlexosDB.from_xml(template_path)

    sim_config = {"horizon_year": 2012, "resolution": "1D"}
    build_result = build_plexos_simulation(sim_config)
    simulation = build_result.unwrap()

    # simulation_configs should be None
    assert simulation.simulation_configs is None

    result = ingest_simulation_to_plexosdb(db, simulation)

    assert result.is_ok()
    info = result.unwrap()
    assert "simulation_objects" in info
    assert len(info["simulation_objects"]) == 0


# Non-leap year
def test_static_horizon_rewrite_non_leap_year():
    from r2x_plexos.utils_simulation import _rewrite_horizon_attributes_for_weather_year

    attrs = {
        "Chrono Date From": 40940.0,  # Feb 1, 2012
        "Date From": 40909.0,
        "Chrono Step Count": 28.0,
        "Step Count": 365.0,
    }

    result = _rewrite_horizon_attributes_for_weather_year(
        attrs,
        horizon_name="base_2023_m2",
        weather_year=2023,
        is_overlap=False,
    )

    assert result["Step Count"] == 365.0
    assert result["Chrono Step Count"] == 28.0


# Leap year
def test_static_horizon_rewrite_leap_year():
    from r2x_plexos.utils_simulation import _rewrite_horizon_attributes_for_weather_year

    attrs = {
        "Chrono Date From": 40940.0,  # Feb 1, 2012
        "Date From": 40909.0,
        "Chrono Step Count": 28.0,
        "Step Count": 365.0,
    }

    result = _rewrite_horizon_attributes_for_weather_year(
        attrs,
        horizon_name="base_2024_m2",
        weather_year=2024,
        is_overlap=False,
    )

    assert result["Step Count"] == 366.0
    assert result["Chrono Step Count"] == 29.0


# Test full year
def test_static_full_year_horizon_rewrite_leap_year():
    from r2x_plexos.utils_simulation import _rewrite_horizon_attributes_for_weather_year

    attrs = {
        "Chrono Date From": 40909.0,
        "Date From": 40909.0,
        "Chrono Step Count": 364.0,
        "Step Count": 365.0,
    }

    result = _rewrite_horizon_attributes_for_weather_year(
        attrs,
        horizon_name="base_2024",
        weather_year=2024,
        is_overlap=False,
    )

    assert result["Step Count"] == 366.0
    assert result["Chrono Step Count"] == 365.0


# Test overlap
def test_static_full_year_overlap_leap():
    from r2x_plexos.utils_simulation import _rewrite_horizon_attributes_for_weather_year

    attrs = {
        "Chrono Date From": 40909.0,
        "Date From": 40909.0,
        "Chrono Step Count": 362.0,
        "Step Count": 365.0,
    }

    result = _rewrite_horizon_attributes_for_weather_year(
        attrs,
        horizon_name="base_2024_ov",
        weather_year=2024,
        is_overlap=True,
    )

    assert result["Step Count"] == 366.0
    assert result["Chrono Step Count"] == 363.0


def test_replace_year_in_name_rewrites_template_year():
    from r2x_plexos.utils_simulation import _replace_year_in_name

    assert _replace_year_in_name("model_2012", 2024) == "model_2024"
    assert _replace_year_in_name("base_2012_m2", 2024) == "base_2024_m2"


def test_replace_year_in_name_none_weather_year():
    from r2x_plexos.utils_simulation import _replace_year_in_name

    assert _replace_year_in_name("model_2012", None) == "model_2012"


def test_shift_ole_date_to_year_preserves_month_day():
    from datetime import datetime

    from r2x_plexos.utils_simulation import _shift_ole_date_to_year, datetime_to_ole_date

    ole = datetime_to_ole_date(datetime(2012, 2, 1))
    shifted = _shift_ole_date_to_year(ole, 2024)

    assert shifted == datetime_to_ole_date(datetime(2024, 2, 1))


def test_shift_ole_date_to_year_handles_feb_29_to_non_leap():
    from datetime import datetime

    from r2x_plexos.utils_simulation import _shift_ole_date_to_year, datetime_to_ole_date

    ole = datetime_to_ole_date(datetime(2012, 2, 29))
    shifted = _shift_ole_date_to_year(ole, 2023)

    assert shifted == datetime_to_ole_date(datetime(2023, 2, 28))


def test_build_static_simulation_rewrites_names_for_weather_year():
    from r2x_plexos import PLEXOSConfig

    static_model_defaults = PLEXOSConfig.load_static_models()
    static_horizon_defaults = PLEXOSConfig.load_static_horizons()
    defaults = {**static_model_defaults, **static_horizon_defaults}

    result = build_plexos_simulation(
        {"horizon_year": 2024},
        defaults=defaults,
    )

    assert result.is_ok()
    build_result = result.unwrap()

    model_names = {m.name for m in build_result.models}
    horizon_names = {h.name for h in build_result.horizons}
    memberships = set(build_result.memberships)

    assert "model_2024" in model_names
    assert "base_2024" in horizon_names

    assert ("model_2024", "base_2024", "Horizon") in memberships

    assert "model_2012" not in model_names
    assert "base_2012" not in horizon_names


def test_ingest_simulation_to_plexosdb_adds_model_attributes(tmp_path):
    """Test that model attributes are written to the DB during simulation ingestion."""
    from r2x_plexos.models import PLEXOSHorizon, PLEXOSModel
    from r2x_plexos.utils_simulation import SimulationConfig

    template_path = _XML_TEMPLATE_DIR / FILE_NAME
    db = PlexosDB.from_xml(template_path)

    model = PLEXOSModel(
        name="Model_2012",
        category="model_2012",
        **{"Random Number Seed": 2718},  # ty: ignore[invalid-argument-type]
    )

    horizon = PLEXOSHorizon(
        name="Horizon_2012",
        chrono_date_from=40909.0,
        date_from=40909.0,
        chrono_step_count=366,
        chrono_step_type=2,
        step_count=1,
        periods_per_day=24,
    )

    simulation = SimulationConfig(
        models=[model],
        horizons=[horizon],
        memberships=[("Model_2012", "Horizon_2012", "Horizon")],
        simulation_configs=None,
    )

    result = ingest_simulation_to_plexosdb(db, simulation)

    assert result.is_ok()
    assert db.check_object_exists(ClassEnum.Model, "Model_2012")

    attr_value = db.get_attribute(
        ClassEnum.Model,
        object_name="Model_2012",
        attribute_name="Random Number Seed",
    )

    assert attr_value is not None
    assert attr_value[0] == 2718.0


def test_shift_ole_date_to_year_non_leap_non_feb29_unchanged():
    """Test normal dates (not Feb 29) shift cleanly without clamping."""
    from r2x_plexos.utils_simulation import _shift_ole_date_to_year, datetime_to_ole_date

    ole = datetime_to_ole_date(datetime(2012, 6, 15))
    shifted = _shift_ole_date_to_year(ole, 2023)
    assert shifted == datetime_to_ole_date(datetime(2023, 6, 15))


def test_ingest_static_simulation_sets_base_diagnose_defaults(tmp_path):
    """Ensure base_diagnose has default diagnostic checkboxes enabled."""

    template_path = _XML_TEMPLATE_DIR / FILE_NAME
    db = PlexosDB.from_xml(template_path)

    static_model_defaults = PLEXOSConfig.load_static_models()
    static_horizon_defaults = PLEXOSConfig.load_static_horizons()
    defaults = {**static_model_defaults, **static_horizon_defaults}

    build_result = build_plexos_simulation({"horizon_year": 2024}, defaults=defaults)
    assert build_result.is_ok()

    ingest_result = ingest_simulation_to_plexosdb(db, build_result.unwrap())
    assert ingest_result.is_ok()

    requested_attr_candidates = [
        ("Database Load",),
        ("Summary Each Step", "Step Summary"),
        ("Execution Times", "Times"),
        ("Task Size",),
        ("LP Solver Progress", "LP Progress"),
        ("Computer Information",),
        ("MIP Solver Progress", "MIP Progress"),
    ]

    assert db.check_object_exists(ClassEnum.Diagnostic, "base_diagnose")
    # The template/version may use alternate names for some requested options.
    # Verify we set defaults for each requested option via any supported candidate name.
    for candidates in requested_attr_candidates:
        resolved_name = None
        for candidate in candidates:
            if validate_simulation_attribute(db, ClassEnum.Diagnostic, candidate).is_ok():
                resolved_name = candidate
                break

        if resolved_name is None:
            continue

        values = db.get_attribute(
            ClassEnum.Diagnostic,
            object_name="base_diagnose",
            attribute_name=resolved_name,
        )
        assert values is not None
        assert values[0] == -1


def test_ingest_static_simulation_sets_base_transmission_defaults(tmp_path):
    """Ensure base_transmission has requested Transmission defaults enabled."""
    from r2x_plexos import PLEXOSConfig

    template_path = _XML_TEMPLATE_DIR / FILE_NAME
    db = PlexosDB.from_xml(template_path)

    static_model_defaults = PLEXOSConfig.load_static_models()
    static_horizon_defaults = PLEXOSConfig.load_static_horizons()
    defaults = {**static_model_defaults, **static_horizon_defaults}

    build_result = build_plexos_simulation({"horizon_year": 2024}, defaults=defaults)
    assert build_result.is_ok()

    ingest_result = ingest_simulation_to_plexosdb(db, build_result.unwrap())
    assert ingest_result.is_ok()

    assert db.check_object_exists(ClassEnum.Transmission, "base_transmission")

    expected_values = {
        "OF Method": 1,
        "Formulate Upfront": -1,
        "Constraints Enabled": -1,
        "Interface Constraints Enabled": -1,
        "Enforce Limits On Lines In Interfaces": -1,
        "Losses Enabled": -1,
    }

    # Set and verify only attributes that are supported by the active schema/template.
    for attr_name, expected in expected_values.items():
        if validate_simulation_attribute(db, ClassEnum.Transmission, attr_name).is_err():
            continue

        values = db.get_attribute(
            ClassEnum.Transmission,
            object_name="base_transmission",
            attribute_name=attr_name,
        )
        assert values is not None
        assert values[0] == expected


def test_ingest_static_simulation_sets_base_performance_mip_gap_to_one_percent(tmp_path):
    """Ensure base_performance uses 1% MIP relative gap by default."""
    from r2x_plexos import PLEXOSConfig

    template_path = _XML_TEMPLATE_DIR / FILE_NAME
    db = PlexosDB.from_xml(template_path)

    static_model_defaults = PLEXOSConfig.load_static_models()
    static_horizon_defaults = PLEXOSConfig.load_static_horizons()
    defaults = {**static_model_defaults, **static_horizon_defaults}

    build_result = build_plexos_simulation({"horizon_year": 2024}, defaults=defaults)
    assert build_result.is_ok()

    ingest_result = ingest_simulation_to_plexosdb(db, build_result.unwrap())
    assert ingest_result.is_ok()

    assert db.check_object_exists(ClassEnum.Performance, "base_performance")
    values = db.get_attribute(
        ClassEnum.Performance,
        object_name="base_performance",
        attribute_name="MIP Relative Gap",
    )
    assert values is not None
    assert values[0] == 0.01


def test_ingest_static_simulation_overwrites_existing_performance_mip_gap(tmp_path):
    """Ensure an existing non-1% MIP gap is forced to 0.01 for base_performance."""
    from r2x_plexos import PLEXOSConfig

    template_path = _XML_TEMPLATE_DIR / FILE_NAME
    db = PlexosDB.from_xml(template_path)

    if not db.check_object_exists(ClassEnum.Performance, "base_performance"):
        db.add_object(ClassEnum.Performance, "base_performance")

    if validate_simulation_attribute(db, ClassEnum.Performance, "MIP Relative Gap").is_ok():
        object_id = db.get_object_id(ClassEnum.Performance, "base_performance")
        attribute_id = db.get_attribute_id(ClassEnum.Performance, name="MIP Relative Gap")
        updated = db._db.execute(
            """
            UPDATE t_attribute_data
            SET value = ?
            WHERE object_id = ? AND attribute_id = ?
            """,
            (0.0001, object_id, attribute_id),
        )
        if getattr(updated, "rowcount", 0) == 0:
            db.add_attribute(
                ClassEnum.Performance,
                "base_performance",
                attribute_name="MIP Relative Gap",
                attribute_value=0.0001,
            )

    static_model_defaults = PLEXOSConfig.load_static_models()
    static_horizon_defaults = PLEXOSConfig.load_static_horizons()
    defaults = {**static_model_defaults, **static_horizon_defaults}

    build_result = build_plexos_simulation({"horizon_year": 2024}, defaults=defaults)
    assert build_result.is_ok()

    ingest_result = ingest_simulation_to_plexosdb(db, build_result.unwrap())
    assert ingest_result.is_ok()

    values = db.get_attribute(
        ClassEnum.Performance,
        object_name="base_performance",
        attribute_name="MIP Relative Gap",
    )
    assert values is not None
    assert values[0] == 0.01


def test_ingest_static_simulation_overwrites_base_mip_to_integer_uc(tmp_path):
    """Ensure base MIP Production uses Integer unit commitment optimality."""
    from r2x_plexos import PLEXOSConfig

    template_path = _XML_TEMPLATE_DIR / FILE_NAME
    db = PlexosDB.from_xml(template_path)

    if not db.check_object_exists(ClassEnum.Production, "base MIP"):
        db.add_object(ClassEnum.Production, "base MIP")

    if validate_simulation_attribute(db, ClassEnum.Production, "Unit Commitment Optimality").is_ok():
        object_id = db.get_object_id(ClassEnum.Production, "base MIP")
        attribute_id = db.get_attribute_id(ClassEnum.Production, name="Unit Commitment Optimality")
        updated = db._db.execute(
            """
            UPDATE t_attribute_data
            SET value = ?
            WHERE object_id = ? AND attribute_id = ?
            """,
            (0, object_id, attribute_id),
        )
        if getattr(updated, "rowcount", 0) == 0:
            db.add_attribute(
                ClassEnum.Production,
                "base MIP",
                attribute_name="Unit Commitment Optimality",
                attribute_value=0,
            )

    static_model_defaults = PLEXOSConfig.load_static_models()
    static_horizon_defaults = PLEXOSConfig.load_static_horizons()
    defaults = {**static_model_defaults, **static_horizon_defaults}

    build_result = build_plexos_simulation({"horizon_year": 2024}, defaults=defaults)
    assert build_result.is_ok()

    ingest_result = ingest_simulation_to_plexosdb(db, build_result.unwrap())
    assert ingest_result.is_ok()

    values = db.get_attribute(
        ClassEnum.Production,
        object_name="base MIP",
        attribute_name="Unit Commitment Optimality",
    )
    assert values is not None
    assert values[0] == 2


def test_ingest_simulation_sets_production_example_to_integer_uc(tmp_path):
    """Ensure Production_Example uses Integer unit commitment optimality."""
    from r2x_plexos import PLEXOSConfig

    template_path = _XML_TEMPLATE_DIR / FILE_NAME
    db = PlexosDB.from_xml(template_path)

    simulation_config = get_default_simulation_config()
    # Set to Linear initially to confirm ingest forces it to Integer.
    simulation_config["production"].unit_commitment_optimality = 0

    static_model_defaults = PLEXOSConfig.load_static_models()
    static_horizon_defaults = PLEXOSConfig.load_static_horizons()
    defaults = {**static_model_defaults, **static_horizon_defaults}

    build_result = build_plexos_simulation(
        {"horizon_year": 2024},
        defaults=defaults,
        simulation_config=simulation_config,
    )
    assert build_result.is_ok()

    ingest_result = ingest_simulation_to_plexosdb(db, build_result.unwrap())
    assert ingest_result.is_ok()

    values = db.get_attribute(
        ClassEnum.Production,
        object_name="Production_Example",
        attribute_name="Unit Commitment Optimality",
    )
    assert values is not None
    assert values[0] == 2


def test_build_static_simulation_keeps_d1_horizon_one_day():
    """Ensure base_<year>_d1 horizon remains a one-day chrono test horizon."""
    from r2x_plexos import PLEXOSConfig

    static_model_defaults = PLEXOSConfig.load_static_models()
    static_horizon_defaults = PLEXOSConfig.load_static_horizons()
    defaults = {**static_model_defaults, **static_horizon_defaults}

    result = build_plexos_simulation(
        {"horizon_year": 2023},
        defaults=defaults,
    )
    assert result.is_ok()

    build_result = result.unwrap()
    d1 = next((h for h in build_result.horizons if h.name == "base_2023_d1"), None)

    assert d1 is not None
    assert d1.chrono_step_count == 1.0


def test_ingest_static_simulation_sets_base_st_deterministic(tmp_path):
    """Ensure base_st ST Schedule runs in deterministic mode."""
    from r2x_plexos import PLEXOSConfig

    template_path = _XML_TEMPLATE_DIR / FILE_NAME
    db = PlexosDB.from_xml(template_path)

    if not db.check_object_exists(ClassEnum.STSchedule, "base_st"):
        db.add_object(ClassEnum.STSchedule, "base_st")

    if validate_simulation_attribute(db, ClassEnum.STSchedule, "Stochastic Method").is_ok():
        object_id = db.get_object_id(ClassEnum.STSchedule, "base_st")
        attribute_id = db.get_attribute_id(ClassEnum.STSchedule, name="Stochastic Method")
        updated = db._db.execute(
            """
            UPDATE t_attribute_data
            SET value = ?
            WHERE object_id = ? AND attribute_id = ?
            """,
            (2, object_id, attribute_id),
        )
        if getattr(updated, "rowcount", 0) == 0:
            db.add_attribute(
                ClassEnum.STSchedule,
                "base_st",
                attribute_name="Stochastic Method",
                attribute_value=2,
            )

    static_model_defaults = PLEXOSConfig.load_static_models()
    static_horizon_defaults = PLEXOSConfig.load_static_horizons()
    defaults = {**static_model_defaults, **static_horizon_defaults}

    build_result = build_plexos_simulation({"horizon_year": 2024}, defaults=defaults)
    assert build_result.is_ok()

    ingest_result = ingest_simulation_to_plexosdb(db, build_result.unwrap())
    assert ingest_result.is_ok()

    values = db.get_attribute(
        ClassEnum.STSchedule,
        object_name="base_st",
        attribute_name="Stochastic Method",
    )
    assert values is not None
    assert values[0] == 0


def test_get_enum_from_string_raises_for_no_match():
    """No close enum member should raise KeyError."""
    with pytest.raises(KeyError):
        get_enum_from_string("zzzzzzzzzz", ClassEnum)


def test_convert_simulation_config_to_attributes_returns_err_on_bad_object():
    """Conversion should return Err when model fields cannot be read."""

    class FieldInfo:
        alias = "BROKEN"

    class BrokenConfig:
        model_fields: typing.ClassVar[dict] = {"broken": FieldInfo()}

        def __getattr__(self, _name):
            raise RuntimeError("boom")

    result = convert_simulation_config_to_attributes(BrokenConfig())
    assert result.is_err()
