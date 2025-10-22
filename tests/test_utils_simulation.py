"""Tests for simulation configuration builder utilities."""

from datetime import datetime

from r2x_plexos.utils_simulation import (
    build_plexos_simulation,
    datetime_to_ole_date,
)


def test_datetime_to_ole_date():
    """Test OLE date conversion."""
    # January 1, 2012 should be 40909
    dt = datetime(2012, 1, 1)
    ole_date = datetime_to_ole_date(dt)
    assert ole_date == 40909.0


def test_build_simple_daily_simulation():
    """Test building simple daily simulation for full year."""
    config = {"horizon_year": 2012, "resolution": "1D"}
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

    assert build_result.memberships[0] == ("Model_2012", "Horizon_2012")


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
    assert "must specify 'horizon_year'" in result.error


def test_unknown_template_raises_error():
    """Test that unknown template returns appropriate error."""
    result = build_plexos_simulation({"horizon_year": 2012, "template": "unknown"})
    assert result.is_err()
    assert "Unknown template" in result.error


def test_unsupported_resolution_raises_error():
    """Test that unsupported resolution returns appropriate error."""
    result = build_plexos_simulation({"horizon_year": 2012, "resolution": "1W"})
    assert result.is_err()
    assert "Unsupported resolution" in result.error
