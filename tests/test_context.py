"""Tests for scenario priority context."""

import pytest

from r2x_plexos.models.context import (
    get_horizon,
    get_scenario_priority,
    horizon,
    scenario_and_horizon,
    scenario_priority,
    set_horizon,
    set_scenario_priority,
)


def test_default_priority_is_none():
    assert get_scenario_priority() is None


def test_set_priority():
    original = get_scenario_priority()
    set_scenario_priority({"Base": 1, "High": 2})
    assert get_scenario_priority() == {"Base": 1, "High": 2}
    set_scenario_priority(original)


def test_context_manager():
    original = get_scenario_priority()
    assert original is None

    with scenario_priority({"Base": 1, "High": 2}):
        assert get_scenario_priority() == {"Base": 1, "High": 2}

    assert get_scenario_priority() is None


def test_context_manager_nested():
    with scenario_priority({"Base": 1}):
        assert get_scenario_priority() == {"Base": 1}

        with scenario_priority({"High": 1, "Base": 2}):
            assert get_scenario_priority() == {"High": 1, "Base": 2}

        assert get_scenario_priority() == {"Base": 1}


def test_context_manager_restores_on_exception():
    with pytest.raises(ValueError), scenario_priority({"Base": 1}):
        assert get_scenario_priority() == {"Base": 1}
        raise ValueError("test")

    assert get_scenario_priority() is None


def test_default_horizon_is_none():
    assert get_horizon() is None


def test_set_horizon():
    set_horizon(("2024-01-01", "2024-12-31"))
    assert get_horizon() == ("2024-01-01", "2024-12-31")
    set_horizon(None)
    assert get_horizon() is None


def test_horizon_context_manager():
    assert get_horizon() is None

    with horizon("2024-01-01", "2024-06-30"):
        assert get_horizon() == ("2024-01-01", "2024-06-30")

    assert get_horizon() is None


def test_horizon_context_manager_nested():
    with horizon("2024-01-01", "2024-12-31"):
        assert get_horizon() == ("2024-01-01", "2024-12-31")

        with horizon("2024-06-01", "2024-06-30"):
            assert get_horizon() == ("2024-06-01", "2024-06-30")

        assert get_horizon() == ("2024-01-01", "2024-12-31")


def test_horizon_context_manager_restores_on_exception():
    with pytest.raises(RuntimeError), horizon("2024-01-01", "2024-12-31"):
        assert get_horizon() == ("2024-01-01", "2024-12-31")
        raise RuntimeError("test")

    assert get_horizon() is None


def test_scenario_and_horizon_sets_both():
    assert get_scenario_priority() is None
    assert get_horizon() is None

    with scenario_and_horizon({"Base": 1}, "2024-01-01", "2024-12-31"):
        assert get_scenario_priority() == {"Base": 1}
        assert get_horizon() == ("2024-01-01", "2024-12-31")

    assert get_scenario_priority() is None
    assert get_horizon() is None


def test_scenario_and_horizon_restores_on_exception():
    with pytest.raises(ValueError), scenario_and_horizon({"Base": 1}, "2024-01-01", "2024-12-31"):
        raise ValueError("test")

    assert get_scenario_priority() is None
    assert get_horizon() is None
