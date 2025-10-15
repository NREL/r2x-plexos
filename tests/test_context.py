"""Tests for scenario priority context."""

import pytest

from r2x_plexos.models.context import (
    get_scenario_priority,
    scenario_priority,
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
