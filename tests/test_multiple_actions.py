"""Tests for multiple actions on a single property."""

from r2x_plexos import PLEXOSPropertyValue


def test_multiple_actions_same_property():
    """Test that a property can have multiple actions (e.g., = and +)."""
    prop = PLEXOSPropertyValue()

    prop.add_entry(value=100, scenario="Base", action="=", units="MW")
    prop.add_entry(value=20, scenario="Base", action="+", units="MW")
    assert len(prop.entries) == 2

    keys_list = list(prop.entries.keys())
    actions = [key.action for key in keys_list]
    assert "=" in actions
    assert "+" in actions


def test_multiple_actions_different_scenarios():
    """Test multiple actions across different scenarios."""
    prop = PLEXOSPropertyValue()

    prop.add_entry(value=100, scenario="Base", action="=", units="MW")
    prop.add_entry(value=150, scenario="High", action="=", units="MW")
    prop.add_entry(value=10, scenario="Base", action="+", units="MW")

    assert len(prop.entries) == 3


def test_action_is_part_of_key():
    """Verify that action is part of the lookup key."""
    prop = PLEXOSPropertyValue()

    prop.add_entry(value=50, action="=")
    prop.add_entry(value=50, action="+")

    assert len(prop.entries) == 2

    keys = list(prop.entries.keys())
    assert keys[0].action != keys[1].action


def test_complex_multi_action_property():
    """Test a realistic scenario with multiple actions and dimensions."""
    prop = PLEXOSPropertyValue()

    prop.add_entry(value=100, band=1, action="=", units="MW")
    prop.add_entry(value=20, band=1, action="+", units="MW")
    prop.add_entry(value=1.1, band=1, action="*")
    prop.add_entry(value=50, band=2, action="=", units="MW")
    assert len(prop.entries) == 4

    band1_keys = [key for key in prop.entries if key.band == 1]
    assert len(band1_keys) == 3

    band1_actions = {key.action for key in band1_keys}
    assert band1_actions == {"=", "+", "*"}


def test_action_with_timeslices():
    """Test actions work correctly with timesliced data."""
    prop = PLEXOSPropertyValue()

    prop.add_entry(value=100, timeslice="Peak", action="=", units="MW")
    prop.add_entry(value=80, timeslice="OffPeak", action="=", units="MW")
    prop.add_entry(value=10, timeslice="Peak", action="+", units="MW")
    prop.add_entry(value=5, timeslice="OffPeak", action="+", units="MW")

    # Should have 4 entries (2 timeslices x 2 actions)
    assert len(prop.entries) == 4

    assert set(prop.get_timeslices()) == {"Peak", "OffPeak"}
