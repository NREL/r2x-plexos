"""Tests for the PLEXOS purchaser model."""

import pytest
from plexosdb.enums import ClassEnum
from pydantic import ValidationError

from r2x_plexos.models import PLEXOSPurchaser
from r2x_plexos.utils_mappings import PLEXOS_TYPE_MAP


def test_purchaser_defaults():
    """Test purchaser defaults and enum-like flags."""
    purchaser = PLEXOSPurchaser(name="p1", object_id=1)

    assert purchaser.benefit_function_shape == 1
    assert purchaser.bid_price == -10000
    assert purchaser.max_energy == 1e30
    assert purchaser.min_energy == 0
    assert purchaser.units == 1


def test_purchaser_validation_rules():
    """Test purchaser validation constraints from model specification."""
    with pytest.raises(ValidationError):
        PLEXOSPurchaser(name="p1", object_id=1, max_load_factor=101)

    with pytest.raises(ValidationError):
        PLEXOSPurchaser(name="p1", object_id=1, bid_quantity=-1)

    with pytest.raises(ValidationError):
        PLEXOSPurchaser(name="p1", object_id=1, load_settlement_source=2)


def test_purchaser_class_type_mapping():
    """Test purchaser is mapped to ClassEnum.Purchaser."""
    assert PLEXOS_TYPE_MAP[ClassEnum.Purchaser] is PLEXOSPurchaser
