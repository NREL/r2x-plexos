"""Compatibility helpers for enum members not present in newer plexosdb releases."""

from typing import cast

from plexosdb.enums import ClassEnum, CollectionEnum


def _set_missing_attr(cls: type, name: str, value: str) -> str:
    """Attach a missing enum-like attribute for backward compatibility."""
    existing = getattr(cls, name, None)
    if existing is not None:
        return cast(str, existing)

    # Enum classes block normal assignment for members; use type.__setattr__.
    type.__setattr__(cls, name, value)
    return value


PURCHASER_CLASS_ENUM = _set_missing_attr(ClassEnum, "Purchaser", "Purchaser")
PURCHASER_COLLECTION_ENUM = _set_missing_attr(CollectionEnum, "Purchasers", "Purchasers")
