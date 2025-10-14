"""PLEXOS component base class with automatic property resolution."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from r2x_plexos.models.property import PLEXOSPropertyValue


class PLEXOSComponent(BaseModel):
    """Base class for PLEXOS components with automatic property resolution.

    When accessing a field that contains a PropertyValue, automatically
    calls get_value() to resolve using global scenario priority context.
    """

    def __getattribute__(self, name: str) -> Any:
        """Override attribute access to auto-resolve PropertyValue fields."""
        value = super().__getattribute__(name)

        # Check if this is a model field and contains a PropertyValue
        # Access model_fields from the class using object.__getattribute__ to avoid recursion
        cls = object.__getattribute__(self, "__class__")
        if name in cls.model_fields and isinstance(value, PLEXOSPropertyValue):
            return value.get_value()

        return value
