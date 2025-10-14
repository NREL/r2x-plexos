"""PLEXOS component base class with automatic property resolution."""

from __future__ import annotations

from typing import Any

from infrasys import Component

from r2x_plexos.models.property import PLEXOSPropertyValue


class PLEXOSComponent(Component):
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

    # PLEXOS has a lot of defaults. Displaying them is a nightmare
    def __repr__(self) -> str:
        """PlexosComponent representation."""
        fields = []
        for name, field_info in self.model_fields.items():
            value = getattr(self, name)
            if value is not None and value != field_info.default:
                fields.append(f"{name}={value!r}")
        return f"{self.__class__.__name__}({', '.join(fields)})"
