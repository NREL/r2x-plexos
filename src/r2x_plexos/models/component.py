"""PLEXOS component base class with automatic property resolution."""

from __future__ import annotations

import logging
from typing import Any, ClassVar

from infrasys import Component

from r2x_plexos.models.property import PLEXOSPropertyValue

logger = logging.getLogger(__name__)


class PLEXOSObject(Component):
    """Base class for PLEXOS components with automatic property resolution.

    When accessing a field that contains a PropertyValue, automatically
    calls get_value() to resolve using global scenario priority context.
    """

    model_config: ClassVar = {"protected_namespaces": ()}
    category: str | None = None
    object_id: int | None = None

    def __getattribute__(self, name: str) -> Any:
        """Override attribute access to auto-resolve PropertyValue fields.

        Automatically calls get_value() on PLEXOSPropertyValue fields to resolve
        using global scenario priority context. Warns when auto-resolution may
        hide complex data (filepath, datafile references, variable references).
        """
        # For Pydantic metadata attributes, access from class to avoid deprecation warnings
        if name in ("model_fields", "model_computed_fields"):
            return getattr(type(self), name)

        value = super().__getattribute__(name)

        # Check if this is a model field and contains a PropertyValue
        # Access model_fields from the class (not instance) to avoid deprecation warning
        cls = type(self)
        if name in cls.model_fields and isinstance(value, PLEXOSPropertyValue):
            # Check if auto-resolution may hide complex data
            if value.has_complex_data():
                resolved = value.get_value()

                # Build warning message showing what's being hidden
                hidden_data = []
                if value.get_filepath():
                    hidden_data.append(f"filepath='{value.get_filepath()}'")
                df_ref = value.get_datafile_reference()
                if df_ref:
                    hidden_data.append(f"datafile='{df_ref['name']}'")
                var_ref = value.get_variable_reference()
                if var_ref:
                    hidden_data.append(f"variable='{var_ref['name']}'")
                if value.has_scenarios():
                    scenarios = value.get_scenarios()
                    hidden_data.append(f"scenarios={list(scenarios)}")
                if len(value.entries) > 1:
                    hidden_data.append(f"{len(value.entries)} entries (bands/timeslices/dates)")

                if hidden_data:
                    logger.warning(
                        f"Accessing {cls.__name__}.{name} returns value={resolved!r}, "
                        f"but property has complex data: {', '.join(hidden_data)}. "
                        f"Use get_property_value('{name}') to access full PropertyValue object."
                    )

            return value.get_value()

        return value

    def get_property_value(self, field_name: str) -> PLEXOSPropertyValue | Any:
        """Get the raw PropertyValue object for a field without auto-resolution.

        This method bypasses the automatic get_value() resolution that occurs
        when accessing fields directly. Use this to access the full PropertyValue
        object with all its complex data (filepath, datafile refs, variable refs, etc.).

        Parameters
        ----------
        field_name : str
            Name of the field to retrieve

        Returns
        -------
        PLEXOSPropertyValue | Any
            The raw field value (PropertyValue if field contains one, otherwise raw value)

        Examples
        --------
        >>> datafile.filename  # Returns resolved value (e.g., 0.0)
        >>> datafile.get_property_value('filename')  # Returns PLEXOSPropertyValue object
        >>> datafile.get_property_value('filename').get_filepath()  # Returns filepath string
        """
        return super().__getattribute__(field_name)

    # PLEXOS has a lot of defaults. Displaying them is a nightmare
    def __repr__(self) -> str:
        """PlexosComponent representation."""
        fields = []
        # Access model_fields from class to avoid deprecation warning
        for name, field_info in type(self).model_fields.items():
            value = super().__getattribute__(name)  # Use super to avoid auto-resolution
            if value is not None and value != field_info.default:
                fields.append(f"{name}={value!r}")
        return f"{self.__class__.__name__}({', '.join(fields)})"


class PLEXOSTopology(PLEXOSObject):
    """Abstract type to filter topological elements on PLEXOS."""


class PLEXOSConfiguration(PLEXOSObject):
    """Abstract type to filter topological elements on PLEXOS."""
