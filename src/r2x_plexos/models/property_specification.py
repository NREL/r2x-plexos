"""Property specification and annotation types for PLEXOS properties."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema

from .property import PLEXOSPropertyValue

if TYPE_CHECKING:
    from pydantic import GetJsonSchemaHandler
    from pydantic.json_schema import JsonSchemaValue


@dataclass(frozen=True)
class PropertySpecification:
    """Metadata descriptor for PLEXOS property fields.

    Similar to r2x-core's UnitSpec, this injects Pydantic validation logic
    into the validation pipeline.

    Attributes
    ----------
    units : str, optional
        Units for this property (e.g., "MW", "%", "hours")
    allow_bands : bool, default True
        Whether to allow multi-band properties. Some PLEXOS properties
        cannot be multi-banded (e.g., forced outage rate).
    is_validator : bool, default False
        Whether this spec performs validation (True) or just adds metadata (False)
    """

    units: str | None = None
    allow_bands: bool = True
    is_enum: bool = False
    is_validator: bool = False

    def _validate_enum_value(self, val: float | int) -> None:
        """Validate that a numeric value is a whole number for enum fields."""
        if isinstance(val, float) and not val.is_integer():
            raise ValueError(f"Enum field requires whole number, got {val}")

    def _validate_bands(self, prop: PLEXOSPropertyValue) -> None:
        """Validate band constraints if enabled."""
        if not self.allow_bands and len(prop.get_bands()) > 1:
            raise ValueError(
                f"Multi-band properties not allowed. Property has {len(prop.get_bands())} bands."
            )

    def _apply_units(self, value: dict[str, Any] | PLEXOSPropertyValue) -> None:
        """Apply default units if not already set."""
        if not self.units:
            return

        if isinstance(value, dict) and "units" not in value:
            value["units"] = self.units
        elif isinstance(value, PLEXOSPropertyValue) and not value.units:
            value.units = self.units

    def _validate_numeric(self, value: int | float) -> int | float:
        """Validate simple numeric values."""
        if self.is_enum:
            self._validate_enum_value(value)
        return value

    def _validate_property(self, value: PLEXOSPropertyValue) -> PLEXOSPropertyValue:
        """Validate PLEXOSPropertyValue objects."""
        if self.is_validator:
            self._validate_bands(value)

        self._apply_units(value)

        if self.is_enum:
            for row in value.entries.values():
                if row.value is not None:
                    self._validate_enum_value(row.value)

        return value

    def _validate_dict(self, value: dict[str, Any]) -> PLEXOSPropertyValue:
        """Validate and convert dictionary to PLEXOSPropertyValue."""
        self._apply_units(value)

        if self.is_enum and "value" in value and value["value"] is not None:
            self._validate_enum_value(value["value"])

        prop = PLEXOSPropertyValue.from_dict(value)

        if self.is_validator:
            self._validate_bands(prop)

        return prop

    def _validate_value(self, value: Any, info: core_schema.ValidationInfo) -> Any:
        """Validate and convert input to float or PLEXOSPropertyValue.

        Parameters
        ----------
        value : float, int, dict, or PLEXOSPropertyValue, or None
            Input value to validate
        info : core_schema.ValidationInfo
            Pydantic validation context

        Returns
        -------
        float, int, PLEXOSPropertyValue, or None
            Validated and converted value

        Raises
        ------
        ValueError
            If value format is invalid or constraints violated
        TypeError
            If value type is not supported
        """
        # Allow None values (for optional fields)
        if value is None:
            return None

        if isinstance(value, int | float):
            return self._validate_numeric(value)

        if isinstance(value, PLEXOSPropertyValue):
            return self._validate_property(value)

        if isinstance(value, dict):
            return self._validate_dict(value)

        raise TypeError(
            f"Expected float, int, dict, PLEXOSPropertyValue, or None, got {type(value).__name__}"
        )

    def __get_pydantic_core_schema__(
        self, source_type: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        """Register validator with Pydantic v2.

        This schema allows Field constraints (like le=100) to be applied only to
        numeric values, not to PLEXOSPropertyValue objects.

        Parameters
        ----------
        source_type : Any
            Source type being annotated
        handler : GetCoreSchemaHandler
            Pydantic schema handler

        Returns
        -------
        core_schema.CoreSchema
            Pydantic core schema for validation
        """
        from r2x_plexos.models.property import PLEXOSPropertyValue

        # Get the base schema from the source type (includes Field constraints)
        python_schema = handler(source_type)

        # Create our custom union schema that accepts multiple types
        return core_schema.with_info_after_validator_function(
            self._validate_value,
            core_schema.union_schema(
                [
                    core_schema.none_schema(),  # Allow None values
                    python_schema,  # Use source schema for numeric values (includes Field constraints)
                    core_schema.dict_schema(),
                    core_schema.is_instance_schema(PLEXOSPropertyValue),
                ]
            ),
        )

    @classmethod
    def __get_pydantic_json_schema__(
        cls,
        _core_schema: core_schema.CoreSchema,
        handler: GetJsonSchemaHandler,
    ) -> JsonSchemaValue:
        """Generate JSON schema representation.

        Parameters
        ----------
        _core_schema : core_schema.CoreSchema
            Pydantic core schema
        handler : GetJsonSchemaHandler
            JSON schema handler

        Returns
        -------
        JsonSchemaValue
            JSON schema representing this as number or object
        """
        return {"oneOf": [{"type": "number"}, {"type": "object"}]}


def _property_spec(
    units: str | None = None,
    allow_bands: bool = True,
    is_enum: bool = False,
) -> PropertySpecification:
    """Create a PropertySpec for field annotation.

    Parameters
    ----------
    units : str, optional
        Units for this property (e.g., "MW", "kV", "%")
    allow_bands : bool, default True
        Whether to allow multi-band properties. Some PLEXOS properties
        cannot have multiple bands.

    Returns
    -------
    PropertySpec
        Property specification instance for use in Annotated[]

    Examples
    --------
    >>> from typing import Annotated
    >>> from pydantic import BaseModel
    >>>
    >>> class Generator(BaseModel):
    ...     max_capacity: Annotated[float, PlexosProperty(units="MW")]
    ...     outage_rate: Annotated[float, PlexosProperty(units="%", allow_bands=False)]
    """
    return PropertySpecification(units=units, allow_bands=allow_bands, is_validator=True, is_enum=is_enum)


class _PLEXOSPropertyFactory:
    """Factory that can be used as both a callable and a default instance.

    Allows both PLEXOSProperty() and PLEXOSProperty to work.
    """

    _default_instance: PropertySpecification | None = None

    def __call__(
        self,
        units: str | None = None,
        allow_bands: bool = True,
        is_enum: bool = False,
    ) -> PropertySpecification:
        """Create a PropertySpec for field annotation."""
        return _property_spec(units=units, allow_bands=allow_bands, is_enum=is_enum)

    def __get_pydantic_core_schema__(
        self, source_type: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        """When used without calling, create a default PropertySpecification."""
        if self._default_instance is None:
            self._default_instance = _property_spec()
        return self._default_instance.__get_pydantic_core_schema__(source_type, handler)


PLEXOSProperty = _PLEXOSPropertyFactory()
