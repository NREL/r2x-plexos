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
    is_validator: bool = False

    def _validate_value(self, value: Any, info: core_schema.ValidationInfo) -> float | Any:
        """Validate and convert input to float or PlexosProperty.

        Parameters
        ----------
        value : float, int, dict, or PlexosProperty
            Input value to validate
        info : core_schema.ValidationInfo
            Pydantic validation context

        Returns
        -------
        float or PlexosProperty
            Validated and converted value

        Raises
        ------
        ValueError
            If value format is invalid or constraints violated
        TypeError
            If value type is not supported
        """
        if isinstance(value, int | float):
            return float(value)

        if isinstance(value, PLEXOSPropertyValue):
            if self.is_validator and not self.allow_bands and len(value.get_bands()) > 1:
                raise ValueError(
                    f"Multi-band properties not allowed. Property has {len(value.get_bands())} bands."
                )

            if self.units and not value.units:
                value.units = self.units

            return value

        if isinstance(value, dict):
            if self.units and "units" not in value:
                value = {**value, "units": self.units}

            prop = PLEXOSPropertyValue.from_dict(value)

            if self.is_validator and not self.allow_bands and len(prop.get_bands()) > 1:
                raise ValueError("Multi-band properties not allowed for this field")

            return prop

        raise TypeError(f"Expected float, int, dict, or PlexosProperty, got {type(value).__name__}")

    def __get_pydantic_core_schema__(
        self, source_type: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        """Register validator with Pydantic v2.

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

        return core_schema.with_info_after_validator_function(
            self._validate_value,
            core_schema.union_schema(
                [
                    core_schema.float_schema(),
                    core_schema.int_schema(),
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
    return PropertySpecification(units=units, allow_bands=allow_bands, is_validator=True)


PLEXOSProperty = _property_spec
