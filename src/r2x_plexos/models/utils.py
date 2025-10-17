"""Utilities for the models."""

from pydantic import BaseModel


def get_field_name_by_alias(model: BaseModel, alias_name: str) -> str | None:
    """Return the Pydantic field name corresponding to a given alias."""
    # Access model_fields from the class, not the instance
    for field_name, field_info in model.__class__.model_fields.items():
        if field_info.alias == alias_name:
            return field_name
    return None
