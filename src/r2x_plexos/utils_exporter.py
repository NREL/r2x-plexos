"""Utility functions for PLEXOS exporter."""

from typing import Any


def get_component_category(component: Any) -> str | None:
    """Get the category of a component if it has one.

    Parameters
    ----------
    component : Any
        The component to get the category from

    Returns
    -------
    str | None
        The category name if the component has a category attribute, None otherwise
    """
    return component.category if hasattr(component, "category") else None
