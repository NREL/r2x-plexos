"""Global context for scenario priority resolution."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

_current_scenario_priority: dict[str, int] | None = None


def get_scenario_priority() -> dict[str, int] | None:
    """Get the current global scenario priority.

    Returns
    -------
    dict[str, int] or None
        Current scenario priority mapping (scenario name -> priority value),
        or None if no priority is set.
    """
    return _current_scenario_priority


def set_scenario_priority(priority: dict[str, int] | None) -> None:
    """Set the global scenario priority.

    Parameters
    ----------
    priority : dict[str, int] or None
        Scenario priority mapping to set (lower number = higher priority),
        or None to clear priority.
    """
    global _current_scenario_priority
    _current_scenario_priority = priority


@contextmanager
def scenario_priority(priority: dict[str, int]) -> Iterator[None]:
    """Context manager for temporary scenario priority changes.

    Parameters
    ----------
    priority : dict[str, int]
        Temporary scenario priority to use within the context
        (lower number = higher priority)

    Yields
    ------
    None

    Examples
    --------
    >>> from r2x_plexos.models.context import scenario_priority
    >>> gen = parser.get_generator("Coal1")
    >>> with scenario_priority({"Test": 1, "Base": 2}):
    ...     print(gen.max_capacity)
    120.0
    >>> print(gen.max_capacity)
    100.0
    """
    previous = get_scenario_priority()
    set_scenario_priority(priority)
    try:
        yield
    finally:
        set_scenario_priority(previous)
