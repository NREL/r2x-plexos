"""Global context for scenario priority and horizon resolution."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

_current_scenario_priority: dict[str, int] | None = None
_current_horizon: tuple[str, str] | None = None  # (date_from, date_to)


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


def get_horizon() -> tuple[str, str] | None:
    """Get the current global horizon (date range).

    Returns
    -------
    tuple[str, str] or None
        Current horizon as (date_from, date_to), or None if no horizon is set.
    """
    return _current_horizon


def set_horizon(horizon: tuple[str, str] | None) -> None:
    """Set the global horizon (date range).

    Parameters
    ----------
    horizon : tuple[str, str] or None
        Horizon to set as (date_from, date_to), or None to clear horizon.
    """
    global _current_horizon
    _current_horizon = horizon


@contextmanager
def scenario_priority(priority: dict[str, int] | None) -> Iterator[None]:
    """Context manager for temporary scenario priority changes.

    Parameters
    ----------
    priority : dict[str, int] or None
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


@contextmanager
def horizon(date_from: str, date_to: str) -> Iterator[None]:
    """Context manager for temporary horizon (date range) changes.

    This filters property values to only include entries that overlap with
    the specified date range.

    Parameters
    ----------
    date_from : str
        Start date of the horizon (e.g., "2024-01-01")
    date_to : str
        End date of the horizon (e.g., "2024-12-31")

    Yields
    ------
    None

    Examples
    --------
    >>> from r2x_plexos.models.context import horizon
    >>> gen = parser.get_generator("Coal1")
    >>> with horizon("2024-01-01", "2024-06-30"):
    ...     print(gen.max_capacity)
    100.0
    """
    previous = get_horizon()
    set_horizon((date_from, date_to))
    try:
        yield
    finally:
        set_horizon(previous)


@contextmanager
def scenario_and_horizon(priority: dict[str, int] | None, date_from: str, date_to: str) -> Iterator[None]:
    """Context manager for both scenario priority and horizon.

    Combines scenario priority and horizon filtering for complete
    temporal and scenario-based resolution.

    Parameters
    ----------
    priority : dict[str, int] or None
        Scenario priority mapping (lower number = higher priority)
    date_from : str
        Start date of the horizon
    date_to : str
        End date of the horizon

    Yields
    ------
    None

    Examples
    --------
    >>> from r2x_plexos.models.context import scenario_and_horizon
    >>> gen = parser.get_generator("Coal1")
    >>> with scenario_and_horizon({"Test": 1}, "2024-01-01", "2024-12-31"):
    ...     print(gen.max_capacity)
    120.0
    """
    with scenario_priority(priority), horizon(date_from, date_to):
        yield
