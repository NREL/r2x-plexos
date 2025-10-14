"""PLEXOS property value class."""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar

from .base import PLEXOSPropertyKey, PLEXOSRow

T = TypeVar("T")


@dataclass(slots=True)
class PLEXOSPropertyValue:
    """Optimized property value class for PLEXOS components.

    Uses a hash-based dictionary for O(1) lookups with pre-built indexes
    for filtering by dimension (scenario, band, timeslice, dates).
    Designed to handle millions of property values efficiently.
    """

    entries: dict[PLEXOSPropertyKey, PLEXOSRow] = field(default_factory=dict)

    # Common metadata (most properties share the same units/action)
    units: str | None = None
    action: str | None = None

    # Pre-built lookup indexes for fast filtering (private attributes)
    _by_scenario: dict[str, set[PLEXOSPropertyKey]] = field(default_factory=dict)
    _by_band: dict[int, set[PLEXOSPropertyKey]] = field(default_factory=dict)
    _by_timeslice: dict[str, set[PLEXOSPropertyKey]] = field(default_factory=dict)
    _by_date: dict[tuple[str | None, str | None], set[PLEXOSPropertyKey]] = field(default_factory=dict)
    _by_variable: dict[str, set[PLEXOSPropertyKey]] = field(default_factory=dict)
    _by_text: dict[str, set[PLEXOSPropertyKey]] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Any) -> "PLEXOSPropertyValue":
        """Create property from a dictionary specification."""
        prop = cls(units=data.get("units"))
        prop.add_entry(
            value=data.get("value"),
            scenario=data.get("scenario"),
            band=data.get("band", 1),
            timeslice=data.get("timeslice"),
            date_from=data.get("date_from"),
            date_to=data.get("date_to"),
            datafile_name=data.get("datafile_name") or data.get("datafile"),
            datafile_id=data.get("datafile_id"),
            column_name=data.get("column_name") or data.get("column"),
            variable_name=data.get("variable_name") or data.get("variable"),
            variable_id=data.get("variable_id"),
            action=data.get("action"),
            text=data.get("text"),
            units=data.get("units"),
        )
        return prop

    @classmethod
    def from_db_results(cls, results: list[PLEXOSRow]) -> "PLEXOSPropertyValue":
        """Create a property from database results."""
        assert results is not None
        instance = cls()
        instance.add_from_db_rows(results)
        return instance

    @classmethod
    def from_records(cls, records: list[dict[str, Any]], units: str | None = None) -> "PLEXOSPropertyValue":
        """Create property from a list of record dictionaries."""
        prop = cls(units=units)
        for record in records:
            if units and "units" not in record:
                record = {**record, "units": units}

            value = record.get("value")

            prop.add_entry(
                value=value,
                scenario=record.get("scenario"),
                band=record.get("band", 1),
                timeslice=record.get("timeslice"),
                date_from=record.get("date_from"),
                date_to=record.get("date_to"),
                variable_name=record.get("variable_name"),
                text=record.get("text"),
                action=record.get("action"),
                units=record.get("units"),
            )
        return prop

    def add_entry(
        self,
        value: Any,
        scenario: str | None = None,
        band: int = 1,
        timeslice: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        period_type_id: int | None = None,
        datafile_name: str | None = None,
        datafile_id: int | None = None,
        column_name: str | None = None,
        variable_name: str | None = None,
        variable_id: int | None = None,
        action: str | None = None,
        units: str | None = None,
        text: str | None = None,
    ) -> None:
        """Add a property value entry with full metadata."""
        key = PLEXOSPropertyKey(
            scenario=scenario,
            band=band,
            timeslice=timeslice,
            date_from=date_from,
            date_to=date_to,
            period_type_id=period_type_id,
            action=action,
            variable=variable_name,
            text=text,
        )

        # Create PLEXOSRow to store value and metadata
        row = PLEXOSRow(
            value=value,
            scenario_name=scenario,
            band=band,
            timeslice_name=timeslice,
            date_from=date_from,
            date_to=date_to,
            datafile_name=datafile_name,
            datafile_id=datafile_id,
            column_name=column_name,
            variable_name=variable_name,
            variable_id=variable_id,
            action=action,
            units=units,
            text=text,
        )

        self.entries[key] = row
        self._add_to_indexes(key)

        # Update property-level metadata
        if not self.units and units:
            self.units = units
        if not self.action and action:
            self.action = action

    def add_from_db_rows(self, rows: PLEXOSRow | list[PLEXOSRow]) -> None:
        """Add multiple database results - stores PLEXOSRow directly."""
        rows = rows if isinstance(rows, list) else [rows]
        for row in rows:
            key = PLEXOSPropertyKey(
                scenario=row.scenario_name,
                band=row.band,
                timeslice=row.timeslice_name,
                date_from=row.date_from,
                date_to=row.date_to,
                action=row.action,
                variable=row.variable_name,
                text=row.text,
            )

            self.entries[key] = row
            self._add_to_indexes(key)

            if not self.units and row.units:
                self.units = row.units
            if not self.action and row.action:
                self.action = row.action

    def get_value(self) -> Any:
        """Get property value with automatic scenario priority resolution.

        Uses global scenario priority context if available.
        Returns all scenario values as dict if no priority set.
        """
        from r2x_plexos.models.context import get_scenario_priority

        if not self.entries:
            return None

        priority = get_scenario_priority()
        if priority:
            return self._resolve_by_priority(priority)

        scenarios = self.get_scenarios()
        if scenarios:
            if len(scenarios) == 1:
                return self.get_value_for(scenario=scenarios[0])
            return {scenario: self.get_value_for(scenario=scenario) for scenario in scenarios}

        return self.get_value_for()

    def get_value_for(
        self,
        scenario: str | None = None,
        band: int = 1,
        timeslice: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> Any:
        """Get value for specific dimensions with fallback logic."""
        key = PLEXOSPropertyKey(
            scenario=scenario,
            band=band,
            timeslice=timeslice,
            date_from=date_from,
            date_to=date_to,
        )
        if key in self.entries:
            return self.entries[key].value

        # Fallback 1: Try without dates
        if date_from or date_to:
            key = PLEXOSPropertyKey(scenario=scenario, band=band, timeslice=timeslice)
            if key in self.entries:
                return self.entries[key].value

        # Fallback 2: Try without scenario
        if scenario:
            key = PLEXOSPropertyKey(band=band, timeslice=timeslice)
            if key in self.entries:
                return self.entries[key].value

        # Fallback 3: Try without timeslice
        if timeslice:
            key = PLEXOSPropertyKey(scenario=scenario, band=band)
            if key in self.entries:
                return self.entries[key].value

        # Fallback 4: Try band only
        key = PLEXOSPropertyKey(band=band)
        if key in self.entries:
            return self.entries[key].value

        # Fallback 5: Return first entry value
        if self.entries:
            return next(iter(self.entries.values())).value

        return None

    def get_bands(self) -> list[int]:
        """Get all unique bands."""
        return sorted(self._by_band.keys())

    def get_dates(self) -> list[tuple[str | None, str | None]]:
        """Get all unique date ranges."""
        return sorted(self._by_date.keys())

    def get_timeslices(self) -> list[str]:
        """Get all unique timeslices."""
        return sorted(self._by_timeslice.keys())

    def get_scenarios(self) -> list[str]:
        """Get all unique scenarios."""
        return sorted(self._by_scenario.keys())

    def get_text(self) -> list[str]:
        """Get all unique text values."""
        return sorted(self._by_text.keys())

    def get_variables(self) -> list[str]:
        """Get all unique variables."""
        return sorted(self._by_variable.keys())

    def has_bands(self) -> bool:
        """Check if this property has multiple bands."""
        return len(self._by_band) > 1

    def has_date_from(self) -> bool:
        """Check if this property has date_from constraints."""
        return any(key.date_from is not None for key in self.entries)

    def has_date_to(self) -> bool:
        """Check if this property has date_to constraints."""
        return any(key.date_to is not None for key in self.entries)

    def has_scenarios(self) -> bool:
        """Check if this property has scenario-specific values."""
        return bool(self._by_scenario)

    def has_timeslices(self) -> bool:
        """Check if this property has timesliced data."""
        return bool(self._by_timeslice)

    def has_datafile(self) -> bool:
        """Check if this property references a datafile."""
        return any(row.datafile_name or row.datafile_id for row in self.entries.values())

    def has_variable(self) -> bool:
        """Check if this property references a variable."""
        return any(row.variable_name or row.variable_id for row in self.entries.values())

    def has_text(self) -> bool:
        """Check if this property has text values."""
        return bool(self._by_text)

    def __lt__(self, other: Any) -> bool:
        """Less than comparison."""
        return self._compare(other, lambda x, y: x < y)

    def __le__(self, other: Any) -> bool:
        """Less than or equal comparison."""
        return self._compare(other, lambda x, y: x <= y)

    def __gt__(self, other: Any) -> bool:
        """Greater than comparison."""
        return self._compare(other, lambda x, y: x > y)

    def __ge__(self, other: Any) -> bool:
        """Greater than or equal comparison."""
        return self._compare(other, lambda x, y: x >= y)

    def __eq__(self, other: Any) -> bool:
        """Equal comparison."""
        return self._compare(other, lambda x, y: x == y)

    def _compare(self, other: Any, op: Callable[[Any, Any], bool]) -> bool:
        """Compare this property with another value."""
        if (self.has_datafile() or self.has_variable()) and not self.entries:
            return True

        if not self.entries:
            return True

        values = [row.value for row in self.entries.values()]
        return all(v is not None and op(v, other) for v in values)

    def _add_to_indexes(self, key: PLEXOSPropertyKey) -> None:
        """Add a key to all relevant indexes."""
        if key.scenario:
            if key.scenario not in self._by_scenario:
                self._by_scenario[key.scenario] = set()
            self._by_scenario[key.scenario].add(key)

        if key.band not in self._by_band:
            self._by_band[key.band] = set()
            self._by_band[key.band].add(key)
        if key.timeslice:
            if key.timeslice not in self._by_timeslice:
                self._by_timeslice[key.timeslice] = set()
            self._by_timeslice[key.timeslice].add(key)

        date_key = (key.date_from, key.date_to)
        if date_key != (None, None):
            if date_key not in self._by_date:
                self._by_date[date_key] = set()
            self._by_date[date_key].add(key)

        if key.variable:
            if key.variable not in self._by_variable:
                self._by_variable[key.variable] = set()
            self._by_variable[key.variable].add(key)

        if key.text:
            if key.text not in self._by_text:
                self._by_text[key.text] = set()
            self._by_text[key.text].add(key)

    def _rebuild_indexes(self) -> None:
        """Rebuild all indexes from entries."""
        self._by_scenario.clear()
        self._by_band.clear()
        self._by_timeslice.clear()
        self._by_date.clear()
        self._by_variable.clear()
        self._by_text.clear()

        for key in self.entries:
            self._add_to_indexes(key)

    def _resolve_by_priority(self, priority: dict[str, int]) -> Any:
        """Resolve value using scenario priority (lower number = higher priority)."""
        candidates: list[tuple[str | None, Any, float]] = []

        # Look for simple values (band 1, no timeslice, no dates)
        for key, row in self.entries.items():
            if key.band == 1 and key.timeslice is None and key.date_from is None and key.date_to is None:
                if key.scenario is None:
                    prio = float("inf")
                elif key.scenario in priority:
                    prio = float(priority[key.scenario])
                else:
                    prio = float("inf") - 1

                candidates.append((key.scenario, row.value, prio))

        if not candidates:
            return self.get_value_for()

        candidates.sort(key=lambda x: x[2])
        return candidates[0][1]


PropertyType = PLEXOSPropertyValue
