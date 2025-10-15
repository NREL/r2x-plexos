"""PLEXOS property value class."""

from collections.abc import Callable
from dataclasses import dataclass, field
from functools import total_ordering
from typing import Any

from .base import PLEXOSPropertyKey, PLEXOSRow
from .context import get_horizon, get_scenario_priority


@total_ordering
@dataclass(slots=True)
class PLEXOSPropertyValue:
    """Optimized property value class for PLEXOS components.

    Uses a hash-based dictionary for O(1) lookups with pre-built indexes
    for filtering by dimension (scenario, band, timeslice, dates).
    Designed to handle millions of property values efficiently.
    """

    entries: dict[PLEXOSPropertyKey, PLEXOSRow] = field(default_factory=dict)

    units: str | None = None
    action: str | None = None

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
            prop.add_entry(
                value=record.get("value"),
                scenario=record.get("scenario"),
                band=record.get("band", 1),
                timeslice=record.get("timeslice") or record.get("time_slice"),
                date_from=record.get("date_from"),
                date_to=record.get("date_to"),
                variable_name=record.get("variable_name") or record.get("variable"),
                text=record.get("text"),
                action=record.get("action"),
                units=record.get("units") or units,
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
        self._update_metadata(units, action)

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
            self._update_metadata(row.units, row.action)

    def _update_metadata(self, units: str | None, action: str | None) -> None:
        """Update property-level metadata if not already set."""
        if not self.units and units:
            self.units = units
        if not self.action and action:
            self.action = action

    def get_value(self) -> Any:
        """Get property value with automatic scenario priority and horizon resolution.

        Resolution order:
        1. Filter by horizon (date range) if set
        2. If priority context is set, use priority-based resolution
        3. Pure default entry (no scenario/timeslice) takes precedence
        4. Non-scenario timeslices preferred over scenarios
        5. Non-scenario bands preferred over scenarios
        6. Return scenario/timeslice/band dicts or simple values as appropriate
        """
        if not self.entries:
            return None

        # Filter by horizon if set
        horizon = get_horizon()
        if horizon:
            filtered_entries = self._filter_by_horizon(horizon)
            if not filtered_entries:
                return None

            # Temporarily swap entries and rebuild indexes
            original_entries = self.entries
            original_indexes = self._save_indexes()
            try:
                self.entries = filtered_entries
                self._rebuild_indexes()
                return self._resolve_value()
            finally:
                # Restore original entries and indexes
                self.entries = original_entries
                self._restore_indexes(original_indexes)
        else:
            return self._resolve_value()

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

        if date_from or date_to:
            key = PLEXOSPropertyKey(scenario=scenario, band=band, timeslice=timeslice)
            if key in self.entries:
                return self.entries[key].value

        if scenario and scenario in self._by_scenario:
            scenario_keys = sorted(
                self._by_scenario[scenario], key=lambda k: (k.timeslice or "", k.band or 1)
            )
            if scenario_keys:
                return self.entries[scenario_keys[0]].value

        if timeslice and timeslice in self._by_timeslice:
            timeslice_keys = sorted(
                self._by_timeslice[timeslice], key=lambda k: (k.band or 1, k.scenario or "")
            )
            if timeslice_keys:
                return self.entries[timeslice_keys[0]].value

        if scenario:
            key = PLEXOSPropertyKey(band=band, timeslice=timeslice)
            if key in self.entries:
                return self.entries[key].value

        if timeslice:
            key = PLEXOSPropertyKey(scenario=scenario, band=band)
            if key in self.entries:
                return self.entries[key].value

        key = PLEXOSPropertyKey(band=band)
        if key in self.entries:
            return self.entries[key].value

        # Check if we have entries for this band (with dates or other dimensions)
        if band in self._by_band:
            band_keys = sorted(
                self._by_band[band], key=lambda k: (k.scenario or "", k.timeslice or "", k.date_from or "")
            )
            if band_keys:
                return self.entries[band_keys[0]].value

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

    def __repr__(self) -> str:
        """Return a readable string representation."""
        parts = []

        parts.append(f"entries={len(self.entries)}")

        if self.units:
            parts.append(f"units={self.units!r}")

        if self.action and self.action != "=":
            parts.append(f"action={self.action!r}")

        scenarios = self.get_scenarios()
        if scenarios:
            parts.append(f"scenarios={scenarios}")

        timeslices = self.get_timeslices()
        if timeslices:
            parts.append(f"timeslices={timeslices}")

        bands = self.get_bands()
        if len(bands) > 1:  # Only show if multi-band
            parts.append(f"bands={bands}")

        if self.has_date_from() or self.has_date_to():
            parts.append("has_dates=True")

        if self.has_datafile():
            parts.append("has_datafile=True")
        if self.has_variable():
            parts.append("has_variable=True")

        if self.entries:
            sample_values = list(self.entries.values())[:3]
            values_str = ", ".join(str(row.value) for row in sample_values)
            if len(self.entries) > 3:
                values_str += ", ..."
            parts.append(f"values=[{values_str}]")

        return f"PLEXOSPropertyValue({', '.join(parts)})"

    def __lt__(self, other: Any) -> bool:
        """Less than comparison."""
        return self._compare(other, lambda x, y: x < y)

    def __eq__(self, other: Any) -> bool:
        """Equal comparison."""
        return self._compare(other, lambda x, y: x == y)

    def _compare(self, other: Any, op: Callable[[Any, Any], bool]) -> bool:
        """Compare this property with another value."""
        if not self.entries or (self.has_datafile() or self.has_variable()):
            return True

        values = [row.value for row in self.entries.values()]
        return all(v is not None and op(v, other) for v in values)

    def _add_to_indexes(self, key: PLEXOSPropertyKey) -> None:
        """Add a key to all relevant indexes."""

        def add_to_index(index: dict[Any, set[PLEXOSPropertyKey]], index_key: Any) -> None:
            """Help adding to index."""
            if index_key not in index:
                index[index_key] = set()
            index[index_key].add(key)

        if key.scenario:
            add_to_index(self._by_scenario, key.scenario)

        add_to_index(self._by_band, key.band)

        if key.timeslice:
            add_to_index(self._by_timeslice, key.timeslice)

        date_key = (key.date_from, key.date_to)
        if date_key != (None, None):
            add_to_index(self._by_date, date_key)

        if key.variable:
            add_to_index(self._by_variable, key.variable)

        if key.text:
            add_to_index(self._by_text, key.text)

    def _get_non_scenario_timeslices(self) -> set[str]:
        """Get timeslices from entries without scenarios."""
        return {key.timeslice for key in self.entries if key.scenario is None and key.timeslice is not None}

    def _get_non_scenario_bands(self) -> set[PLEXOSPropertyKey]:
        """Get keys for entries without scenarios but with non-default bands."""
        return {key for key in self.entries if key.scenario is None and key.band != 1}

    def _resolve_scenarios(self, scenarios: list[str], bands: list[int]) -> Any:
        """Resolve value when scenarios are present."""
        if len(scenarios) == 1 and len(self.entries) == len(self._by_scenario[scenarios[0]]):
            if len(bands) > 1:
                return {scenarios[0]: self.get_value_for(scenario=scenarios[0])}
            return self.get_value_for(scenario=scenarios[0])

        return {scenario: self.get_value_for(scenario=scenario) for scenario in scenarios}

    def _resolve_timeslices(self, timeslices: list[str]) -> Any:
        """Resolve value when timeslices are present but no scenarios."""
        if len(timeslices) == 1:
            return self.get_value_for(timeslice=timeslices[0])
        return {ts: self.get_value_for(timeslice=ts) for ts in timeslices}

    def _rebuild_indexes(self) -> None:
        """Rebuild all indexes from entries."""
        for index in (
            self._by_scenario,
            self._by_band,
            self._by_timeslice,
            self._by_date,
            self._by_variable,
            self._by_text,
        ):
            index.clear()

        for key in self.entries:
            self._add_to_indexes(key)

    def _resolve_by_priority(self, priority: dict[str, int]) -> Any:
        """Resolve value using scenario priority (lower number = higher priority)."""
        candidates: list[tuple[str | None, Any, float]] = []

        # Look for entries, preferring simple values (band 1, no timeslice, no dates)
        # but also considering entries with timeslices/dates if no simple values exist
        simple_candidates: list[tuple[str | None, Any, float]] = []
        complex_candidates: list[tuple[str | None, Any, float]] = []

        for key, row in self.entries.items():
            if key.scenario is None:
                prio = float("inf")
            elif key.scenario in priority:
                prio = float(priority[key.scenario])
            else:
                prio = float("inf") - 1

            # Categorize as simple or complex
            if key.band == 1 and key.timeslice is None and key.date_from is None and key.date_to is None:
                simple_candidates.append((key.scenario, row.value, prio))
            else:
                complex_candidates.append((key.scenario, row.value, prio))

        # Prefer simple candidates if they exist, otherwise use complex
        candidates = simple_candidates if simple_candidates else complex_candidates

        if not candidates:
            return self.get_value_for()

        candidates.sort(key=lambda x: x[2])
        return candidates[0][1]

    def _save_indexes(self) -> dict[str, Any]:
        """Save current indexes for restoration."""
        return {
            name: getattr(self, name).copy()
            for name in ("_by_scenario", "_by_band", "_by_timeslice", "_by_date", "_by_variable", "_by_text")
        }

    def _restore_indexes(self, saved_indexes: dict[str, Any]) -> None:
        """Restore previously saved indexes."""
        for name, index in saved_indexes.items():
            setattr(self, name, index)

    def _resolve_value(self) -> Any:
        """Resolve property value based on current entries (main resolution logic)."""
        priority = get_scenario_priority()
        if priority:
            return self._resolve_by_priority(priority)

        default_key = PLEXOSPropertyKey(scenario=None, band=1, timeslice=None)
        has_pure_default = default_key in self.entries
        scenarios = self.get_scenarios()
        timeslices = self.get_timeslices()
        bands = self.get_bands()

        non_scenario_timeslices = self._get_non_scenario_timeslices()
        non_scenario_bands = self._get_non_scenario_bands()

        if has_pure_default and len(self.entries) > 1 and (scenarios or timeslices):
            return self.entries[default_key].value

        if scenarios and non_scenario_timeslices:
            return {ts: self.get_value_for(timeslice=ts) for ts in sorted(non_scenario_timeslices)}

        if scenarios and non_scenario_bands:
            return self.get_value_for(band=1)

        if scenarios:
            return self._resolve_scenarios(scenarios, bands)

        if timeslices:
            return self._resolve_timeslices(timeslices)

        if self.has_bands():
            return {band: self.get_value_for(band=band) for band in bands}

        return self.get_value_for()

    def _filter_by_horizon(self, horizon: tuple[str, str]) -> dict[PLEXOSPropertyKey, PLEXOSRow]:
        """Filter entries by horizon (date range).

        Includes entries that:
        - Have no dates (apply to all periods)
        - Have dates that overlap with the horizon
        """
        horizon_from, horizon_to = horizon
        filtered = {}

        for key, entry in self.entries.items():
            # Include entries without dates (apply to all periods)
            if key.date_from is None and key.date_to is None:
                filtered[key] = entry
            # Check for date overlap if entry has dates
            elif key.date_from is not None or key.date_to is not None:
                entry_from = key.date_from or "0000-00-00"
                entry_to = key.date_to or "9999-99-99"
                if entry_from <= horizon_to and entry_to >= horizon_from:
                    filtered[key] = entry

        return filtered


PropertyType = PLEXOSPropertyValue
