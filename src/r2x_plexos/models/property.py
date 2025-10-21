"""PLEXOS property value class."""

from collections.abc import Callable
from dataclasses import dataclass, field
from functools import total_ordering
from typing import Any, cast

from plexosdb import ClassEnum

from .base import PLEXOSPropertyKey, PLEXOSRow
from .context import get_horizon, get_scenario_priority

# Constants
DEFAULT_BAND = 1
MAX_REPR_VALUES = 5
PRIORITY_NO_SCENARIO = 0  # Entries with no scenario (base case) have priority 0
PRIORITY_UNKNOWN_SCENARIO = -1  # Unknown scenarios have lowest priority


@total_ordering
@dataclass(slots=True)
class PLEXOSPropertyValue:
    """Optimized property value class for PLEXOS components.

    Uses a hash-based dictionary for O(1) lookups with pre-built indexes
    for filtering by dimension (scenario, band, timeslice, dates).
    Designed to handle millions of property values efficiently.

    Serialization
    -------------
    Supports Pydantic serialization for compatibility with infrasys System.to_json().
    When used as a field in Pydantic models, serializes to a list of records
    compatible with plexosdb_from_records:

    ```python
    [
        {
            "value": 10,
            "scenario_name": None,
            "band": 1,
            "timeslice_name": None,
            "date_from": None,
            "date_to": None,
            "datafile_name": None,
            "datafile_id": None,
            "column_name": None,
            "variable_name": None,
            "variable_id": None,
            "action": "=",
            "units": None,
            "text": None,
            "text_class_name": None
        },
        ...
    ]
    ```

    Round-trip serialization is supported via Pydantic's model_dump(mode='json')
    when used as a field type, and reconstruction from serialized format.
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
            band=data.get("band", DEFAULT_BAND),
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
            # Some datasets place CSV filenames in generic text/value fields.
            possible_text = record.get("text") or record.get("value")
            csv_in_text = None
            if isinstance(possible_text, str) and possible_text.lower().endswith(".csv"):
                csv_in_text = possible_text
            prop.add_entry(
                value=record.get("value"),
                scenario=record.get("scenario_name") or record.get("scenario"),
                band=record.get("band", DEFAULT_BAND),
                timeslice=record.get("timeslice_name") or record.get("timeslice") or record.get("time_slice"),
                date_from=record.get("date_from"),
                date_to=record.get("date_to"),
                # Preserve datafile metadata so downstream time series resolution works
                datafile_name=(
                    record.get("datafile_name")
                    or record.get("datafile")
                    or record.get("filename")
                    or csv_in_text
                ),
                datafile_id=record.get("datafile_id"),
                column_name=record.get("column_name") or record.get("column"),
                # Variable metadata
                variable_name=record.get("variable_name") or record.get("variable"),
                variable_id=record.get("variable_id"),
                text=record.get("text"),
                text_class_name=record.get("text_class_name"),  # Capture type of text reference
                action=record.get("action"),
                units=record.get("units") or units,
            )
        return prop

    def add_entry(
        self,
        value: Any,
        scenario: str | None = None,
        band: int = DEFAULT_BAND,
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
        text_class_name: str | None = None,
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
            text_class_name=text_class_name,
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

        horizon = get_horizon()
        if horizon:
            filtered_entries = self._filter_by_horizon(horizon)
            if not filtered_entries:
                return None

            original_entries = self.entries
            original_indexes = self._save_indexes()
            try:
                self.entries = filtered_entries
                self._rebuild_indexes()
                return self._resolve_value()
            finally:
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

    def get_filepath(self) -> str | None:
        """Get filepath if this property references a Data File via text field.

        Returns the filepath string from the text field when text_class_name='Data File'.
        Useful for properties like DataFile.Filename.

        Returns
        -------
        str | None
            Filepath string or None if no filepath reference exists
        """
        for entry in self.entries.values():
            if (
                entry.text
                and hasattr(entry, "text_class_name")
                and entry.text_class_name == ClassEnum.DataFile
            ):
                return entry.text
        return None

    def get_variable_reference(self) -> dict[str, Any] | None:
        """Get variable reference if this property uses a Variable.

        Returns dictionary with variable metadata including name, id, and action.

        Returns
        -------
        dict | None
            Dictionary with keys: 'name', 'id', 'action' or None if no variable reference
        """
        for entry in self.entries.values():
            if entry.variable_name:
                return {"name": entry.variable_name, "id": entry.variable_id, "action": entry.action}
        return None

    def get_datafile_reference(self) -> dict[str, Any] | None:
        """Get datafile reference if this property uses another DataFile object.

        Returns dictionary with datafile metadata including name and id.
        Different from get_filepath() which returns a direct filepath string.

        Returns
        -------
        dict | None
            Dictionary with keys: 'name', 'id' or None if no datafile reference
        """
        for entry in self.entries.values():
            if entry.datafile_name:
                return {"name": entry.datafile_name, "id": entry.datafile_id}
        return None

    def get_text_value(self) -> str | None:
        """Get single text value if property has exactly one text entry.

        Convenience method for properties with a single text value.
        For multiple texts, use get_text() instead.

        Returns
        -------
        str | None
            Text string if exactly one text exists, None otherwise
        """
        texts = self.get_text()
        return texts[0] if len(texts) == 1 else None

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
        return any(
            row.datafile_name
            or row.datafile_id
            or (isinstance(row.text, str) and row.text.lower().endswith(".csv"))
            for row in self.entries.values()
        )

    def has_variable(self) -> bool:
        """Check if this property references a variable."""
        return any(row.variable_name or row.variable_id for row in self.entries.values())

    def has_complex_data(self) -> bool:
        """Check if property has complex data beyond a simple value.

        Complex data includes:
        - Datafile or variable references
        - Multiple scenarios
        - Multiple entries (bands, timeslices, dates)

        Returns
        -------
        bool
            True if property has complex data structure
        """
        return self.has_datafile() or self.has_variable() or self.has_scenarios() or len(self.entries) > 1

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
            sample_values = list(self.entries.values())[:MAX_REPR_VALUES]
            values_str = ", ".join(str(row.value) for row in sample_values)
            if len(self.entries) > MAX_REPR_VALUES:
                values_str += ", ..."
            parts.append(f"values=[{values_str}]")

        return f"PLEXOSPropertyValue({', '.join(parts)})"

    def __lt__(self, other: Any) -> bool:
        """Less than comparison."""
        return self._compare(other, lambda x, y: x < y)

    def __le__(self, other: Any) -> bool:
        """Less than or equal comparison."""
        return self._compare(other, lambda x, y: x <= y)

    def __eq__(self, other: Any) -> bool:
        """Equal comparison."""
        return self._compare(other, lambda x, y: x == y)

    def __ge__(self, other: Any) -> bool:
        """Greater than or equal comparison."""
        return self._compare(other, lambda x, y: x >= y)

    def __gt__(self, other: Any) -> bool:
        """Greater than comparison."""
        return self._compare(other, lambda x, y: x > y)

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
        """Resolve value using scenario priority (higher number = higher priority, following PLEXOS convention)."""
        return self._resolve_field_by_priority(priority, field="value")

    def get_entry(self) -> PLEXOSRow | None:
        """Get property entry with automatic scenario priority resolution.

        Similar to get_value() but returns the full PLEXOSRow entry instead of just the value.
        This is useful when you need to access other fields from the entry like variable_name,
        variable_id, action, etc.

        Returns
        -------
        PLEXOSRow | None
            The highest-priority entry or None if no entries exist
        """
        if not self.entries:
            return None

        priority = get_scenario_priority()
        if priority:
            return self._resolve_entry_by_priority(priority)

        # Without priority context, return first entry
        return next(iter(self.entries.values()))

    def _resolve_entry_by_priority(self, priority: dict[str, int]) -> PLEXOSRow | None:
        """Resolve entry using scenario priority (higher number = higher priority).

        Parameters
        ----------
        priority : dict[str, int]
            Scenario priority map (higher number = higher priority)

        Returns
        -------
        PLEXOSRow | None
            The entry from the highest priority scenario or None
        """
        simple_candidates: list[tuple[str | None, PLEXOSRow, float]] = []
        complex_candidates: list[tuple[str | None, PLEXOSRow, float]] = []

        for key, row in self.entries.items():
            # Assign priority based on scenario
            prio: float
            if key.scenario is None:
                prio = float(PRIORITY_NO_SCENARIO)
            elif key.scenario in priority:
                prio = float(priority[key.scenario])
            else:
                # Skip entries from scenarios not in the priority map
                continue

            # Categorize as simple or complex
            is_simple = (
                key.band == DEFAULT_BAND
                and key.timeslice is None
                and key.date_from is None
                and key.date_to is None
            )

            if is_simple:
                simple_candidates.append((key.scenario, row, prio))
            else:
                complex_candidates.append((key.scenario, row, prio))

        # Prefer simple candidates if they exist, otherwise use complex
        candidates = simple_candidates if simple_candidates else complex_candidates

        if not candidates:
            # Fallback: try to get first entry
            if self.entries:
                return next(iter(self.entries.values()))
            return None

        # Sort descending: highest priority value wins (PLEXOS convention)
        candidates.sort(key=lambda x: x[2], reverse=True)
        return candidates[0][1]

    def _resolve_field_by_priority(self, priority: dict[str, int], field: str = "value") -> Any:
        """Resolve any field using scenario priority.

        Parameters
        ----------
        priority : dict[str, int]
            Scenario priority map (higher number = higher priority)
        field : str
            Field name to extract from PLEXOSRow ('value', 'text', 'variable_name', etc.)

        Returns
        -------
        Any
            The field value from the highest priority entry
        """
        simple_candidates: list[tuple[str | None, Any, float]] = []
        complex_candidates: list[tuple[str | None, Any, float]] = []

        for key, row in self.entries.items():
            # Get the field value from the row
            field_value = getattr(row, field, None)

            # Skip entries without the requested field
            if field_value is None:
                continue

            # Assign priority based on scenario
            prio: float
            if key.scenario is None:
                prio = float(PRIORITY_NO_SCENARIO)
            elif key.scenario in priority:
                prio = float(priority[key.scenario])
            else:
                prio = float(PRIORITY_UNKNOWN_SCENARIO)

            # Categorize as simple or complex
            is_simple = (
                key.band == DEFAULT_BAND
                and key.timeslice is None
                and key.date_from is None
                and key.date_to is None
            )

            if is_simple:
                simple_candidates.append((key.scenario, field_value, prio))
            else:
                complex_candidates.append((key.scenario, field_value, prio))

        # Prefer simple candidates if they exist, otherwise use complex
        candidates = simple_candidates if simple_candidates else complex_candidates

        if not candidates:
            # Fallback: try to get first entry with the field
            for row in self.entries.values():
                field_value = getattr(row, field, None)
                if field_value is not None:
                    return field_value
            return None

        # Sort descending: highest priority value wins (PLEXOS convention)
        candidates.sort(key=lambda x: x[2], reverse=True)
        return candidates[0][1]

    def get_text_with_priority(self) -> str | None:
        """Get text field using scenario priority resolution.

        When multiple entries have the same priority (e.g., all with no scenario),
        returns the first non-None text value found.

        Returns
        -------
        str | None
            Text from highest priority entry, or None if no text exists
        """
        from r2x_plexos.models.context import get_scenario_priority

        priority = get_scenario_priority()
        if priority:
            result = self._resolve_field_by_priority(priority, field="text")
            if result:
                return cast(str, result)

        # Fallback: return first non-None text found
        # When all entries have same priority, just return first one
        for row in self.entries.values():
            if row.text:
                return row.text
        return None

    def get_variable_with_priority(self) -> dict[str, Any] | None:
        """Get variable reference using scenario priority resolution.

        Returns
        -------
        dict | None
            Dictionary with 'name', 'id', 'action' from highest priority entry
        """
        from r2x_plexos.models.context import get_scenario_priority

        priority = get_scenario_priority()
        if priority:
            var_name = self._resolve_field_by_priority(priority, field="variable_name")
            if var_name:
                # Find the entry with this variable name to get full details
                for row in self.entries.values():
                    if row.variable_name == var_name:
                        return {
                            "name": row.variable_name,
                            "id": row.variable_id,
                            "action": row.action,
                        }

        # Fallback: return first variable found
        for row in self.entries.values():
            if row.variable_name:
                return {
                    "name": row.variable_name,
                    "id": row.variable_id,
                    "action": row.action,
                }
        return None

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
        # Get priority and scenarios early to determine resolution strategy
        priority = get_scenario_priority()
        scenarios = self.get_scenarios()

        # Use priority-based resolution only if:
        # 1. Priority context exists, AND
        # 2. Property has scenarios OR property doesn't have multiple bands
        # Multi-band properties without scenarios should return band dict even with priority context
        if priority and (scenarios or not self.has_bands()):
            return self._resolve_by_priority(priority)

        default_key = PLEXOSPropertyKey(scenario=None, band=1, timeslice=None)
        has_pure_default = default_key in self.entries
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
