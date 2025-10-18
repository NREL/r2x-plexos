"""PLEXOS parser implementation for r2x-core framework."""

import itertools
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from importlib.metadata import version
from importlib.resources import files
from operator import itemgetter
from pathlib import Path
from typing import Any
from uuid import UUID

from infrasys import Component
from infrasys.time_series_models import SingleTimeSeries
from loguru import logger
from plexosdb import ClassEnum, PlexosDB

from r2x_core import BaseParser, DataStore

from .config import PLEXOSConfig
from .datafile_handler import extract_all_time_series
from .models import (
    PLEXOSDatafile,
    PLEXOSMembership,
    PLEXOSObject,
    PLEXOSPropertyValue,
    get_horizon,
    set_scenario_priority,
)
from .models.utils import get_field_name_by_alias
from .models.variable import PLEXOSVariable
from .utils_mappings import PLEXOS_TYPE_MAP
from .utils_plexosdb import get_collection_enum, get_collection_name

__version__ = version("r2x_plexos")

SCENARIO_ORDER = files("r2x_plexos.sql").joinpath("scenario_read_order.sql").read_text(encoding="utf-8-sig")


class TimeSeriesSourceType(str, Enum):
    """Enum for timeseries references."""

    DIRECT_DATAFILE = "DIRECT_DATAFILE"
    DATAFILE_COMPONENT = "DATAFILE_COMPONENT"
    VARIABLE = "VARIABLE"
    NESTED_VARIABLE = "NESTED_VARIABLE"


@dataclass
class TimeSeriesReference:
    """Reference for time series."""

    component_uuid: UUID
    component_name: str
    field_name: str
    source_type: TimeSeriesSourceType
    datafile_path: str | None = None
    datafile_component_name: str | None = None
    variable_name: str | None = None
    units: str | None = None
    property_name: str | None = None


class PLEXOSParser(BaseParser):
    """PLEXOS parser."""

    def __init__(
        self,
        config: PLEXOSConfig,
        data_store: DataStore,
        *,
        name: str | None = None,
        auto_add_composed_components: bool = True,
        skip_validation: bool = False,
        db: PlexosDB | None = None,
    ) -> None:
        """Initialize PLEXOS parser."""
        super().__init__(
            config,
            data_store,
            name=name,
            auto_add_composed_components=auto_add_composed_components,
            skip_validation=skip_validation,
        )
        self.config: PLEXOSConfig = config
        self.model_name = config.model_name
        self.time_series_references: list[TimeSeriesReference] = []
        self._component_cache: dict[int, PLEXOSObject] = {}
        self._valid_scenarios: list[str] = []
        self._datafile_cache: dict[str, dict[str, Any]] = {}
        self._property_cache: dict[str, float] = {}
        self._parsed_files_cache: dict[str, dict[str, SingleTimeSeries]] = {}
        self._attached_timeseries: dict[tuple[UUID, str], bool] = {}
        self._failed_references: list[tuple[TimeSeriesReference, str]] = []

        if not db:
            # NOTE: We should change either plexosdb db to take xmltree or an
            # easier way to get the fpath of resolved globs.
            data_file = data_store.get_data_file_by_name(name="xml_file")
            fpath = data_store.reader._get_file_path(data_file, data_store.folder)
            if fpath is None:
                msg = "Could not resolve XML file path from data store"
                raise ValueError(msg)
            self.db = PlexosDB.from_xml(fpath)
        assert self.db, "Database not created correctly. Check XML file."

    def validate_inputs(self) -> None:
        """Validate input data before parsing."""
        logger.info("Selecting model={}", self.model_name)
        model_id = self.db.get_object_id(ClassEnum.Model, self.model_name)
        scenario_results = self.db._db.query(SCENARIO_ORDER, (model_id,))
        # Build priority map from Read Order values
        # Follow PLEXOS convention: HIGHER Read Order = HIGHER priority (higher value wins)
        priority_map: dict[str, int] = {
            scenario: read_order or 0 for scenario, read_order in scenario_results
        }
        set_scenario_priority(priority_map)
        logger.debug("Found {} scenarios for model = {}", len(priority_map), self.model_name)
        return

    def build_system_components(self) -> None:
        """Create PLEXOS components."""
        logger.info("Building PLEXOS system components...")

        logger.trace("Querying objects from PLEXOS database...")
        # Sort by object_id before grouping to ensure all properties for the same object are consecutive
        sorted_properties = sorted(self.db.iterate_properties(), key=itemgetter("object_id"))
        for object_id, rows in itertools.groupby(sorted_properties, key=itemgetter("object_id")):
            rows_list = list(rows)
            if not rows_list:
                continue

            # PropertyRecord is a TypedDict from plexosdb, treat as dict for compatibility
            first_row: dict[str, Any] = rows_list[0]  # type: ignore[assignment]
            obj_type = first_row["child_class"]
            component = self._create_component(obj_type, rows_list)  # type: ignore[arg-type]
            if component:
                self.system.add_component(component)
                self._component_cache[object_id] = component

        self._add_memberships()

        return

    def build_time_series(self) -> None:
        """Attach time series data to components."""
        logger.info("Building time series data...")

        reference_year = self.config.reference_year or 2024
        horizon = get_horizon()

        try:
            from r2x_plexos.models.timeslice import PLEXOSTimeslice

            timeslices = list(self.system.get_components(PLEXOSTimeslice))
        except Exception:
            timeslices = []

        direct_refs = [
            ref
            for ref in self.time_series_references
            if ref.source_type == TimeSeriesSourceType.DIRECT_DATAFILE
        ]
        datafile_component_refs = [
            ref
            for ref in self.time_series_references
            if ref.source_type == TimeSeriesSourceType.DATAFILE_COMPONENT
        ]

        logger.info(f"Processing {len(direct_refs)} direct datafile references")
        logger.info(f"Processing {len(datafile_component_refs)} datafile component references")

        for ref in direct_refs:
            try:
                self._attach_direct_datafile_timeseries(ref, reference_year, timeslices, horizon)
            except Exception as e:
                logger.warning(f"Failed to attach {ref.component_name}.{ref.field_name}: {e}")
                self._failed_references.append((ref, str(e)))

        for ref in datafile_component_refs:
            try:
                self._attach_datafile_component_timeseries(ref, reference_year, timeslices, horizon)
            except Exception as e:
                logger.warning(f"Failed to attach {ref.component_name}.{ref.field_name}: {e}")
                self._failed_references.append((ref, str(e)))

        total_refs = len(direct_refs) + len(datafile_component_refs)
        success_count = total_refs - len(self._failed_references)
        logger.info(f"Time series complete: {success_count}/{total_refs} successful")

        if self._failed_references:
            failed_names = [ref.component_name for ref, _ in self._failed_references[:5]]
            logger.warning(f"Failed references (first 5): {failed_names}")

    def post_process_system(self) -> None:
        """Perform post-processing on the built system."""
        logger.info("Post-processing PLEXOS system...")

        self.system.data_format_version = __version__
        self.system.description = f"PLEXOS system for model'{self.config.model_name}"

        total_components = len(list(self.system.get_components(Component)))
        logger.info("System name: {}", self.system.name)
        logger.info("Total components: {}", total_components)
        logger.info("Post-processing complete")

    def _add_memberships(self) -> None:
        """Add membership relationships between PLEXOS components."""
        assert self.db is not None

        system_class_id = self.db.get_class_id(ClassEnum.System)
        membership_query = f"SELECT * from t_membership where parent_class_id <> {system_class_id}"

        for membership_dict in self.db._db.iter_dicts(membership_query):
            parent_object_id = membership_dict["parent_object_id"]
            child_object_id = membership_dict["child_object_id"]
            membership_id = membership_dict["membership_id"]
            collection_id = membership_dict["collection_id"]

            collection_name = get_collection_name(self.db, collection_id)
            if collection_name is None:
                continue

            collection_enum = get_collection_enum(collection_name)
            if collection_enum is None:
                continue

            parent_object = self._component_cache.get(parent_object_id)
            child_object = self._component_cache.get(child_object_id)

            if not parent_object or not child_object:
                logger.trace("Skip collection {} - missing parent or child", collection_name)
                continue

            self.system.add_supplemental_attribute(
                child_object,
                PLEXOSMembership(
                    membership_id=membership_id,
                    parent_object=parent_object,
                    collection=collection_enum,
                ),
            )

    def _create_component(self, obj_type: str, db_rows: list[dict[str, Any]]) -> PLEXOSObject | None:
        """Create a PLEXOS component from database rows."""
        if not db_rows:
            return None

        first_row = db_rows[0]
        name = first_row["name"]
        object_id = first_row["object_id"]
        object_category = first_row["category"]
        plexos_class = first_row["child_class"]

        # Try to get the class enum for this PLEXOS class
        # Use ClassEnum(value) to look up by value (handles "Data File" -> ClassEnum.DataFile)
        try:
            component_enum = ClassEnum(plexos_class)
        except ValueError:
            logger.warning(
                "Cannot parse object={} with type={}. Skipping it.",
                name,
                plexos_class,
            )
            return None

        # Get the corresponding Python class
        component_class = PLEXOS_TYPE_MAP.get(component_enum)
        if not component_class:
            logger.debug(f"Unsupported component type: {obj_type}")
            return None

        # Create the component
        logger.trace("Creating model for object={} with type={}", name, component_class)
        component: PLEXOSObject = component_class.model_construct(
            name=name,
            object_id=object_id,
            category=object_category,
        )

        # Process properties
        self._process_component_properties(component, db_rows)
        return component

    def _process_component_properties(self, component: PLEXOSObject, db_rows: list[dict[str, Any]]) -> None:
        """Process and attach properties to a component."""
        for prop_name, rows in itertools.groupby(db_rows, key=itemgetter("property")):
            if prop_name is None:
                continue

            field_name = get_field_name_by_alias(component, prop_name)
            if field_name is None:
                logger.warning(
                    "Property={} not supported for model={}. Skipping it.",
                    prop_name,
                    type(component).__name__,
                )
                continue

            prop_records = list(rows)
            property_value = PLEXOSPropertyValue.from_records(prop_records)
            setattr(component, field_name, property_value)

            if not isinstance(component, PLEXOSDatafile) and (
                property_value.has_datafile() or property_value.has_variable()
            ):
                self._register_time_series_reference(component, field_name, property_value)
        return

    def _register_time_series_reference(
        self, component: PLEXOSObject, field_name: str, property: PLEXOSPropertyValue
    ) -> None:
        """Register time series reference for later attachment."""
        if property.has_datafile():
            for row in property.entries.values():
                name = row.datafile_name or (
                    row.text if isinstance(row.text, str) and row.text.lower().endswith(".csv") else None
                )
                if not name:
                    continue

                if name.lower().endswith(".csv"):
                    self.time_series_references.append(
                        TimeSeriesReference(
                            component_uuid=component.uuid,
                            component_name=component.name,
                            field_name=field_name,
                            source_type=TimeSeriesSourceType.DIRECT_DATAFILE,
                            datafile_path=name,
                            units=property.units,
                        )
                    )
                else:
                    self.time_series_references.append(
                        TimeSeriesReference(
                            component_uuid=component.uuid,
                            component_name=component.name,
                            field_name=field_name,
                            source_type=TimeSeriesSourceType.DATAFILE_COMPONENT,
                            datafile_component_name=name,
                            units=property.units,
                        )
                    )
                break

        elif property.has_variable():
            self.time_series_references.append(
                TimeSeriesReference(
                    component_uuid=component.uuid,
                    component_name=component.name,
                    field_name=field_name,
                    source_type=TimeSeriesSourceType.VARIABLE,
                    variable_name=property.get_variables()[0],
                    units=property.units,
                )
            )

    def _resolve_datafile_path(self, datafile_path: str | None) -> Path:
        """Resolve datafile path relative to data store and timeseries dir."""
        if not datafile_path:
            raise ValueError("No datafile path provided")

        normalized_path = datafile_path.replace("\\", "/")

        base_path = Path(self.data_store.folder)
        if self.config.timeseries_dir:
            base_path = base_path / self.config.timeseries_dir

        full_path = base_path / normalized_path

        logger.trace(f"Resolved datafile path: {datafile_path} -> {full_path}")
        return full_path

    def _get_variable_profile_value(self, variable_id: int, variable_name: str) -> float:
        """Get the profile value from a variable component using scenario priority.

        Note: Variables may have properties in different scenarios than the current model.
        We check all entries to find a profile value, respecting scenario priority if available.
        """
        variable = self._component_cache.get(variable_id)
        if not variable:
            raise ValueError(f"Variable {variable_name} (ID={variable_id}) not found in component cache")

        if not isinstance(variable, PLEXOSVariable):
            raise ValueError(f"Component ID={variable_id} is not a Variable, got {type(variable).__name__}")

        profile_prop = variable.get_property_value("profile")
        if profile_prop is None:
            raise ValueError(f"Variable {variable_name} has no profile property")

        # Try scenario priority resolution first
        profile_value = profile_prop.get_value()
        if profile_value is not None:
            return float(profile_value)

        # If no value from priority resolution, check all entries
        # (Variable properties may exist in scenarios not loaded for the current model)
        for entry in profile_prop.entries.values():
            if entry.value is not None:
                return float(entry.value)

        raise ValueError(f"Variable {variable_name} has no profile value in any entry")

    def _apply_action_to_timeseries(
        self, ts: SingleTimeSeries, action: str, value: float
    ) -> SingleTimeSeries:
        """Apply an action operator to a time series."""
        action_map = {"\u00d7": "*", "x": "*", "*": "*", "+": "+", "-": "-", "/": "/", "=": "="}
        normalized_action = action_map.get(action, action)

        if normalized_action not in action_map.values():
            raise ValueError(f"Unsupported action: {action}")

        if normalized_action == "=" or normalized_action is None:
            return ts

        if normalized_action == "*":
            new_data = [x * value for x in ts.data]
        elif normalized_action == "+":
            new_data = [x + value for x in ts.data]
        elif normalized_action == "-":
            new_data = [x - value for x in ts.data]
        elif normalized_action == "/":
            if value == 0:
                raise ValueError("Cannot divide by zero")
            new_data = [x / value for x in ts.data]
        else:
            return ts

        return SingleTimeSeries.from_array(new_data, ts.name, ts.initial_timestamp, ts.resolution)

    def _get_or_parse_timeseries(
        self,
        file_path: str,
        component_name: str,
        reference_year: int,
        timeslices: list[Any] | None = None,
    ) -> SingleTimeSeries:
        """Get time series from cache or parse from file."""
        if file_path in self._parsed_files_cache:
            logger.trace(f"Using cached file parse: {file_path}")
            component_map = self._parsed_files_cache[file_path]
            if component_name in component_map:
                return component_map[component_name]
            else:
                raise ValueError(f"Component {component_name} not found in cached file {file_path}")

        logger.debug(f"Parsing time series file: {file_path}")

        initial_time = datetime(reference_year, 1, 1)
        ts_map = extract_all_time_series(
            path=file_path,
            default_initial_time=initial_time,
            year=reference_year,
            timeslices=timeslices,
        )

        self._parsed_files_cache[file_path] = ts_map
        logger.trace(f"Cached file with {len(ts_map)} time series: {file_path}")

        if component_name not in ts_map:
            if len(ts_map) == 1:
                return next(iter(ts_map.values()))
            raise ValueError(f"Component {component_name} not found in parsed file {file_path}")

        return ts_map[component_name]

    def _attach_direct_datafile_timeseries(
        self,
        ref: TimeSeriesReference,
        reference_year: int,
        timeslices: list[Any] | None,
        horizon: tuple[str, str] | None,
    ) -> None:
        """Attach time series from direct CSV file path."""
        cache_key = (ref.component_uuid, ref.field_name)
        if cache_key in self._attached_timeseries:
            return

        component = self.system.get_component_by_uuid(ref.component_uuid)
        if not component:
            raise ValueError(f"Component {ref.component_name} not found")

        file_path = self._resolve_datafile_path(ref.datafile_path)
        if not file_path.exists():
            raise FileNotFoundError(f"Data file not found: {file_path}")

        ts = self._get_or_parse_timeseries(
            file_path=str(file_path),
            component_name=ref.component_name,
            reference_year=reference_year,
            timeslices=timeslices,
        )

        property_value = component.get_property_value(ref.field_name)
        if isinstance(property_value, PLEXOSPropertyValue):
            entry = property_value.get_entry()
            if entry and entry.variable_name and entry.variable_id:
                logger.debug(
                    f"Property {ref.component_name}.{ref.field_name} has variable "
                    f"{entry.variable_name} (ID={entry.variable_id}) with action {entry.action}"
                )
                variable_value = self._get_variable_profile_value(entry.variable_id, entry.variable_name)
                logger.debug(f"Variable {entry.variable_name} profile value: {variable_value}")

                if entry.action:
                    ts_max_before = max(ts.data)
                    ts = self._apply_action_to_timeseries(ts, entry.action, variable_value)
                    ts_max_after = max(ts.data)

                    logger.debug(
                        f"Applied action {entry.action}: time series max "
                        f"before={ts_max_before}, after={ts_max_after}"
                    )

        self._attach_or_update_property(component, ref, ts, horizon)
        self._attached_timeseries[cache_key] = True

    def _attach_or_update_property(
        self,
        component: PLEXOSObject,
        ref: TimeSeriesReference,
        ts: SingleTimeSeries,
        horizon: tuple[str, str] | None,
    ) -> None:
        """Attach time series or update property value if constant."""
        unique_values = set(ts.data)
        is_constant = len(unique_values) == 1

        if is_constant:
            single_value = float(next(iter(unique_values)))
            logger.info(f"Updating constant value for {ref.component_name}.{ref.field_name}: {single_value}")

            if hasattr(component, ref.field_name):
                from r2x_plexos.models.base import PLEXOSRow

                prop_value = component.get_property_value(ref.field_name)
                if isinstance(prop_value, PLEXOSPropertyValue):
                    for key, entry in list(prop_value.entries.items()):
                        prop_value.entries[key] = PLEXOSRow(
                            value=single_value,
                            units=entry.units,
                            action=entry.action,
                            scenario_name=entry.scenario_name,
                            band=entry.band,
                            timeslice_name=entry.timeslice_name,
                            date_from=entry.date_from,
                            date_to=entry.date_to,
                            datafile_name=entry.datafile_name,
                            datafile_id=entry.datafile_id,
                            column_name=entry.column_name,
                            variable_name=entry.variable_name,
                            variable_id=entry.variable_id,
                            text=entry.text,
                        )
        else:
            field_ts = SingleTimeSeries.from_array(
                data=ts.data,
                name=ref.field_name,
                initial_timestamp=ts.initial_timestamp,
                resolution=ts.resolution,
            )
            features = {"horizon": horizon} if horizon else {}
            logger.trace(f"Attaching time series to {ref.component_name}.{ref.field_name}")
            self.system.add_time_series(field_ts, component, **features)

            # Update property value to max of time series
            max_value = float(max(ts.data))
            if hasattr(component, ref.field_name):
                from r2x_plexos.models.base import PLEXOSRow

                prop_value = component.get_property_value(ref.field_name)
                if isinstance(prop_value, PLEXOSPropertyValue):
                    for key, entry in list(prop_value.entries.items()):
                        prop_value.entries[key] = PLEXOSRow(
                            value=max_value,
                            units=entry.units,
                            action=entry.action,
                            scenario_name=entry.scenario_name,
                            band=entry.band,
                            timeslice_name=entry.timeslice_name,
                            date_from=entry.date_from,
                            date_to=entry.date_to,
                            datafile_name=entry.datafile_name,
                            datafile_id=entry.datafile_id,
                            column_name=entry.column_name,
                            variable_name=entry.variable_name,
                            variable_id=entry.variable_id,
                            text=entry.text,
                        )
                else:
                    # Property doesn't have a PLEXOSPropertyValue, set the attribute directly
                    setattr(component, ref.field_name, max_value)

    def _attach_datafile_component_timeseries(
        self,
        ref: TimeSeriesReference,
        reference_year: int,
        timeslices: list[Any] | None,
        horizon: tuple[str, str] | None,
    ) -> None:
        """Attach time series from datafile component reference (e.g., "FOM" -> "FOM.csv")."""
        cache_key = (ref.component_uuid, ref.field_name)
        if cache_key in self._attached_timeseries:
            return

        component = self.system.get_component_by_uuid(ref.component_uuid)
        if not component:
            raise ValueError(f"Component {ref.component_name} not found")

        from r2x_plexos.models.datafile import PLEXOSDatafile

        datafile_component = self.system.get_component(PLEXOSDatafile, ref.datafile_component_name)
        if not datafile_component:
            raise ValueError(f"Datafile '{ref.datafile_component_name}' not found")

        filename_prop = datafile_component.get_property_value("filename")
        if not filename_prop:
            raise ValueError(f"Datafile '{ref.datafile_component_name}' has no filename property")

        file_path_str = filename_prop.get_text_with_priority()

        if not file_path_str or not isinstance(file_path_str, str):
            raise ValueError(f"No valid filename in datafile '{ref.datafile_component_name}'")

        if not file_path_str.lower().endswith(".csv"):
            raise ValueError(
                f"Datafile '{ref.datafile_component_name}' filename is not a CSV: {file_path_str}"
            )

        file_path = self._resolve_datafile_path(file_path_str)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        ts = self._get_or_parse_timeseries(
            file_path=str(file_path),
            component_name=ref.component_name,
            reference_year=reference_year,
            timeslices=timeslices,
        )

        property_value = component.get_property_value(ref.field_name)
        if isinstance(property_value, PLEXOSPropertyValue):
            entry = property_value.get_entry()
            if entry and entry.variable_name and entry.variable_id:
                logger.debug(
                    f"Property {ref.component_name}.{ref.field_name} has variable "
                    f"{entry.variable_name} (ID={entry.variable_id}) with action {entry.action}"
                )
                variable_value = self._get_variable_profile_value(entry.variable_id, entry.variable_name)
                logger.debug(f"Variable {entry.variable_name} profile value: {variable_value}")

                if entry.action:
                    ts_max_before = max(ts.data)
                    ts = self._apply_action_to_timeseries(ts, entry.action, variable_value)
                    ts_max_after = max(ts.data)

                    logger.debug(
                        f"Applied action {entry.action}: time series max "
                        f"before={ts_max_before}, after={ts_max_after}"
                    )

        self._attach_or_update_property(component, ref, ts, horizon)
        self._attached_timeseries[cache_key] = True
