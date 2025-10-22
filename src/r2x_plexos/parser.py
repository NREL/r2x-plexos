"""PLEXOS parser implementation for r2x-core framework."""

import itertools
from collections import defaultdict
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
from .datafile_handler import ParsedFileData, extract_file_data, extract_one_time_series
from .models import (
    PLEXOSDatafile,
    PLEXOSMembership,
    PLEXOSObject,
    PLEXOSPropertyValue,
    get_horizon,
    set_horizon,
    set_scenario_priority,
)
from .models.collection_property import CollectionProperties
from .models.timeslice import PLEXOSTimeslice
from .models.utils import get_field_name_by_alias
from .models.variable import PLEXOSVariable
from .utils_mappings import PLEXOS_TYPE_MAP
from .utils_parser import (
    apply_action,
    apply_action_to_timeseries,
    create_plexos_row,
    to_snake_case,
    trim_timeseries_to_horizon,
)
from .utils_plexosdb import (
    get_collection_enum,
    get_collection_name,
    resolve_horizon_for_model,
)

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
    is_collection_property: bool = False
    membership_id: int | None = None


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
        self._parsed_files_cache: dict[str, ParsedFileData] = {}
        self._attached_timeseries: dict[tuple[UUID, str], bool] = {}
        self._failed_references: list[tuple[TimeSeriesReference, str]] = []
        self._membership_cache: dict[int, PLEXOSMembership] = {}
        # PropertyRecord from plexosdb.iterate_properties(), stored as dict for flexibility
        self._collection_properties_cache: dict[int, list[dict[str, Any]]] = {}

        self.db = db
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
        if self.db is None:
            msg = "Database not initialized"
            raise ValueError(msg)

        logger.info("Selecting model={}", self.model_name)
        model_id = self.db.get_object_id(ClassEnum.Model, self.model_name)
        scenario_results = self.db._db.query(SCENARIO_ORDER, (model_id,))
        priority_map: dict[str, int] = {
            scenario: read_order or 0 for scenario, read_order in scenario_results
        }
        set_scenario_priority(priority_map)
        logger.debug("Found {} scenarios for model = {}", len(priority_map), self.model_name)

        # Resolve horizon from the model
        horizon_range = resolve_horizon_for_model(self.db, self.model_name)
        if horizon_range is not None:
            horizon_start: datetime
            horizon_end: datetime
            horizon_start, horizon_end = horizon_range
            # Validate that horizon year is reasonable relative to reference year
            # If horizon is more than 5 years before reference year, ignore it
            if self.config.horizon_year is not None and horizon_start.year < self.config.horizon_year - 5:
                logger.warning(
                    "Horizon year {} is too far before reference year {}, ignoring horizon",
                    horizon_start.year,
                    self.config.horizon_year,
                )
            else:
                self._horizon_start, self._horizon_end = horizon_start, horizon_end
                horizon_str = (self._horizon_start.isoformat(), self._horizon_end.isoformat())
                set_horizon(horizon_str)
                logger.info(
                    "Horizon set: {} to {} ({} total hours)",
                    self._horizon_start,
                    self._horizon_end,
                    int((self._horizon_end - self._horizon_start).total_seconds() / 3600),
                )

        return

    def build_system_components(self) -> None:
        """Create PLEXOS components."""
        if self.db is None:
            msg = "Database not initialized"
            raise ValueError(msg)

        logger.info("Building PLEXOS system components...")

        logger.trace("Querying objects from PLEXOS database...")
        main_properties_by_object: dict[int, list[dict[str, Any]]] = defaultdict(list)
        collection_properties_by_object: dict[int, list[dict[str, Any]]] = defaultdict(list)

        for prop in self.db.iterate_properties():
            parent_class = prop.get("parent_class")
            object_id = prop.get("object_id")

            if not object_id or not isinstance(object_id, int):
                continue

            prop_dict: dict[str, Any] = prop  # type: ignore[assignment]

            if parent_class and parent_class != "System":
                collection_properties_by_object[object_id].append(prop_dict)
            else:
                main_properties_by_object[object_id].append(prop_dict)

        self._collection_properties_cache = collection_properties_by_object

        for object_id, rows_list in main_properties_by_object.items():
            if not rows_list:
                continue

            first_row = rows_list[0]
            obj_type = first_row["child_class"]
            component = self._create_component(obj_type, rows_list)
            if component:
                self.system.add_component(component)
                self._component_cache[object_id] = component

        self._add_memberships()
        self._add_collection_properties()

        return

    def build_time_series(self) -> None:
        """Attach time series data to components."""
        logger.info("Building time series data...")

        reference_year = self.config.horizon_year or 2024
        horizon = get_horizon()

        # Get horizon datetime objects if available
        horizon_datetime = None
        if hasattr(self, "_horizon_start") and hasattr(self, "_horizon_end"):
            horizon_datetime = (self._horizon_start, self._horizon_end)

        try:
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
        variable_refs = [
            ref for ref in self.time_series_references if ref.source_type == TimeSeriesSourceType.VARIABLE
        ]

        logger.info(f"Processing {len(direct_refs)} direct datafile references")
        logger.info(f"Processing {len(datafile_component_refs)} datafile component references")
        logger.info(f"Processing {len(variable_refs)} variable references")

        for ref in direct_refs:
            try:
                self._attach_direct_datafile_timeseries(
                    ref, reference_year, timeslices, horizon, horizon_datetime
                )
            except Exception as e:
                logger.warning(f"Failed to attach {ref.component_name}.{ref.field_name}: {e}")
                self._failed_references.append((ref, str(e)))

        for ref in datafile_component_refs:
            try:
                self._attach_datafile_component_timeseries(
                    ref, reference_year, timeslices, horizon, horizon_datetime
                )
            except Exception as e:
                logger.warning(f"Failed to attach {ref.component_name}.{ref.field_name}: {e}")
                self._failed_references.append((ref, str(e)))

        for ref in variable_refs:
            try:
                self._attach_variable_timeseries(ref, reference_year, timeslices, horizon, horizon_datetime)
            except Exception as e:
                logger.warning(f"Failed to attach {ref.component_name}.{ref.field_name} from variable: {e}")
                self._failed_references.append((ref, str(e)))

        total_refs = len(direct_refs) + len(datafile_component_refs) + len(variable_refs)
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

            membership = PLEXOSMembership(
                membership_id=membership_id,
                parent_object=parent_object,
                child_object=child_object,
                collection=collection_enum,
            )

            self._membership_cache[membership_id] = membership

            self.system.add_supplemental_attribute(
                child_object,
                membership,
            )
            self.system.add_supplemental_attribute(
                parent_object,
                membership,
            )

    def _add_collection_properties(self) -> None:
        """Add collection properties as supplemental attributes."""
        for object_id, coll_props_list in self._collection_properties_cache.items():
            component = self._component_cache.get(object_id)
            if not component:
                logger.trace(f"Component for object_id {object_id} not found, skipping collection properties")
                continue

            memberships = self.system.get_supplemental_attributes_with_component(component, PLEXOSMembership)
            if not memberships:
                logger.trace(f"No memberships found for {component.name}, skipping collection properties")
                continue

            props_by_parent_and_collection: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)

            for prop in coll_props_list:
                parent_class = prop.get("parent_class")
                parent_id = prop.get("parent_id")
                if parent_class and parent_id:
                    key = (parent_class, str(parent_id))
                    props_by_parent_and_collection[key].append(prop)

            for (_parent_class, parent_id_str), props_list in props_by_parent_and_collection.items():
                parent_id = int(parent_id_str)

                matching_membership = None
                for mem in memberships:
                    if mem.parent_object.object_id == parent_id:
                        matching_membership = mem
                        break

                if not matching_membership:
                    logger.trace(f"No matching membership found for parent_id {parent_id}")
                    continue

                collection_name = (
                    matching_membership.collection.value if matching_membership.collection else "Unknown"
                )

                properties_by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
                for prop in props_list:
                    prop_name = prop.get("property")
                    if prop_name:
                        properties_by_name[prop_name].append(prop)

                property_values: dict[str, PLEXOSPropertyValue] = {}
                for property_name, prop_rows in properties_by_name.items():
                    field_name = to_snake_case(property_name)
                    property_value = PLEXOSPropertyValue.from_records(prop_rows)
                    property_values[field_name] = property_value

                    if property_value.has_datafile() or property_value.has_variable():
                        self._register_collection_property_time_series_reference(
                            component, field_name, property_value, matching_membership.membership_id
                        )

                if property_values:
                    collection_props = CollectionProperties(
                        membership=matching_membership,
                        collection_name=collection_name,
                        properties=property_values,
                    )
                    self.system.add_supplemental_attribute(component, collection_props)
                    logger.trace(
                        f"Added collection properties for {component.name} (membership {matching_membership.membership_id}): "
                        f"{list(property_values.keys())}"
                    )

    def _register_collection_property_time_series_reference(
        self, component: PLEXOSObject, field_name: str, property: PLEXOSPropertyValue, membership_id: int
    ) -> None:
        """Register time series reference for collection properties."""
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
                            is_collection_property=True,
                            membership_id=membership_id,
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
                            is_collection_property=True,
                            membership_id=membership_id,
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
                    is_collection_property=True,
                    membership_id=membership_id,
                )
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

        try:
            component_enum = ClassEnum(plexos_class)
        except ValueError:
            logger.warning(
                "Cannot parse object={} with type={}. Skipping it.",
                name,
                plexos_class,
            )
            return None

        component_class = PLEXOS_TYPE_MAP.get(component_enum)
        if not component_class:
            logger.debug(f"Unsupported component type: {obj_type}")
            return None

        logger.trace("Creating model for object={} with type={}", name, component_class)
        component: PLEXOSObject = component_class.model_construct(
            name=name,
            object_id=object_id,
            category=object_category,
        )

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

            # Skip time series registration for DataFile, Variable, and Timeslice components
            # Variables should always use their constant property values
            # Timeslices are configuration metadata defining time periods, not data components
            if not isinstance(component, (PLEXOSDatafile, PLEXOSVariable, PLEXOSTimeslice)) and (
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
        return base_path / normalized_path

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

        profile_value = profile_prop.get_value()
        if profile_value is not None:
            return float(profile_value)

        for entry in profile_prop.entries.values():
            if entry.value is not None:
                return float(entry.value)

        raise ValueError(f"Variable {variable_name} has no profile value in any entry")

    def _get_or_parse_timeseries(
        self,
        file_path: str,
        component_name: str,
        reference_year: int,
        timeslices: list[Any] | None = None,
        horizon_datetime: tuple[datetime, datetime] | None = None,
    ) -> Any:
        """Get time series from cache or parse from file. Returns float for constant value files."""
        if file_path in self._parsed_files_cache:
            logger.debug(f"Using cached file parse: {file_path}")
            component_map = self._parsed_files_cache[file_path]
            logger.debug(f"Cached map has {len(component_map)} entries: {list(component_map.keys())[:5]}")

            # Handle empty cache (file has no data for this year/scenario)
            if len(component_map) == 0:
                raise ValueError(f"File {file_path} contains no data for the requested year")

            ts: Any
            if component_name in component_map:
                ts = component_map[component_name]
            elif len(component_map) == 1:
                logger.debug(f"Using single entry fallback for component '{component_name}'")
                ts = next(iter(component_map.values()))
            else:
                available = list(component_map.keys())[:10]
                raise ValueError(
                    f"Component '{component_name}' not found in cached file {file_path}. "
                    f"Available components (first 10): {available}"
                )

            # Apply horizon trimming if needed (only for time series, not floats)
            if horizon_datetime and isinstance(ts, SingleTimeSeries):
                ts = trim_timeseries_to_horizon(ts, horizon_datetime[0], horizon_datetime[1])

            return ts

        logger.debug(f"Parsing time series file: {file_path}")

        # Use horizon start year if available, otherwise reference year
        extraction_year = horizon_datetime[0].year if horizon_datetime else reference_year
        initial_time = datetime(extraction_year, 1, 1)
        ts_map = extract_file_data(
            path=file_path,
            default_initial_time=initial_time,
            year=extraction_year,
            timeslices=timeslices,
        )

        self._parsed_files_cache[file_path] = ts_map
        logger.trace(f"Cached file with {len(ts_map)} time series: {file_path}")

        if component_name not in ts_map:
            if len(ts_map) == 1:
                logger.debug(f"Using single entry fallback for component '{component_name}' in parsed file")
                ts = next(iter(ts_map.values()))
            else:
                available = list(ts_map.keys())[:10]
                raise ValueError(
                    f"Component '{component_name}' not found in parsed file {file_path}. "
                    f"Available components (first 10): {available}"
                )
        else:
            ts = ts_map[component_name]

        # Apply horizon trimming if needed (only for time series, not floats)
        if horizon_datetime and isinstance(ts, SingleTimeSeries):
            ts = trim_timeseries_to_horizon(ts, horizon_datetime[0], horizon_datetime[1])

        return ts

    def _attach_direct_datafile_timeseries(
        self,
        ref: TimeSeriesReference,
        reference_year: int,
        timeslices: list[Any] | None,
        horizon: tuple[str, str] | None,
        horizon_datetime: tuple[datetime, datetime] | None = None,
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
            horizon_datetime=horizon_datetime,
        )

        # Handle float constant values
        if isinstance(ts, float):
            constant_value = ts
            if not ref.is_collection_property:
                property_value = component.get_property_value(ref.field_name)
                if isinstance(property_value, PLEXOSPropertyValue):
                    entry = property_value.get_entry()
                    if entry and entry.variable_name and entry.variable_id:
                        logger.debug(
                            f"Property {ref.component_name}.{ref.field_name} has variable "
                            f"{entry.variable_name} (ID={entry.variable_id}) with action {entry.action}"
                        )
                        variable_value = self._get_variable_profile_value(
                            entry.variable_id, entry.variable_name
                        )
                        logger.debug(f"Variable {entry.variable_name} profile value: {variable_value}")

                        if entry.action:
                            constant_value = apply_action(constant_value, variable_value, entry.action)
                            logger.debug(f"Applied action {entry.action}: constant value = {constant_value}")

            # Set the constant value on the component by updating property entries
            if hasattr(component, ref.field_name):
                prop_value = component.get_property_value(ref.field_name)
                if isinstance(prop_value, PLEXOSPropertyValue):
                    for key, entry in list(prop_value.entries.items()):
                        prop_value.entries[key] = create_plexos_row(constant_value, entry)
                else:
                    setattr(component, ref.field_name, constant_value)
        else:
            # Handle time series (SingleTimeSeries)
            if not ref.is_collection_property:
                property_value = component.get_property_value(ref.field_name)
                if isinstance(property_value, PLEXOSPropertyValue):
                    entry = property_value.get_entry()
                    if entry and entry.variable_name and entry.variable_id:
                        logger.debug(
                            f"Property {ref.component_name}.{ref.field_name} has variable "
                            f"{entry.variable_name} (ID={entry.variable_id}) with action {entry.action}"
                        )
                        variable_value = self._get_variable_profile_value(
                            entry.variable_id, entry.variable_name
                        )
                        logger.debug(f"Variable {entry.variable_name} profile value: {variable_value}")

                        if entry.action:
                            ts_max_before = max(ts.data)
                            ts = apply_action_to_timeseries(ts, entry.action, variable_value)
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

        if ref.is_collection_property:
            self._attach_collection_property_timeseries(component, ref, ts, horizon, is_constant)
            return

        if is_constant:
            single_value = float(next(iter(unique_values)))
            logger.debug(f"Updating constant value for {ref.component_name}.{ref.field_name}: {single_value}")

            if hasattr(component, ref.field_name):
                prop_value = component.get_property_value(ref.field_name)
                if isinstance(prop_value, PLEXOSPropertyValue):
                    for key, entry in list(prop_value.entries.items()):
                        prop_value.entries[key] = create_plexos_row(single_value, entry)
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

            max_value = float(max(ts.data))
            if hasattr(component, ref.field_name):
                prop_value = component.get_property_value(ref.field_name)
                if isinstance(prop_value, PLEXOSPropertyValue):
                    for key, entry in list(prop_value.entries.items()):
                        prop_value.entries[key] = create_plexos_row(max_value, entry)
                else:
                    setattr(component, ref.field_name, max_value)

    def _attach_collection_property_timeseries(
        self,
        component: PLEXOSObject,
        ref: TimeSeriesReference,
        ts: SingleTimeSeries,
        horizon: tuple[str, str] | None,
        is_constant: bool,
    ) -> None:
        """Attach time series to a collection property."""
        coll_props_list = self.system.get_supplemental_attributes_with_component(
            component, CollectionProperties
        )

        target_coll_props = None
        for cp in coll_props_list:
            if cp.membership.membership_id == ref.membership_id:
                target_coll_props = cp
                break

        if not target_coll_props:
            logger.warning(f"Collection properties not found for membership {ref.membership_id}")
            return

        if ref.field_name not in target_coll_props.properties:
            logger.warning(f"Property {ref.field_name} not found in collection properties")
            return

        property_value = target_coll_props.properties[ref.field_name]

        if is_constant:
            unique_values = set(ts.data)
            single_value = float(next(iter(unique_values)))
            logger.debug(
                f"Updating constant collection property value for {ref.component_name}.{ref.field_name}: {single_value}"
            )

            for key, entry in list(property_value.entries.items()):
                property_value.entries[key] = create_plexos_row(single_value, entry)
        else:
            field_ts = SingleTimeSeries.from_array(
                data=ts.data,
                name=ref.field_name,
                initial_timestamp=ts.initial_timestamp,
                resolution=ts.resolution,
            )
            features = {"horizon": horizon} if horizon else {}
            logger.trace(
                f"Attaching time series to collection property {ref.component_name}.{ref.field_name}"
            )
            self.system.add_time_series(field_ts, target_coll_props, **features)

            max_value = float(max(ts.data))
            for key, entry in list(property_value.entries.items()):
                property_value.entries[key] = create_plexos_row(max_value, entry)

    def _attach_band_timeseries(
        self,
        component: PLEXOSObject,
        ref: TimeSeriesReference,
        band_num: int,
        ts: SingleTimeSeries,
        horizon: tuple[str, str] | None,
    ) -> float:
        """Attach a single band time series to component and return max value."""
        field_ts = SingleTimeSeries.from_array(
            data=ts.data,
            name=ref.field_name,
            initial_timestamp=ts.initial_timestamp,
            resolution=ts.resolution,
        )

        features: dict[str, Any] = {"band": band_num}
        if horizon:
            features["horizon"] = horizon

        logger.debug(f"Attaching band {band_num} time series to {ref.component_name}.{ref.field_name}")
        self.system.add_time_series(field_ts, component, **features)

        return float(max(ts.data))

    def _handle_constant_variable(
        self,
        ref: TimeSeriesReference,
        component: PLEXOSObject,
        variable_name: str,
        variable_value: float,
        cache_key: tuple[UUID, str],
    ) -> None:
        """Handle variables without datafiles (constant values)."""
        logger.debug(
            f"Applying constant variable '{variable_name}' ({variable_value}) to {ref.component_name}.{ref.field_name}"
        )

        if ref.is_collection_property:
            coll_props_list = self.system.get_supplemental_attributes_with_component(
                component, CollectionProperties
            )

            target_coll_props = None
            for cp in coll_props_list:
                if cp.membership.membership_id == ref.membership_id:
                    target_coll_props = cp
                    break

            if not target_coll_props:
                logger.warning(f"Collection properties not found for membership {ref.membership_id}")
                self._attached_timeseries[cache_key] = True
                return

            if ref.field_name not in target_coll_props.properties:
                logger.warning(f"Property {ref.field_name} not found in collection properties")
                self._attached_timeseries[cache_key] = True
                return

            property_value = target_coll_props.properties[ref.field_name]
            for key, entry in list(property_value.entries.items()):
                property_value.entries[key] = create_plexos_row(variable_value, entry)
        else:
            if hasattr(component, ref.field_name):
                prop_value = component.get_property_value(ref.field_name)
                if isinstance(prop_value, PLEXOSPropertyValue):
                    for key, entry in list(prop_value.entries.items()):
                        prop_value.entries[key] = create_plexos_row(variable_value, entry)

        self._attached_timeseries[cache_key] = True

    def _attach_variable_timeseries(
        self,
        ref: TimeSeriesReference,
        reference_year: int,
        timeslices: list[Any] | None,
        horizon: tuple[str, str] | None,
        horizon_datetime: tuple[datetime, datetime] | None = None,
    ) -> None:
        """Attach time series from a variable's profile property."""
        cache_key = (ref.component_uuid, ref.field_name)
        if cache_key in self._attached_timeseries:
            return

        component = self.system.get_component_by_uuid(ref.component_uuid)
        if not component:
            raise ValueError(f"Component {ref.component_name} not found")

        variable_name = ref.variable_name
        if not variable_name:
            raise ValueError(f"No variable name in reference for {ref.component_name}.{ref.field_name}")

        variable = self.system.get_component(PLEXOSVariable, variable_name)
        if not variable:
            raise ValueError(f"Variable '{variable_name}' not found")

        profile_prop = variable.get_property_value("profile")
        if not profile_prop or not isinstance(profile_prop, PLEXOSPropertyValue):
            logger.debug(f"Variable '{variable_name}' has no profile property")
            return

        bands = sorted({entry.band for entry in profile_prop.entries.values() if entry.band})

        first_entry = next(iter(profile_prop.entries.values()))
        if not first_entry.datafile_name:
            variable_value = profile_prop.get_value()

            if variable_value is None:
                logger.debug(f"Variable '{variable_name}' profile has no datafile and no constant value")
                return

            self._handle_constant_variable(ref, component, variable_name, variable_value, cache_key)
            return

        if not bands:
            variable_constant_value = profile_prop.get_value()

            if variable_constant_value is not None and variable_constant_value != 0:
                logger.debug(
                    f"Variable '{variable_name}' has constant value {variable_constant_value}, applying to datafile values"
                )

                datafile_component_name = first_entry.datafile_name
                file_path = None

                datafile_component = self.system.get_component(PLEXOSDatafile, datafile_component_name)
                if datafile_component:
                    filename_prop = datafile_component.get_property_value("filename")
                    if filename_prop:
                        file_path_str = filename_prop.get_text_with_priority()
                        if file_path_str and isinstance(file_path_str, str):
                            file_path = self._resolve_datafile_path(file_path_str)

                if file_path is None:
                    logger.debug(f"Trying as direct path: {datafile_component_name}")
                    file_path = self._resolve_datafile_path(datafile_component_name)

                if not file_path.exists():
                    raise FileNotFoundError(f"File not found: {file_path}")

                try:
                    # Use horizon start year if available, otherwise reference year
                    extraction_year = horizon_datetime[0].year if horizon_datetime else reference_year
                    ts_or_float = extract_one_time_series(
                        path=str(file_path),
                        component=ref.component_name,
                        default_initial_time=datetime(extraction_year, 1, 1),
                        year=extraction_year,
                    )

                    # For collection properties with variables, we expect time series not constants
                    if not isinstance(ts_or_float, SingleTimeSeries):
                        logger.warning(
                            f"Expected time series but got constant value for {ref.component_name}.{ref.field_name}"
                        )
                        return

                    ts = ts_or_float

                    # Apply horizon trimming if needed
                    if horizon_datetime:
                        ts = trim_timeseries_to_horizon(ts, horizon_datetime[0], horizon_datetime[1])

                    base_value = ts.data[0] if len(set(ts.data)) == 1 else max(ts.data)
                    result_value = base_value * variable_constant_value

                    logger.info(
                        f"Applying variable '{variable_name}' ({variable_constant_value}) to {ref.component_name}.{ref.field_name}: {base_value} x {variable_constant_value} = {result_value}"
                    )

                    if hasattr(component, ref.field_name):
                        prop_value = component.get_property_value(ref.field_name)
                        if isinstance(prop_value, PLEXOSPropertyValue):
                            for key, entry in list(prop_value.entries.items()):
                                prop_value.entries[key] = create_plexos_row(result_value, entry)

                    self._attached_timeseries[cache_key] = True
                    return

                except Exception as e:
                    logger.warning(
                        f"Failed to apply constant variable '{variable_name}' to {ref.component_name}.{ref.field_name}: {e}"
                    )
                    return
            else:
                logger.debug(f"Variable '{variable_name}' has no bands")
                return

        property_value = component.get_property_value(ref.field_name)
        action = None

        if isinstance(property_value, PLEXOSPropertyValue):
            prop_entry = property_value.get_entry()
            if prop_entry:
                action = prop_entry.action

        all_max_values = []

        for band_num in bands:
            band_entry = None
            for entry in profile_prop.entries.values():
                if entry.band == band_num:
                    band_entry = entry
                    break

            if not band_entry or not band_entry.datafile_name:
                logger.debug(f"Variable '{variable_name}' band {band_num} has no datafile")
                continue

            # Resolve actual file path from datafile component
            datafile_component_name = band_entry.datafile_name
            band_file_path = None

            try:
                datafile_component = self.system.get_component(PLEXOSDatafile, datafile_component_name)
                if datafile_component:
                    filename_prop = datafile_component.get_property_value("filename")
                    if filename_prop:
                        file_path_str = filename_prop.get_text_with_priority()
                        if file_path_str and isinstance(file_path_str, str):
                            band_file_path = self._resolve_datafile_path(file_path_str)
            except Exception:
                # Datafile component not found in system, will try as direct path
                pass

            if band_file_path is None:
                logger.debug(
                    f"Datafile '{datafile_component_name}' has no filename property, trying as direct path"
                )
                band_file_path = self._resolve_datafile_path(datafile_component_name)

            if not band_file_path.exists():
                logger.warning(
                    f"Datafile not found: '{datafile_component_name}' (band {band_num}) "
                    f"referenced by variable '{variable_name}' for {ref.component_name}.{ref.field_name}. "
                    f"Expected path: {band_file_path}"
                )
                continue

            if str(band_file_path) in self._parsed_files_cache:
                ts_map = self._parsed_files_cache[str(band_file_path)]
            else:
                # Use horizon start year if available, otherwise reference year
                extraction_year = horizon_datetime[0].year if horizon_datetime else reference_year
                ts_map = extract_file_data(
                    path=str(band_file_path),
                    default_initial_time=datetime(extraction_year, 1, 1),
                    year=extraction_year,
                    timeslices=timeslices,
                )
                self._parsed_files_cache[str(band_file_path)] = ts_map

            if ref.component_name not in ts_map:
                logger.debug(
                    f"Component '{ref.component_name}' not found in band {band_num} file {band_file_path}"
                )
                continue

            ts_value = ts_map[ref.component_name]

            # Band data must be time series, not float constants
            if not isinstance(ts_value, SingleTimeSeries):
                continue

            ts = ts_value

            # Apply horizon trimming if needed
            if horizon_datetime:
                ts = trim_timeseries_to_horizon(ts, horizon_datetime[0], horizon_datetime[1])

            if action:
                base_value = property_value.get_value()
                if base_value and base_value != 0:
                    ts = apply_action_to_timeseries(ts, action, base_value)

            max_value = self._attach_band_timeseries(component, ref, band_num, ts, horizon)
            all_max_values.append(max_value)

        if all_max_values:
            global_max = max(all_max_values)
            logger.debug(f"Global max across {len(all_max_values)} variable bands: {global_max}")
            if hasattr(component, ref.field_name):
                prop_value = component.get_property_value(ref.field_name)
                if isinstance(prop_value, PLEXOSPropertyValue):
                    for key, entry in list(prop_value.entries.items()):
                        prop_value.entries[key] = create_plexos_row(global_max, entry)
                else:
                    setattr(component, ref.field_name, global_max)

        self._attached_timeseries[cache_key] = True

    def _attach_datafile_component_timeseries(
        self,
        ref: TimeSeriesReference,
        reference_year: int,
        timeslices: list[Any] | None,
        horizon: tuple[str, str] | None,
        horizon_datetime: tuple[datetime, datetime] | None = None,
    ) -> None:
        """Attach time series from datafile component reference (e.g., "FOM" -> "FOM.csv")."""
        cache_key = (ref.component_uuid, ref.field_name)
        if cache_key in self._attached_timeseries:
            return

        component = self.system.get_component_by_uuid(ref.component_uuid)
        if not component:
            raise ValueError(f"Component {ref.component_name} not found")

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

        # Use horizon start year if available, otherwise reference year
        extraction_year = horizon_datetime[0].year if horizon_datetime else reference_year
        initial_time = datetime(extraction_year, 1, 1)

        if str(file_path) not in self._parsed_files_cache:
            self._parsed_files_cache[str(file_path)] = extract_file_data(
                path=str(file_path),
                default_initial_time=initial_time,
                year=extraction_year,
                timeslices=timeslices,
            )

        file_map = self._parsed_files_cache[str(file_path)]
        band_ts_keys = [key for key in file_map if key.startswith(f"{ref.component_name}_band_")]

        if band_ts_keys:
            logger.debug(
                f"Found {len(band_ts_keys)} band time series for {ref.component_name}.{ref.field_name}"
            )

            property_value = component.get_property_value(ref.field_name)
            variable_value = None
            action = None

            if isinstance(property_value, PLEXOSPropertyValue):
                entry = property_value.get_entry()
                if entry and entry.variable_name and entry.variable_id:
                    variable_value = self._get_variable_profile_value(entry.variable_id, entry.variable_name)
                    action = entry.action

            all_max_values = []

            for band_key in sorted(band_ts_keys):
                band_num = int(band_key.split("_band_")[-1])
                ts_value = file_map[band_key]

                # Band data must be time series, not float constants
                if not isinstance(ts_value, SingleTimeSeries):
                    continue

                ts = ts_value

                # Apply horizon trimming if needed
                if horizon_datetime:
                    ts = trim_timeseries_to_horizon(ts, horizon_datetime[0], horizon_datetime[1])

                if action and variable_value is not None:
                    ts = apply_action_to_timeseries(ts, action, variable_value)

                max_value = self._attach_band_timeseries(component, ref, band_num, ts, horizon)
                all_max_values.append(max_value)

            if all_max_values:
                global_max = max(all_max_values)
                logger.debug(f"Global max across {len(band_ts_keys)} bands: {global_max}")
                if hasattr(component, ref.field_name):
                    prop_value = component.get_property_value(ref.field_name)
                    if isinstance(prop_value, PLEXOSPropertyValue):
                        for key, entry in list(prop_value.entries.items()):
                            prop_value.entries[key] = create_plexos_row(global_max, entry)
                    else:
                        setattr(component, ref.field_name, global_max)
        else:
            ts_or_float = self._get_or_parse_timeseries(
                file_path=str(file_path),
                component_name=ref.component_name,
                reference_year=reference_year,
                timeslices=timeslices,
                horizon_datetime=horizon_datetime,
            )

            # Handle float constant values
            if isinstance(ts_or_float, float):
                constant_value: float = ts_or_float
                property_value = component.get_property_value(ref.field_name)
                if isinstance(property_value, PLEXOSPropertyValue):
                    entry = property_value.get_entry()
                    if entry and entry.variable_name and entry.variable_id:
                        logger.debug(
                            f"Property {ref.component_name}.{ref.field_name} has variable "
                            f"{entry.variable_name} (ID={entry.variable_id}) with action {entry.action}"
                        )
                        variable_value = self._get_variable_profile_value(
                            entry.variable_id, entry.variable_name
                        )
                        logger.debug(f"Variable {entry.variable_name} profile value: {variable_value}")

                        constant_value = apply_action(constant_value, variable_value, entry.action)
                        logger.debug(f"Applied action {entry.action}: constant value = {constant_value}")

                # Set the constant value on the component by updating property entries
                if hasattr(component, ref.field_name):
                    prop_value = component.get_property_value(ref.field_name)
                    if isinstance(prop_value, PLEXOSPropertyValue):
                        for key, entry in list(prop_value.entries.items()):
                            prop_value.entries[key] = create_plexos_row(constant_value, entry)
                    else:
                        setattr(component, ref.field_name, constant_value)
            else:
                # Handle time series (SingleTimeSeries)
                ts = ts_or_float
                property_value = component.get_property_value(ref.field_name)
                if isinstance(property_value, PLEXOSPropertyValue):
                    entry = property_value.get_entry()
                    if entry and entry.variable_name and entry.variable_id:
                        logger.debug(
                            f"Property {ref.component_name}.{ref.field_name} has variable "
                            f"{entry.variable_name} (ID={entry.variable_id}) with action {entry.action}"
                        )
                        variable_value = self._get_variable_profile_value(
                            entry.variable_id, entry.variable_name
                        )
                        logger.debug(f"Variable {entry.variable_name} profile value: {variable_value}")

                        if entry.action:
                            ts_max_before = max(ts.data)
                            ts = apply_action_to_timeseries(ts, entry.action, variable_value)
                            ts_max_after = max(ts.data)

                            logger.debug(
                                f"Applied action {entry.action}: time series max "
                                f"before={ts_max_before}, after={ts_max_after}"
                            )

                self._attach_or_update_property(component, ref, ts, horizon)

        self._attached_timeseries[cache_key] = True
