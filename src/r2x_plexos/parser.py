"""PLEXOS parser implementation for r2x-core framework."""

import itertools
from dataclasses import dataclass
from enum import Enum
from importlib.metadata import version
from importlib.resources import files
from operator import itemgetter
from typing import Any
from uuid import UUID

from infrasys import Component
from loguru import logger
from plexosdb import ClassEnum, CollectionEnum, PlexosDB

from r2x_core import BaseParser, DataStore
from r2x_plexos.models.membership import PLEXOSMembership
from r2x_plexos.models.property import PLEXOSPropertyValue
from r2x_plexos.models.utils import get_field_name_by_alias

from .config import PLEXOSConfig
from .models.component import PLEXOSObject
from .models.context import set_scenario_priority
from .util_mappings import PLEXOS_TYPE_MAP

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
        logger.info("Time series attachment complete")

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
        """Add membership relationships between PLEXOS components.

        Memberships define parent-child relationships between components
        through collections (e.g., generators belonging to nodes).
        """
        assert self.db is not None

        system_class_id = self.db.get_class_id(ClassEnum.System)
        membership_query = f"SELECT * from t_membership where parent_class_id <> {system_class_id}"

        for membership_dict in self.db._db.iter_dicts(membership_query):
            parent_object_id = membership_dict["parent_object_id"]
            child_object_id = membership_dict["child_object_id"]
            membership_id = membership_dict["membership_id"]
            collection_id = membership_dict["collection_id"]

            # Get collection name
            collection_name = self._get_collection_name(collection_id)
            if collection_name is None:
                continue

            # Get collection enum
            collection_enum = self._get_collection_enum(collection_name)
            if collection_enum is None:
                continue

            # Find parent and child components
            parent_object = self._component_cache.get(parent_object_id)
            child_object = self._component_cache.get(child_object_id)

            if not parent_object or not child_object:
                logger.trace("Skip collection {} - missing parent or child", collection_name)
                continue

            # Create and add membership
            self.system.add_supplemental_attribute(
                child_object,
                PLEXOSMembership(
                    membership_id=membership_id,
                    parent_object=parent_object,
                    collection=collection_enum,
                ),
            )

    def _get_collection_name(self, collection_id: int) -> str | None:
        """Get collection name from collection ID.

        Parameters
        ----------
        collection_id : int
            The collection ID to lookup

        Returns
        -------
        str | None
            Collection name with spaces removed, or None if not found
        """
        collection_name_result = self.db._db.fetchone(
            "SELECT name from t_collection where collection_id = ?",
            (collection_id,),
        )
        if collection_name_result is None:
            logger.debug("Collection not found for ID {}", collection_id)
            return None

        # fetchone returns tuple[Any, ...], we know first element is the name string
        collection_name: str = collection_name_result[0]
        return collection_name.replace(" ", "")

    def _get_collection_enum(self, collection_name: str) -> CollectionEnum | None:
        """Get CollectionEnum from collection name.

        Parameters
        ----------
        collection_name : str
            The collection name to lookup

        Returns
        -------
        CollectionEnum | None
            The collection enum or None if not found
        """
        try:
            return CollectionEnum[collection_name]
        except KeyError:
            logger.warning(
                "Collection={} not found on `CollectionEnum`. Skipping it.",
                collection_name,
            )
            return None

    def _create_component(self, obj_type: str, db_rows: list[dict[str, Any]]) -> PLEXOSObject | None:
        """Create a PLEXOS component from database rows.

        Parameters
        ----------
        obj_type : str
            The type of object to create
        db_rows : list[dict[str, Any]]
            Database rows for this object

        Returns
        -------
        PLEXOSObject | None
            The created component or None if not supported
        """
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
        """Process and attach properties to a component.

        Parameters
        ----------
        component : PLEXOSObject
            The component to attach properties to
        db_rows : list[dict[str, Any]]
            Database rows containing property data
        """
        for prop_name, rows in itertools.groupby(db_rows, key=itemgetter("property")):
            if prop_name is None:
                continue

            # Find the corresponding field name for this property alias
            field_name = get_field_name_by_alias(component, prop_name)
            if field_name is None:
                logger.warning(
                    "Property={} not supported for model={}. Skipping it.",
                    prop_name,
                    type(component).__name__,
                )
                continue

            # Create property from records and attach to component
            prop_records = list(rows)
            property_value = PLEXOSPropertyValue.from_records(prop_records)
            setattr(component, field_name, property_value)

            # Register time series references if needed
            if property_value.has_datafile() or property_value.has_variable():
                self._register_time_series_reference(component, field_name, property_value)
        return

    def _register_time_series_reference(
        self, component: PLEXOSObject, field_name: str, property: PLEXOSPropertyValue
    ) -> None:
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

    def _apply_action(self, base_value: float, new_value: float, action: str | None) -> float:
        """Apply an action operation to combine values.

        Parameters
        ----------
        base_value : float
            The current/base value
        new_value : float
            The new value to apply
        action : str | None
            The action to perform: "=", "*", "+", "-", "/"

        Returns
        -------
        float
            The result of applying the action
        """
        if action == "*":
            return base_value * new_value
        elif action == "+":
            return base_value + new_value
        elif action == "-":
            return base_value - new_value
        elif action == "/" and new_value != 0:
            return base_value / new_value
        else:  # "=" or unknown - just return new value
            return new_value
