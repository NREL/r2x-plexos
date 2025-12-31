"""Export PLEXOS system to XML."""

import os
from itertools import groupby
from pathlib import Path
from typing import Any, cast

from loguru import logger
from plexosdb import ClassEnum, PlexosDB
from plexosdb.enums import get_default_collection

from r2x_core import BaseExporter, DataStore, Err, ExporterError, Ok, Result

from .config import PLEXOSConfig
from .models import PLEXOSDatafile, PLEXOSHorizon, PLEXOSMembership, PLEXOSModel, PLEXOSObject
from .models.property import PLEXOSPropertyValue
from .utils_exporter import (
    export_time_series_csv,
    generate_csv_filename,
    get_output_directory,
)
from .utils_mappings import PLEXOS_TYPE_MAP_INVERTED
from .utils_simulation import (
    build_plexos_simulation,
    get_default_simulation_config,
    ingest_simulation_to_plexosdb,
)

NESTED_ATTRIBUTES = {"ext", "bus", "services"}
DEFAULT_XML_TEMPLATE = "master_9.2R6_btu.xml"


class PLEXOSExporter(BaseExporter):
    """PLEXOS XML exporter."""

    def __init__(
        self,
        *args: Any,
        data_store: DataStore | None = None,
        plexos_scenario: str = "default",
        output_path: str | None = None,
        xml_fname: str | None = None,
        exclude_defaults: bool = True,
        db: PlexosDB | None = None,  # Allow passing existing DB for testing
        **kwargs: Any,
    ) -> None:
        """Start exporter."""
        self.exclude_defaults = exclude_defaults
        if not exclude_defaults:
            logger.info("Including default values while populating PLEXOS database")

        super().__init__(*args, **kwargs)
        logger.debug("Starting {} using configuration {}", type(self).__name__, self.config)

        if not isinstance(self.config, PLEXOSConfig):
            msg = (
                f"Config is of type {type(self.config)}. "
                f"It should be type of `{type(PLEXOSConfig).__name__}`."
            )
            raise TypeError(msg)
        self.config: PLEXOSConfig
        self.output_path = output_path
        self.plexos_scenario = plexos_scenario or self.config.model_name

        # Use provided DB if available (for testing), otherwise create from XML
        if db is not None:
            self.db = db
            logger.debug("Using provided PlexosDB instance")
        else:
            if not xml_fname and not (xml_fname := self.config.template):
                xml_fname = self.config.get_config_path().joinpath(DEFAULT_XML_TEMPLATE)
                logger.debug("Using default XML template")

            self.db = PlexosDB.from_xml(xml_path=xml_fname)

        if not self.db.check_object_exists(ClassEnum.Scenario, plexos_scenario):
            self.db.add_scenario(plexos_scenario)

    def setup_configuration(self) -> Result[None, ExporterError]:
        """Set up simulation configuration (models, horizons, and simulation configs).

        This method supports two workflows:

        1. **Existing Database Workflow**: If the database already contains models and horizons
           (e.g., loaded from an existing XML template), the simulation configuration is skipped.
           This allows users to work with pre-configured databases without modification.

        2. **New Database Workflow**: If the database is new (no models or horizons exist),
           this method creates the complete simulation structure from user configuration:
           - Models and horizons based on horizon_year and resolution
           - Model-horizon memberships
           - Simulation configuration objects (Performance, Production, etc.)

        Returns
        -------
        Result[None, str]
            Ok(None) if successful, Err with error message if failed
        """
        logger.info("Setting up simulation configuration")

        existing_models = self.db.list_objects_by_class(ClassEnum.Model)
        existing_horizons = self.db.list_objects_by_class(ClassEnum.Horizon)

        if existing_models and existing_horizons:
            logger.info(
                f"Using existing database configuration: "
                f"{len(existing_models)} model(s), {len(existing_horizons)} horizon(s)"
            )
            return Ok(None)

        logger.info("New database detected - creating simulation configuration from user input")
        simulation_config_dict = getattr(self.config, "simulation_config", None)
        if simulation_config_dict is None:
            logger.debug("Using default simulation configuration")
            simulation_config_dict = get_default_simulation_config()

        horizon_year = getattr(self.config, "horizon_year", None) or getattr(
            self.config, "reference_year", None
        )
        if horizon_year is None:
            return Err(
                ExporterError(
                    "New database requires 'horizon_year' (or 'reference_year') in config "
                    "to create simulation configuration"
                )
            )

        sim_config = {
            "horizon_year": horizon_year,
            "resolution": getattr(self.config, "resolution", "1D"),
        }

        logger.info(f"Building simulation for year {horizon_year}")

        simulation_result = build_plexos_simulation(
            config=sim_config,
            defaults=None,
            simulation_config=simulation_config_dict,
        )

        if simulation_result.is_err():
            assert isinstance(simulation_result, Err)
            return Err(ExporterError(f"Failed to build simulation: {simulation_result.error}"))

        build_result = simulation_result.unwrap()
        logger.info(
            f"Built simulation: {len(build_result.models)} model(s), "
            f"{len(build_result.horizons)} horizon(s), "
            f"{len(build_result.memberships)} membership(s)"
        )

        ingest_result = ingest_simulation_to_plexosdb(self.db, build_result, validate=False)
        if ingest_result.is_err():
            assert isinstance(ingest_result, Err)
            return Err(ExporterError(f"Failed to ingest simulation: {ingest_result.error}"))

        ingest_info = ingest_result.unwrap()
        sim_config_count = len(ingest_info.get("simulation_objects", []))
        logger.info(
            f"Successfully created simulation configuration: "
            f"{len(ingest_info['models'])} model(s), "
            f"{len(ingest_info['horizons'])} horizon(s), "
            f"{sim_config_count} simulation config object(s)"
        )

        return Ok(None)

    def prepare_export(self) -> Result[None, ExporterError]:
        """Add component objects to the database.

        This method bulk inserts component objects (generators, nodes, etc.) into the database.
        It skips simulation configuration objects (Model, Horizon) as those are handled in setup_configuration().
        It does NOT add properties or memberships - those are added in postprocess_export().
        """
        from itertools import groupby

        logger.info("Adding components to database")

        # Skip these types - they're either config objects or don't get added as objects
        skip_types = {PLEXOSModel, PLEXOSHorizon, PLEXOSDatafile, PLEXOSMembership}

        for component_type in self.system.get_component_types():
            if component_type in skip_types:
                logger.debug(f"Skipping component type: {component_type.__name__}")
                continue

            class_enum = PLEXOS_TYPE_MAP_INVERTED.get(cast(type[PLEXOSObject], component_type))
            if not class_enum:
                logger.warning("No ClassEnum mapping for {}, skipping.", type(component_type).__name__)
                continue

            components = list(self.system.get_components(component_type))
            if not components:
                logger.debug(f"No components found for type: {component_type.__name__}, skipping.")
                continue

            logger.debug(f"Adding {len(components)} {component_type.__name__} components")

            # Sort components by category to group them
            components.sort(key=lambda x: x.category or "")  # type: ignore

            # Group components by category and add each group in one call
            for category, group in groupby(components, key=lambda x: x.category or ""):  # type: ignore
                names = [comp.name for comp in group]
                try:
                    if category:
                        self.db.add_objects(class_enum, *names, category=category)
                    else:
                        self.db.add_objects(class_enum, *names)
                except KeyError as e:
                    logger.error(f"Failed to add {class_enum} objects with category '{category}': {e}")
                    logger.debug(f"Component type: {component_type.__name__}, names: {names[:5]}")
                    raise

        return Ok(None)

    def postprocess_export(self) -> Result[None, ExporterError]:
        """Add properties and memberships to the database.

        This method:
        1. Adds component properties using bulk insert from system.to_records()
        2. Adds component memberships (relationships between components)
        3. Exports time series to CSV files

        Components without properties (PLEXOSDatafile, PLEXOSMembership) are filtered out.
        """
        logger.info("Adding properties and memberships")

        self._add_datafile_objects_to_db()

        self._add_component_properties()
        self._add_memberships()

        # Export time series before finalizing XML
        logger.info("Exporting time series")
        ts_result = self.export_time_series()
        if isinstance(ts_result, Err):
            logger.error("Failed to export time series: {}", ts_result.error)
            return ts_result

        output_dir = get_output_directory(self.config, self.system, output_path=self.output_path)
        base_folder = Path(self.output_path) if self.output_path else output_dir.parent
        xml_filename = f"{self.config.model_name}.xml"
        xml_path = base_folder / xml_filename

        logger.info(f"Exporting XML to {xml_path}")
        self.db.to_xml(xml_path)

        return Ok(None)

    def export_time_series(self) -> Result[None, ExporterError]:
        """Export time series to CSV files and update property references.

        Returns
        -------
        Result[None, ExporterError]
            Ok(None) on success, Err(ExporterError) on failure
        """
        # Get ALL components with time series, not just PLEXOSObject
        all_components_with_ts = []
        for component_type in self.system.get_component_types():
            components = list(
                self.system.get_components(
                    component_type, filter_func=lambda c: self.system.has_time_series(c)
                )
            )
            all_components_with_ts.extend(components)

        if not all_components_with_ts:
            logger.warning("No components with time series found")
            return Ok(None)

        logger.debug(f"Found {len(all_components_with_ts)} components with time series")

        ts_metadata: list[tuple[Any, Any]] = []
        for component in all_components_with_ts:
            ts_keys = self.system.list_time_series_keys(component)
            ts_metadata.extend((component, ts_key) for ts_key in ts_keys)

        logger.debug(f"Found {len(ts_metadata)} time series keys total")

        def _grouping_key(item: tuple[Any, Any]) -> tuple[str, tuple[tuple[str, Any], ...]]:
            """Sort by component_type."""
            _, ts_key = item
            return (ts_key.name, tuple(sorted(ts_key.features.items())))

        ts_metadata_sorted = sorted(ts_metadata, key=_grouping_key)

        csv_filepaths: list[Path] = []
        output_dir = get_output_directory(self.config, self.system, output_path=self.output_path)

        for group_key, group_items in groupby(ts_metadata_sorted, key=_grouping_key):
            field_name, features_tuple = group_key
            metadata_dict = dict(features_tuple)
            group_list = list(group_items)

            first_component = group_list[0][0]
            component_class = type(first_component).__name__

            filename = generate_csv_filename(field_name, component_class, metadata_dict)
            filepath = output_dir / filename
            csv_filepaths.append(filepath)

            time_series_data: list[tuple[str, Any]] = []
            for component, ts_key in group_list:
                ts = self.system.get_time_series_by_key(component, ts_key)
                time_series_data.append((component.name, ts))

            result = export_time_series_csv(filepath, time_series_data)

            if result.is_err():
                assert isinstance(result, Err)
                logger.error(f"Failed to export time series: {result.error}")
                return Err(ExporterError(f"Time series export failed: {result.error}"))

        logger.info(f"Exported {len(csv_filepaths)} time series files to {output_dir}")

        return Ok(None)

    def validate_export(self) -> Result[None, ExporterError]:
        """Validate the export (placeholder for future validation logic)."""
        return Ok(None)

    def _add_component_properties(self) -> None:
        """Add properties for all components, including DataFile objects first."""
        logger.info("Adding component properties...")

        # Add properties for PlexosDataFile objects firstb
        for component in self.system.get_components(PLEXOSDatafile):
            datafile_text = component.name

            self.db.add_property(
                ClassEnum.DataFile,
                object_name=component.name,
                name="Filename",
                value=0,
                datafile_text=datafile_text,
            )
            logger.debug(f"Added Filename property for DataFile: {component.name}")

        # Add properties for each component type except Datafile and Membership
        skip_types = {PLEXOSModel, PLEXOSHorizon, PLEXOSDatafile, PLEXOSMembership}

        for component_type in self.system.get_component_types():
            if component_type in skip_types:
                continue

            class_enum = PLEXOS_TYPE_MAP_INVERTED.get(cast(type[PLEXOSObject], component_type))
            if not class_enum:
                continue

            collection = get_default_collection(class_enum)
            plexos_records = []

            for comp in self.system.get_components(component_type):
                aliased_dict = comp.model_dump(by_alias=True, exclude_defaults=self.exclude_defaults)
                metadata_fields = {"name", "category", "uuid", "label", "description", "object_id"}
                properties: dict[str, Any] = {}
                for k, v in aliased_dict.items():
                    if k in metadata_fields or v is None:
                        continue
                    if isinstance(v, (int, float, str, bool)):
                        properties[k] = {"value": v, "band": 1}
                    elif isinstance(v, dict) and "text" in v:
                        properties[k] = v
                if properties:
                    plexos_record = {"name": comp.name, "properties": properties}
                    plexos_records.append(plexos_record)

            if not plexos_records:
                continue

            logger.debug(f"Adding properties for {len(plexos_records)} {component_type.__name__} components")
            self.db.add_properties_from_records(
                plexos_records,
                object_class=class_enum,
                parent_class=ClassEnum.System,
                collection=collection,
                scenario=self.plexos_scenario,
            )

    def _add_memberships(self) -> None:
        """Add membership relationships to the database."""
        memberships = list(self.system.get_supplemental_attributes(PLEXOSMembership))

        if not memberships:
            logger.warning("No memberships found in system")
            return

        records = []
        seen_memberships = set()  # Track unique (parent_object_id, collection_id, child_object_id)
        duplicate_count = 0

        for membership in memberships:
            if not membership.parent_object or not membership.child_object:
                logger.debug("Skipping membership with missing parent or child object")
                continue

            parent_class = PLEXOS_TYPE_MAP_INVERTED.get(type(membership.parent_object))
            child_class = PLEXOS_TYPE_MAP_INVERTED.get(type(membership.child_object))

            if not parent_class or not child_class or not membership.collection:
                logger.info("Skipping membership with unmapped classes or missing collection")
                continue

            if parent_class in (ClassEnum.Model, ClassEnum.Horizon) or child_class in (
                ClassEnum.Model,
                ClassEnum.Horizon,
            ):
                continue

            try:
                parent_object_id = self.db.get_object_id(parent_class, membership.parent_object.name)
                child_object_id = self.db.get_object_id(child_class, membership.child_object.name)
                collection_id = self.db.get_collection_id(
                    membership.collection,
                    parent_class_enum=parent_class,
                    child_class_enum=child_class,
                )

                # Check for duplicates based on the unique constraint
                membership_key = (parent_object_id, collection_id, child_object_id)

                if membership_key in seen_memberships:
                    duplicate_count += 1
                    continue

                seen_memberships.add(membership_key)

                record = {
                    "parent_class_id": self.db.get_class_id(parent_class),
                    "parent_object_id": parent_object_id,
                    "collection_id": collection_id,
                    "child_class_id": self.db.get_class_id(child_class),
                    "child_object_id": child_object_id,
                }
                records.append(record)

            except Exception:
                logger.debug("Failed to process membership: {}", membership)
                continue

        if not records:
            logger.warning("No valid membership records to add")
            return

        self.db.add_memberships_from_records(records)
        logger.success(f"Successfully added {len(records)} memberships")

    def _create_datafile_objects(self) -> None:
        """Create DataFile objects for the CSVs that are being created."""
        logger.info("Creating DataFile objects...")

        output_path = self.output_path or "."
        time_series_dir = os.path.join(output_path, "Data")
        if not os.path.exists(time_series_dir):
            logger.info(f"No time series directory found at {time_series_dir}")
            return

        for filename in os.listdir(time_series_dir):
            if filename.endswith(".csv"):
                file_path = os.path.join("Data", filename)
                datafile_obj = PLEXOSDatafile(
                    name=filename.removesuffix(".csv"),
                    filename=PLEXOSPropertyValue.from_dict({"datafile_name": file_path}),
                )
                if not self.system.has_component(datafile_obj):
                    self.system.add_component(datafile_obj)

    def _add_datafile_objects_to_db(self) -> None:
        """Add PLEXOSDatafile objects from the system to the database."""
        self._create_datafile_objects()
        datafiles = list(self.system.get_components(PLEXOSDatafile))
        if not datafiles:
            logger.info("No PLEXOSDatafile objects to add to DB.")
            return

        # Add DataFile objects to the DB
        names = [df.name for df in datafiles]
        self.db.add_objects(ClassEnum.DataFile, *names, category="CSV")

        # Assign object IDs to the DataFile objects and their filename property
        for data_file in datafiles:
            object_id = self.db.get_object_id(ClassEnum.DataFile, data_file.name)
            data_file.object_id = object_id
            if data_file.filename is not None and hasattr(data_file.filename, "datafile_id"):
                data_file.filename.datafile_id = object_id

    def _insert_component_tags(self, component_type: type["PLEXOSObject"]) -> None:
        for component in self.system.get_components(
            component_type,
            filter_func=lambda x: self.system.has_time_series(x),
        ):
            attr_list = self.system.list_time_series_keys(component)
            for attr in attr_list:
                attr_value = getattr(component, attr.name)
                if (
                    getattr(attr_value, "datafile_id", None) is not None
                    or getattr(attr_value, "variable_id", None) is not None
                ):
                    object_data_ids = self.db.get_object_data_ids(
                        PLEXOS_TYPE_MAP_INVERTED[component_type],
                        component.name,
                        property_names=component_type.model_fields[attr.name].alias,
                    )

                    for object_data_id in object_data_ids:
                        if getattr(attr_value, "datafile_id", None) is not None:
                            self._insert_tag(attr_value.datafile_id, object_data_id)
                        if getattr(attr_value, "variable_id", None) is not None:
                            self._insert_tag(attr_value.variable_id, object_data_id)

    def _insert_tag(self, object_id: int, data_id: int) -> None:
        query = "INSERT INTO t_tag (object_id, data_id) VALUES (?, ?)"
        self.db._db.execute(query, (object_id, data_id))
