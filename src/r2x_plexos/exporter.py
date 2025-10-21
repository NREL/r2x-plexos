"""Export PLEXOS system to XML."""

from typing import Any, cast

from loguru import logger
from plexosdb import ClassEnum, PlexosDB
from plexosdb.enums import get_default_collection

from r2x_core import BaseExporter, DataStore, Err, ExporterError, Ok, Result
from r2x_plexos.utils_simulation import (
    build_plexos_simulation,
    get_default_simulation_config,
    ingest_simulation_to_plexosdb,
)

from .config import PLEXOSConfig
from .models import PLEXOSDatafile, PLEXOSHorizon, PLEXOSMembership, PLEXOSModel, PLEXOSObject
from .utils_exporter import get_component_category
from .utils_mappings import PLEXOS_TYPE_MAP_INVERTED

NESTED_ATTRIBUTES = {"ext", "bus", "services"}
DEFAULT_XML_TEMPLATE = "master_9.2R6_btu.xml"


class PLEXOSExporter(BaseExporter):
    """PLEXOS XML exporter."""

    def __init__(
        self,
        *args: Any,
        data_store: DataStore | None = None,
        plexos_scenario: str = "default",
        xml_fname: str | None = None,
        exclude_defaults: bool = True,
        db: PlexosDB | None = None,  # Allow passing existing DB for testing
        **kwargs: Any,
    ) -> None:
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

        # Workflow 1: Existing Database - Skip if both models and horizons exist
        if existing_models and existing_horizons:
            logger.info(
                f"Using existing database configuration: "
                f"{len(existing_models)} model(s), {len(existing_horizons)} horizon(s)"
            )
            return Ok(None)

        # Workflow 2: New Database - Create simulation from user configuration
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
                continue

            class_enum = PLEXOS_TYPE_MAP_INVERTED.get(cast(type[PLEXOSObject], component_type))
            if not class_enum:
                logger.warning("No ClassEnum mapping for {}, skipping.", type(component_type).__name__)
                continue

            components = list(self.system.get_components(component_type))
            if not components:
                continue

            logger.debug(f"Adding {len(components)} {component_type.__name__} components")

            # Sort by category first (groupby requires sorted data)
            components.sort(key=get_component_category)  # type: ignore[arg-type]

            # Group components by category and add each group
            for category, group in groupby(components, key=get_component_category):
                names = [comp.name for comp in group]
                self.db.add_objects(class_enum, *names, category=category)

        return Ok(None)

    def postprocess_export(self) -> Result[None, ExporterError]:
        """Add properties and memberships to the database.

        This method:
        1. Adds component properties using bulk insert from system.to_records()
        2. Adds component memberships (relationships between components)

        Components without properties (PLEXOSDatafile, PLEXOSMembership) are filtered out.
        """
        logger.info("Adding properties and memberships")

        self._add_component_properties()
        self._add_memberships()

        return Ok(None)

    def export_time_series(self) -> Result[None, ExporterError]:
        """Export components to db."""
        logger.info("Exporting time series to CSV files")

        return Ok(None)

    def validate_export(self) -> Result[None, ExporterError]:
        """Validate the export (placeholder for future validation logic)."""
        return Ok(None)

    def _add_component_properties(self) -> None:
        """Add properties for components.

        Skips configuration objects that don't have properties:
        - PLEXOSModel, PLEXOSHorizon: only have attributes, not properties
        - PLEXOSDatafile, PLEXOSMembership: configuration objects
        """
        skip_types = {PLEXOSModel, PLEXOSHorizon, PLEXOSDatafile, PLEXOSMembership}

        for component_type in self.system.get_component_types():
            if component_type in skip_types:
                continue

            class_enum = PLEXOS_TYPE_MAP_INVERTED.get(cast(type[PLEXOSObject], component_type))
            if not class_enum:
                continue

            records = list(self.system.to_records(component_type, exclude_defaults=self.exclude_defaults))
            if not records:
                continue

            logger.debug(f"Adding properties for {len(records)} {component_type.__name__} components")

            collection = get_default_collection(class_enum)
            for i in range(0, len(records), 1000):
                chunk = records[i : i + 1000]
                self.db.add_properties_from_records(
                    chunk,
                    object_class=class_enum,
                    parent_class=ClassEnum.System,
                    collection=collection,
                    scenario=self.plexos_scenario,
                )

    def _add_memberships(self) -> None:
        """Add membership relationships to the database.

        Memberships are stored as supplemental attributes attached to child components.
        Each membership defines a parent_object and collection, while the child is the
        component the supplemental attribute is attached to.
        """
        added_count = 0

        # Iterate through all component types to find their memberships
        for component_type in self.system.get_component_types():
            # Skip types that don't participate in memberships
            if component_type in {PLEXOSModel, PLEXOSHorizon, PLEXOSDatafile}:
                continue

            for child_object in self.system.get_components(component_type):
                # Get all memberships attached to this component (child)
                memberships = self.system.get_supplemental_attributes_with_component(
                    child_object, PLEXOSMembership
                )

                for membership in memberships:
                    if not membership.parent_object:
                        continue

                    # Skip System memberships (already created by add_objects)
                    if membership.parent_object.name == "System":
                        continue

                    parent_class = PLEXOS_TYPE_MAP_INVERTED.get(type(membership.parent_object))
                    child_class = PLEXOS_TYPE_MAP_INVERTED.get(cast(type[PLEXOSObject], type(child_object)))

                    if not parent_class or not child_class:
                        continue

                    # Skip memberships without collection
                    if not membership.collection:
                        continue

                    try:
                        parent_object_id = self.db.get_object_id(parent_class, membership.parent_object.name)
                        child_object_id = self.db.get_object_id(child_class, child_object.name)

                        record = {
                            "parent_class_id": self.db.get_class_id(parent_class),
                            "parent_object_id": parent_object_id,
                            "collection_id": self.db.get_collection_id(
                                membership.collection,
                                parent_class_enum=parent_class,
                                child_class_enum=child_class,
                            ),
                            "child_class_id": self.db.get_class_id(child_class),
                            "child_object_id": child_object_id,
                        }

                        self.db.add_memberships_from_records([record])
                        added_count += 1
                    except Exception as e:
                        logger.debug(
                            f"Failed to add membership {membership.parent_object.name} -> {child_object.name}: {e}"
                        )
                        continue

        if added_count > 0:
            logger.info(f"Added {added_count} memberships")
        else:
            logger.warning("No memberships were added")
