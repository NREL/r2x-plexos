"""Export PLEXOS system to XML."""

import os
from datetime import datetime, timedelta
from itertools import groupby
from pathlib import Path
from typing import Any, cast

from loguru import logger
from plexosdb import ClassEnum, PlexosDB
from plexosdb.enums import CollectionEnum, get_default_collection

from r2x_core import Err, Ok, Plugin, Result

from .models import (
    PLEXOSDatafile,
    PLEXOSGenerator,
    PLEXOSHorizon,
    PLEXOSMembership,
    PLEXOSModel,
    PLEXOSObject,
    PLEXOSRegion,
    PLEXOSReserve,
    PLEXOSStorage,
)
from .models.property import PLEXOSPropertyValue
from .plugin_config import PLEXOSConfig
from .utils_exporter import (
    build_metadata_suffix,
    export_time_series_csv,
    generate_csv_filename,
    get_hydro_budget_property_name,
    get_output_directory,
)
from .utils_mappings import (
    FIXED_TS_PROP,
    GENERATOR_TO_STORAGE_TS_PROPERTY_MAP,
    GENERATOR_TS_PROPERTY_MAP,
    PLEXOS_TYPE_MAP_INVERTED,
    STORAGE_TO_GENERATOR_TS_PROPERTY_MAP,
    STORAGE_TS_PROPERTY_MAP,
)
from .utils_simulation import (
    build_plexos_simulation,
    get_default_simulation_config,
    get_enum_from_string,
    ingest_simulation_to_plexosdb,
)

NESTED_ATTRIBUTES = {"ext", "bus", "services"}
DEFAULT_XML_TEMPLATE = "master_10.0R2_btu.xml"
XML_TEMPLATE_MAP = {
    "PLEXOS9.2": "master_9.2R6_btu.xml",
    "PLEXOS10.0": "master_10.0R2_btu.xml",
}
BATCH_SIZE = 500
FLOW_CLIP_MEMO_TEXT = "Setting fixed value of ±99999 to flows greater/less than ±100000"


class PLEXOSExporter(Plugin[PLEXOSConfig]):
    """PLEXOS XML exporter."""

    def __init__(self) -> None:
        """Initialize the exporter with minimal state.

        Notes
        -----
        Actual initialization happens after context is set via from_context().
        """
        super().__init__()

        self.should_export_time_series: bool = True
        self.exclude_defaults: bool = True
        self.output_path: str | None = None
        self.solve_year: int | None = None
        self.weather_year: int | None = None
        self.plexos_scenario: str = "default"
        self.db: PlexosDB | None = None
        self.defaults: dict[str, Any] = PLEXOSConfig.load_defaults()

    def on_export(self) -> Result[None, str]:
        """Initialize the exporter after context is set.

        Returns
        -------
        Result[None, str]
            Ok(None) on success, Err with error message on failure
        """
        try:
            logger.debug("Starting {} using configuration {}", type(self).__name__, self.config)

            if not isinstance(self.config, PLEXOSConfig):
                msg = (
                    f"Config is of type {type(self.config)}. "
                    f"It should be type of `{type(PLEXOSConfig).__name__}`."
                )
                return Err(msg)

            self._sync_runtime_options_from_config()

            self.plexos_scenario = self.plexos_scenario or self.config.model_name

            if self.db is None:
                xml_fname = self._resolve_template_path()
                self.db = PlexosDB.from_xml(xml_path=xml_fname)

            if not self.db.check_object_exists(ClassEnum.Scenario, self.plexos_scenario):
                self.db.add_scenario(self.plexos_scenario)

            setup_result = self.setup_configuration()
            if setup_result.is_err():
                return setup_result

            self._add_reports()

            prepare_result = self.prepare_export()
            if prepare_result.is_err():
                return prepare_result

            postprocess_result = self.postprocess_export()
            if postprocess_result.is_err():
                return postprocess_result

            return Ok(None)
        except Exception as e:
            return Err(f"Export failed: {e}")

    def _sync_runtime_options_from_config(self) -> None:
        """Sync runtime attributes used internally by exporter methods."""
        configured_output = getattr(self.config, "output_path", None)
        if configured_output:
            self.output_path = str(configured_output)

        # Keep explicit runtime overrides (e.g., exporter.weather_year=...) and
        # only hydrate from config when runtime value was not set.
        if self.weather_year is None:
            self.weather_year = getattr(self.config, "weather_year", None)

    def _build_xml_filename(self) -> str:
        """Build XML filename from model, horizon year, and weather year."""
        horizon_year = (
            self.solve_year if self.solve_year is not None else getattr(self.config, "horizon_year", None)
        )
        weather_year = (
            self.weather_year if self.weather_year is not None else getattr(self.config, "weather_year", None)
        )
        metadata = {
            "model_name": self.config.model_name,
            "horizon_year": horizon_year,
            "weather_year": weather_year,
        }
        return f"{build_metadata_suffix(metadata)}.xml"

    def setup_configuration(self) -> Result[None, str]:
        """
        Set up the simulation configuration in the PlexosDB.

        Loads static model and horizon definitions from JSON files, merges them,
        and builds the simulation configuration for the specified year and resolution.
        Also applies simulation configuration objects (Performance, Production, etc.)
        if provided. Returns Ok(None) on success or Err with an error message.

        Returns
        -------
        Result[None, str]
            Ok(None) if setup is successful, Err(error message) otherwise.
        """
        if self.db is None:
            return Err("Database not initialized")

        logger.info("Setting up simulation configuration")

        static_model_defaults = PLEXOSConfig.load_static_models()
        static_horizon_defaults = PLEXOSConfig.load_static_horizons()
        defaults = {**static_model_defaults, **static_horizon_defaults}

        simulation_config_dict = getattr(self.config, "simulation_config", None)
        if simulation_config_dict is None:
            logger.debug("Using default simulation configuration")
            simulation_config_dict = get_default_simulation_config()

        horizon_year = getattr(self.config, "horizon_year", None)
        if horizon_year is None:
            return Err("New database requires 'horizon_year' in config to create simulation configuration")

        sim_config = {
            "horizon_year": horizon_year,
            "resolution": getattr(self.config, "resolution", "1D"),
        }

        logger.info(f"Building simulation for year {horizon_year}")
        simulation_result = build_plexos_simulation(
            config=sim_config,
            defaults=defaults,
            simulation_config=simulation_config_dict,
        )

        if simulation_result.is_err():
            assert isinstance(simulation_result, Err)
            return Err(f"Failed to build simulation: {simulation_result.error}")

        build_result = simulation_result.unwrap()
        logger.info(
            f"Built simulation: {len(build_result.models)} model(s), "
            f"{len(build_result.horizons)} horizon(s), "
            f"{len(build_result.memberships)} membership(s)"
        )

        ingest_result = ingest_simulation_to_plexosdb(
            self.db, build_result, validate=False, scenario_name=self.plexos_scenario
        )
        if ingest_result.is_err():
            assert isinstance(ingest_result, Err)
            return Err(f"Failed to ingest simulation: {ingest_result.error}")

        ingest_info = ingest_result.unwrap()
        sim_config_count = len(ingest_info.get("simulation_objects", []))
        logger.info(
            f"Successfully created simulation configuration: "
            f"{len(ingest_info['models'])} model(s), "
            f"{len(ingest_info['horizons'])} horizon(s), "
            f"{sim_config_count} simulation config object(s)"
        )

        return Ok(None)

    def _add_objects_safe(
        self,
        class_enum: ClassEnum,
        names: list[str],
        category: str | None = None,
    ) -> None:
        """Add objects to the database, avoiding duplicates and ensuring collection membership."""
        import uuid as _uuid

        assert self.db is not None
        if not names:
            return

        names = list(set(names))
        existing = set(self.db.list_objects_by_class(class_enum))
        new_names = [n for n in names if n not in existing]
        if not new_names:
            return

        category_str = category or "-"
        category_id = self.db.add_category(class_enum, category_str)
        class_id = self.db.get_class_id(class_enum)

        collection_enum = get_default_collection(class_enum)
        parent_class_id = self.db.get_class_id(ClassEnum.System)
        parent_object_id = self.db.get_object_id(ClassEnum.System, "System")
        collection_id = self.db.get_collection_id(
            collection_enum, parent_class_enum=ClassEnum.System, child_class_enum=class_enum
        )

        insert_params = [(name, class_id, category_id, str(_uuid.uuid4())) for name in new_names]
        self.db._db.executemany(
            "INSERT INTO t_object(name, class_id, category_id, GUID) VALUES(?,?,?,?)",
            insert_params,
        )

        membership_records = []
        for batch in self._chunked(new_names, 900):
            placeholders = ",".join("?" for _ in batch)
            rows = self.db._db.fetchall(
                f"SELECT object_id FROM t_object WHERE class_id=? AND name IN ({placeholders})",
                (class_id, *batch),
            )
            for (object_id,) in rows:
                membership_records.append(
                    {
                        "parent_class_id": parent_class_id,
                        "parent_object_id": parent_object_id,
                        "collection_id": collection_id,
                        "child_class_id": class_id,
                        "child_object_id": object_id,
                    }
                )

        if membership_records:
            self.db.add_memberships_from_records(membership_records)

    def _chunked(self, items: list[str], size: int = 900) -> list[list[str]]:
        """Yield successive chunks of items of given size."""
        return [items[i : i + size] for i in range(0, len(items), size)]

    def prepare_export(self) -> Result[None, str]:
        """Add component objects to the database.

        This method bulk inserts component objects (generators, nodes, etc.) into the database.
        It skips simulation configuration objects (Model, Horizon) as those are handled in setup_configuration().
        It does NOT add properties or memberships - those are added in postprocess_export().
        """
        from itertools import groupby

        if self.db is None:
            return Err("Database not initialized")

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

            # Fetch all existing objects of this class once to avoid duplicate inserts
            existing = set(self.db.list_objects_by_class(class_enum))

            # Group components by category and add each group in one call
            for category, group in groupby(components, key=lambda x: x.category or ""):  # type: ignore
                names = [comp.name for comp in group]

                # Filter out names that already exist in the database
                new_names = [n for n in names if n not in existing]

                if not new_names:
                    logger.debug(f"All {len(names)} {class_enum.name} objects already exist, skipping.")
                    continue

                if len(new_names) < len(names):
                    logger.debug(
                        f"Skipping {len(names) - len(new_names)} existing {class_enum.name} objects."
                    )

                logger.debug(
                    f"Adding {len(new_names)} objects for category='{category}' class={class_enum.name}"
                )

                try:
                    self._add_objects_safe(class_enum, new_names, category=category or None)
                except Exception as e:
                    logger.error(f"Failed to add {class_enum} objects with category '{category}': {e}")
                    logger.debug(f"Component type: {component_type.__name__}, names: {names[:5]}")
                    raise

            self.db._db.execute(f"UPDATE t_class SET is_enabled=1 WHERE t_class.name='{class_enum}'")
            logger.debug(f"Enabled class: {class_enum.name}")

        return Ok(None)

    def postprocess_export(self) -> Result[None, str]:
        """Add properties and memberships to the database.

        This method:
        1. Adds component properties using bulk insert from system.to_records()
        2. Adds component memberships (relationships between components)
        3. Exports time series to CSV files

        Components without properties (PLEXOSDatafile, PLEXOSMembership) are filtered out.
        """
        if self.db is None:
            return Err("Database not initialized")

        # Define base and output diectories
        output_dir = get_output_directory(self.config, self.system, output_path=self.output_path)
        datafile_prefix = output_dir.name
        base_folder = Path(self.output_path) if self.output_path else output_dir.parent

        logger.info("Exporting time series")
        if self.should_export_time_series:
            ts_result = self.export_time_series()
            if isinstance(ts_result, Err):
                logger.error("Failed to export time series: {}", ts_result.error)
                return ts_result

        logger.info("Creating DataFile objects from exported CSVs")
        self._add_component_datafile_objects()

        logger.info("Adding component properties and memberships")
        self._add_component_properties(datafile_prefix=datafile_prefix)
        self._add_component_memberships()

        xml_filename = self._build_xml_filename()
        xml_path = base_folder / xml_filename

        logger.info(f"Exporting XML to {xml_path}")
        self.db.to_xml(xml_path)

        if not self._validate_xml(str(xml_path)):
            logger.error(f"Exported XML at {xml_path} is not valid!")
            return Err(f"Exported XML at {xml_path} is not valid!")
        else:
            logger.success("Exported XML was correctly validated.")

        return Ok(None)

    def _deduplicate_property_records(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Remove duplicate property records before bulk insert.

        `plexosdb.add_properties_from_records` identifies rows by
        `(name, property, value)` for scenario tagging. If this exporter emits
        the same triple multiple times, scenario tag insertion can fail with
        `UNIQUE constraint failed: t_tag.data_id, t_tag.object_id`.

        To keep insertion idempotent and stable, collapse records by that key
        while preserving the richest metadata encountered.
        """
        if not records:
            return records

        def _normalize_value(value: Any) -> Any:
            # PLEXOS warnings can be triggered by duplicate values written with
            # different Python types (e.g. 23.3 vs "23.3"). Canonicalize these.
            if isinstance(value, str):
                stripped = value.strip()
                if stripped == "":
                    return ""
                lowered = stripped.lower()
                if lowered in {"true", "false"}:
                    return lowered == "true"
                try:
                    return float(stripped)
                except ValueError:
                    return stripped
            return value

        seen: dict[tuple, dict[str, Any]] = {}
        for rec in records:
            key = (
                str(rec.get("name", "")).strip(),
                str(rec.get("property", "")).strip(),
                _normalize_value(rec.get("value")),
            )
            if key not in seen:
                seen[key] = dict(rec)
            else:
                current = seen[key]
                for field in ("band", "timeslice", "date_from", "date_to", "datafile_text"):
                    if current.get(field) is None and rec.get(field) is not None:
                        current[field] = rec[field]

        deduped = list(seen.values())
        dropped = len(records) - len(deduped)
        if dropped > 0:
            logger.debug("Dropped {} duplicate property rows before bulk insert", dropped)
        return deduped

    def _get_required_properties_for_component(self, comp: Any, type_name: str) -> set[str]:
        """Resolve required properties for a component using category-group aware lookup."""
        required_properties = self.defaults.get("required-properties", {})
        category_groups = self.defaults.get("category-groups", {})
        category = getattr(comp, "category", None)

        if category:
            category_norm = str(category).strip().lower().replace("_", "-")
            alias_map = {
                "thermal": "thermal-standard",
                "renewable": "renewable-dispatch",
                "storage": "energy-reservoir-storage",
            }
            category_norm = alias_map.get(category_norm, category_norm)

            category_to_group = {
                str(cat).strip().lower().replace("_", "-"): group_name
                for group_name, categories in category_groups.items()
                for cat in categories
            }
            group_name = category_to_group.get(category_norm)

            if group_name:
                group_key = f"{type_name}.{group_name}"
                if group_key in required_properties:
                    value = required_properties[group_key]
                    if isinstance(value, str):
                        value = required_properties.get(value, [])
                    return set(value)

        if type_name == "PLEXOSGenerator":
            return set(required_properties.get("PLEXOSGenerator.thermal-standard", []))
        if type_name == "PLEXOSStorage":
            return set(required_properties.get("PLEXOSStorage", []))
        if type_name == "PLEXOSRegion":
            return set(required_properties.get("PLEXOSRegion", []))
        return set(required_properties.get(type_name, []))

    def _add_component_properties(self, datafile_prefix: str = "Data") -> None:
        """
        Add properties for all components in the system to the database.

        Uses bulk add_properties_from_records (fast path) with flat records so both
        scalar and serialized PLEXOSPropertyValue payloads are preserved.
        """
        if self.db is None:
            logger.error("Database not initialized")
            return

        logger.info("Adding component properties...")
        datafile_records: list[dict[str, Any]] = []
        for component in self.system.get_components(PLEXOSDatafile):
            relative_path = f"{datafile_prefix}/{component.name}.csv"
            datafile_records.append(
                {
                    "name": component.name,
                    "property": "Filename",
                    "value": 0,
                    "datafile_text": relative_path,
                    "band": 1,
                }
            )

        if datafile_records:
            datafile_records = self._deduplicate_property_records(datafile_records)
            self.db.add_properties_from_records(
                datafile_records,
                object_class=ClassEnum.DataFile,
                parent_class=ClassEnum.System,
                collection=get_default_collection(ClassEnum.DataFile),
                scenario=self.plexos_scenario,
            )

        skip_types = {PLEXOSModel, PLEXOSHorizon, PLEXOSDatafile, PLEXOSMembership}
        metadata_fields = {"name", "category", "uuid", "label", "description", "object_id"}

        # Build once for the generator → storage redirect logic below
        gen_to_storage = self._build_generator_to_storage_map()

        for component_type in self.system.get_component_types():
            if component_type in skip_types:
                continue

            class_enum = PLEXOS_TYPE_MAP_INVERTED.get(cast(type[PLEXOSObject], component_type))
            if not class_enum:
                continue

            collection = get_default_collection(class_enum)
            type_name = component_type.__name__

            all_comps = list(self.system.get_components(component_type))
            fixed_ts_prop: str | None = FIXED_TS_PROP.get(component_type)
            comp_ts_props: dict[str, set[str]] = {}

            for _comp in all_comps:
                if not self.system.has_time_series(_comp):
                    continue
                if fixed_ts_prop is not None:
                    comp_ts_props[_comp.name] = {fixed_ts_prop}
                elif isinstance(_comp, PLEXOSGenerator):
                    sienna_type = (getattr(_comp, "ext", None) or {}).get("sienna_type", "")
                    _props: set[str] = set()
                    for ts_key in self._filter_ts_keys_by_weather_year(
                        self.system.list_time_series_keys(_comp)
                    ):
                        key = ts_key.name.strip().lower().replace(" ", "_")
                        if key == "max_active_power":
                            if sienna_type == "HydroDispatch":
                                _props.add("Load")
                            else:
                                _props.update(["Rating", "Load Subtracter"])
                        elif key == "hydro_budget":
                            resolved_ts = self._resolve_matching_time_series(_comp, ts_key)
                            if resolved_ts is not None:
                                _props.add(get_hydro_budget_property_name(resolved_ts.resolution))
                        elif key in GENERATOR_TO_STORAGE_TS_PROPERTY_MAP:
                            pass
                        else:
                            mapped = GENERATOR_TS_PROPERTY_MAP.get(key)
                            if mapped:
                                _props.add(mapped)
                    if _props:
                        comp_ts_props[_comp.name] = _props
                elif isinstance(_comp, PLEXOSStorage):
                    _props = set()
                    for ts_key in self._filter_ts_keys_by_weather_year(
                        self.system.list_time_series_keys(_comp)
                    ):
                        key = ts_key.name.strip().lower().replace(" ", "_")
                        mapped = STORAGE_TS_PROPERTY_MAP.get(key)
                        if mapped:
                            _props.add(mapped)
                    if _props:
                        comp_ts_props[_comp.name] = _props

            if issubclass(component_type, PLEXOSGenerator):
                for _gen in all_comps:
                    linked_storage = gen_to_storage.get(_gen.name)
                    if not linked_storage or not self.system.has_time_series(linked_storage):
                        continue
                    for ts_key in self._filter_ts_keys_by_weather_year(
                        self.system.list_time_series_keys(linked_storage)
                    ):
                        skey = ts_key.name.strip().lower().replace(" ", "_")
                        if skey == "hydro_budget":
                            resolved_ts = self._resolve_matching_time_series(linked_storage, ts_key)
                            if resolved_ts is not None:
                                comp_ts_props.setdefault(_gen.name, set()).add(
                                    get_hydro_budget_property_name(resolved_ts.resolution)
                                )

            records: list[dict[str, Any]] = []

            for comp in all_comps:
                ts_property_names = comp_ts_props.get(comp.name, set())
                ts_max_energy_props = {
                    name
                    for name in ts_property_names
                    if isinstance(name, str) and name.startswith("Max Energy ")
                }

                aliased_dict = comp.model_dump(by_alias=True, exclude_defaults=self.exclude_defaults)

                if self.exclude_defaults:
                    for prop_name in self._get_required_properties_for_component(comp, type_name):
                        field = comp.__class__.model_fields.get(prop_name)
                        if not field:
                            continue
                        alias_name = getattr(field, "alias", prop_name)
                        if alias_name not in aliased_dict:
                            value = getattr(comp, prop_name, None)
                            if value is not None:
                                aliased_dict[alias_name] = value

                if isinstance(comp, PLEXOSGenerator):
                    has_heat_rate_base = (
                        "Heat Rate Base" in aliased_dict and aliased_dict["Heat Rate Base"] is not None
                    )
                    has_heat_rate_incr = (
                        "Heat Rate Incr" in aliased_dict and aliased_dict["Heat Rate Incr"] is not None
                    )
                    if has_heat_rate_base and has_heat_rate_incr:
                        aliased_dict.pop("Heat Rate", None)

                for prop_name, raw in aliased_dict.items():
                    if prop_name in metadata_fields or raw is None:
                        continue

                    # If interval-specific Max Energy time series are present,
                    # keep only the matching interval property and drop other static intervals.
                    if (
                        isinstance(prop_name, str)
                        and prop_name.startswith("Max Energy ")
                        and ts_max_energy_props
                        and prop_name not in ts_max_energy_props
                    ):
                        continue

                    if prop_name in ts_property_names:
                        continue

                    if isinstance(raw, (int, float, str, bool)):
                        records.append(
                            {
                                "name": comp.name,
                                "property": prop_name,
                                "value": raw,
                                "band": 1,
                            }
                        )
                        continue

                    if isinstance(raw, list):
                        for rec in raw:
                            if not isinstance(rec, dict):
                                continue
                            rec_value = rec.get("value")
                            if rec_value is None:
                                continue
                            records.append(
                                {
                                    "name": comp.name,
                                    "property": prop_name,
                                    "value": rec_value,
                                    "band": rec.get("band", 1),
                                    "date_from": rec.get("date_from"),
                                    "date_to": rec.get("date_to"),
                                    "timeslice": rec.get("timeslice_name")
                                    or rec.get("timeslice")
                                    or rec.get("time_slice"),
                                    "datafile_text": rec.get("datafile_text")
                                    or rec.get("datafile_name")
                                    or rec.get("text"),
                                }
                            )
                        continue

                    if isinstance(raw, dict):
                        records.append(
                            {
                                "name": comp.name,
                                "property": prop_name,
                                "value": raw.get("value", 0),
                                "band": raw.get("band", 1),
                                "date_from": raw.get("date_from"),
                                "date_to": raw.get("date_to"),
                                "timeslice": raw.get("timeslice"),
                                "datafile_text": raw.get("datafile_text")
                                or raw.get("datafile_name")
                                or raw.get("text"),
                            }
                        )
                        continue

            if not records:
                continue

            records = self._deduplicate_property_records(records)
            if not records:
                continue

            logger.debug(
                "Adding properties for {} {} components ({} property rows)",
                len({r["name"] for r in records}),
                component_type.__name__,
                len(records),
            )
            self.db.add_properties_from_records(
                records,
                object_class=class_enum,
                parent_class=ClassEnum.System,
                collection=collection,
                scenario=self.plexos_scenario,
            )

        self._add_line_flow_clip_memo_entries()

    def _add_line_flow_clip_memo_entries(self) -> None:
        """Insert memo text for clipped Line flow properties.

        Adds memo entries only to `Line` `Min Flow`/`Max Flow` data rows where
        values are exactly -99999/99999, and only for lines that contain both
        clipped values.
        """
        if self.db is None:
            return

        try:
            # Apply memo text only to clipped flow bounds and preserve any existing memo rows.
            self.db._db.execute(
                """
                WITH line_flow_rows AS (
                    SELECT
                        d.data_id,
                        o.object_id,
                        p.name AS property_name,
                        CAST(d.value AS REAL) AS flow_value
                    FROM t_data d
                    INNER JOIN t_membership m ON d.membership_id = m.membership_id
                    INNER JOIN t_object o ON m.child_object_id = o.object_id
                    INNER JOIN t_class c ON o.class_id = c.class_id
                    INNER JOIN t_property p ON d.property_id = p.property_id
                    WHERE c.name = 'Line'
                      AND p.name IN ('Min Flow', 'Max Flow')
                ),
                qualified_lines AS (
                    SELECT object_id
                    FROM line_flow_rows
                    GROUP BY object_id
                    HAVING MAX(CASE WHEN property_name = 'Max Flow' AND flow_value = 99999 THEN 1 ELSE 0 END) = 1
                       AND MAX(CASE WHEN property_name = 'Min Flow' AND flow_value = -99999 THEN 1 ELSE 0 END) = 1
                )
                INSERT INTO t_memo_data (data_id, value)
                SELECT lfr.data_id, ?
                FROM line_flow_rows lfr
                INNER JOIN qualified_lines ql ON ql.object_id = lfr.object_id
                WHERE (
                    (lfr.property_name = 'Max Flow' AND lfr.flow_value = 99999)
                    OR (lfr.property_name = 'Min Flow' AND lfr.flow_value = -99999)
                )
                  AND NOT EXISTS (
                      SELECT 1 FROM t_memo_data md WHERE md.data_id = lfr.data_id
                  )
                """,
                (FLOW_CLIP_MEMO_TEXT,),
            )
        except Exception as exc:
            logger.debug("Skipping line flow memo insertion; t_memo_data may be unavailable: {}", exc)

    def _bulk_resolve_object_ids(
        self, class_to_names: dict[ClassEnum, set[str]]
    ) -> dict[tuple[ClassEnum, str], int]:
        """Bulk resolve object IDs for given class and name pairs."""
        assert self.db is not None
        out: dict[tuple[ClassEnum, str], int] = {}
        for class_enum, names in class_to_names.items():
            if not names:
                continue
            class_id = self.db.get_class_id(class_enum)
            for batch in self._chunked(sorted(names), 900):
                placeholders = ",".join("?" for _ in batch)
                query = f"""
                    SELECT name, object_id
                    FROM t_object
                    WHERE class_id = ? AND name IN ({placeholders})
                """
                rows = self.db._db.fetchall(query, (class_id, *batch))
                for name, object_id in rows:
                    out[(class_enum, name)] = object_id
        return out

    def _add_component_memberships(self) -> None:
        """
        Add membership relationships between components to the database.

        This method collects all PLEXOSMembership objects from the system and adds their relationships
        to the database. It performs the following steps:
        1. Retrieves all supplemental membership attributes from the system.
        2. Skips memberships with missing parent or child objects, or with unmapped classes/collections.
        3. Ignores memberships involving Model or Horizon objects, as these are handled elsewhere.
        4. Looks up the database IDs for parent and child objects and their collection.
        5. Avoids inserting duplicate memberships by tracking unique (parent_object_id, collection_id, child_object_id) keys.
        6. Bulk inserts all valid membership records into the database.

        Warnings and errors are logged for missing or invalid memberships, and a summary is logged upon completion.
        """
        if self.db is None:
            logger.error("Database not initialized")
            return

        memberships = list(self.system.get_supplemental_attributes(PLEXOSMembership))
        if not memberships:
            logger.warning("No memberships found in system")
            return

        filtered: list[tuple[ClassEnum, str, ClassEnum, str, CollectionEnum]] = []
        class_to_names: dict[ClassEnum, set[str]] = {}
        collection_keys: set[tuple[CollectionEnum, ClassEnum, ClassEnum]] = set()

        for m in memberships:
            if not m.parent_object or not m.child_object or not m.collection:
                continue
            parent_class = PLEXOS_TYPE_MAP_INVERTED.get(type(m.parent_object))
            child_class = PLEXOS_TYPE_MAP_INVERTED.get(type(m.child_object))
            if not parent_class or not child_class:
                continue
            if parent_class in (ClassEnum.Model, ClassEnum.Horizon) or child_class in (
                ClassEnum.Model,
                ClassEnum.Horizon,
            ):
                continue

            parent_name = m.parent_object.name
            child_name = m.child_object.name
            collection = m.collection

            filtered.append((parent_class, parent_name, child_class, child_name, collection))
            class_to_names.setdefault(parent_class, set()).add(parent_name)
            class_to_names.setdefault(child_class, set()).add(child_name)
            collection_keys.add((collection, parent_class, child_class))

        object_id_map = self._bulk_resolve_object_ids(class_to_names)

        class_id_cache: dict[ClassEnum, int] = {}
        for class_enum in class_to_names:
            class_id_cache[class_enum] = self.db.get_class_id(class_enum)

        collection_id_cache: dict[tuple[CollectionEnum, ClassEnum, ClassEnum], int] = {}
        for key in collection_keys:
            collection, parent_class, child_class = key
            collection_id_cache[key] = self.db.get_collection_id(
                collection,
                parent_class_enum=parent_class,
                child_class_enum=child_class,
            )

        records: list[dict[str, int]] = []
        seen: set[tuple[int, int, int]] = set()

        for idx, (parent_class, parent_name, child_class, child_name, collection) in enumerate(
            filtered, start=1
        ):
            parent_id = object_id_map.get((parent_class, parent_name))
            child_id = object_id_map.get((child_class, child_name))
            if parent_id is None or child_id is None:
                continue

            collection_id = collection_id_cache[(collection, parent_class, child_class)]
            key = (parent_id, collection_id, child_id)
            if key in seen:
                continue
            seen.add(key)

            records.append(
                {
                    "parent_class_id": class_id_cache[parent_class],
                    "parent_object_id": parent_id,
                    "collection_id": collection_id,
                    "child_class_id": class_id_cache[child_class],
                    "child_object_id": child_id,
                }
            )

            if idx % 100000 == 0:
                logger.info("Prepared {} membership rows...", idx)

        if not records:
            logger.warning("No valid membership records to add.")
            return

        self.db.add_memberships_from_records(records)
        logger.success("Successfully added {} memberships.", len(records))

    def _add_component_datafile_objects(self) -> None:
        """
        Add PLEXOSDatafile objects from the system to the database.

        This method:
        1. Calls _create_datafile_objects() to ensure all DataFile objects for exported CSVs exist in the system.
        2. Retrieves all PLEXOSDatafile components from the system.
        3. Adds each DataFile object to the database with the appropriate class and category.
        4. Updates each DataFile object with its assigned database object_id.
        5. Links DataFile objects to component properties that reference time series CSVs.

        Logs progress and warnings for missing or empty DataFile sets.
        """
        if self.db is None:
            logger.error("Database not initialized")
            return

        self._create_datafile_objects()
        datafiles = list(self.system.get_components(PLEXOSDatafile))
        if not datafiles:
            logger.info("No PLEXOSDatafile objects to add to DB.")
            return

        logger.debug(f"Adding {len(datafiles)} PLEXOSDatafile objects to DB.")

        names = [df.name for df in datafiles]
        self._add_objects_safe(ClassEnum.DataFile, names, category="CSV")

        for data_file in datafiles:
            object_id = self.db.get_object_id(ClassEnum.DataFile, data_file.name)
            data_file.object_id = object_id
            logger.debug(f"Set object_id={object_id} for DataFile: {data_file.name}")

        self._link_datafiles_to_components()

    def _link_datafiles_to_components(self) -> None:
        """
        Link DataFile objects to component properties that reference time series CSVs.

        This method finds all components in the system that have associated time series data,
        matches each time series to its exported CSV file, and updates the corresponding
        component property to reference the correct DataFile object. The linkage is made
        via the `datafile_text` field and, if applicable, the t_tag table in the database.

        This ensures that all time series properties in the exported XML reference the correct
        DataFile objects, enabling PLEXOS to locate and use the time series CSVs.
        """
        import re

        if self.db is None:
            logger.error("Database not initialized")
            return

        logger.info("Linking DataFiles to component properties...")

        output_dir = get_output_directory(self.config, self.system, output_path=self.output_path)

        # Build bidirectional maps between generators and their linked storage reservoirs
        generator_to_storage = self._build_generator_to_storage_map()
        storage_to_generator: dict[str, str] = {
            storage.name: gen_name for gen_name, storage in generator_to_storage.items()
        }

        seen_links: set[tuple[str, str, str]] = set()

        try:
            dir_files = os.listdir(output_dir)
        except FileNotFoundError:
            logger.warning("Output directory {} not found, skipping DataFile linking.", output_dir)
            return

        for component_type in self.system.get_component_types():
            components = list(self.system.get_components(component_type))
            components_with_ts = [c for c in components if self.system.has_time_series(c)]

            if not components_with_ts:
                continue

            for component in components_with_ts:
                ts_keys = self._filter_ts_keys_by_weather_year(self.system.list_time_series_keys(component))
                if not ts_keys:
                    continue

                for ts_key in ts_keys:
                    component_class = type(component).__name__
                    safe_ts_name = ts_key.name.replace(" ", "_").replace("/", "_")
                    pattern = re.compile(rf"{re.escape(component_class)}_{re.escape(safe_ts_name)}_.*\.csv")

                    matched_file = None
                    for filename in dir_files:
                        if pattern.match(filename):
                            matched_file = filename
                            break

                    if not matched_file:
                        fallback_pattern = re.compile(rf"[^_]+_{re.escape(safe_ts_name)}_.*\.csv")
                        for filename in dir_files:
                            if fallback_pattern.match(filename):
                                matched_file = filename
                                break

                    if not matched_file:
                        continue

                    datafile_name = matched_file.removesuffix(".csv")
                    datafile = self.system.get_component(PLEXOSDatafile, name=datafile_name)
                    if not datafile or datafile.object_id is None:
                        continue

                    class_enum = PLEXOS_TYPE_MAP_INVERTED.get(cast(type[PLEXOSObject], type(component)))
                    if not class_enum:
                        continue

                    key = ts_key.name.strip().lower().replace(" ", "_")
                    resolved_ts = self._resolve_matching_time_series(component, ts_key)

                    target_component = component
                    target_class_enum = class_enum
                    property_names: list[str] = []

                    # PLEXOSGenerator: natural_inflow/inflow redirect to linked PLEXOSStorage
                    if isinstance(component, PLEXOSGenerator) and key in GENERATOR_TO_STORAGE_TS_PROPERTY_MAP:
                        storage = generator_to_storage.get(component.name)
                        if storage:
                            storage_class = PLEXOS_TYPE_MAP_INVERTED.get(PLEXOSStorage)
                            if storage_class:
                                target_component = storage
                                target_class_enum = storage_class
                                property_names = [GENERATOR_TO_STORAGE_TS_PROPERTY_MAP[key]]
                        else:
                            logger.warning(
                                "No storage counterpart found for generator {} with key {}; skipping TS link.",
                                component.name,
                                key,
                            )

                    # PLEXOSStorage: hydro_budget redirects to linked PLEXOSGenerator
                    elif isinstance(component, PLEXOSStorage) and key in STORAGE_TO_GENERATOR_TS_PROPERTY_MAP:
                        gen_name = storage_to_generator.get(component.name)
                        if gen_name:
                            generator = self.system.get_component(PLEXOSGenerator, name=gen_name)
                            gen_class = PLEXOS_TYPE_MAP_INVERTED.get(PLEXOSGenerator)
                            if generator and gen_class:
                                target_component = generator
                                target_class_enum = gen_class
                                if key == "hydro_budget":
                                    if resolved_ts is not None:
                                        property_names = [
                                            get_hydro_budget_property_name(resolved_ts.resolution)
                                        ]
                                else:
                                    property_names = [STORAGE_TO_GENERATOR_TS_PROPERTY_MAP[key]]
                        else:
                            logger.warning(
                                "No generator counterpart found for storage {} with key {}; skipping TS link.",
                                component.name,
                                key,
                            )

                    # General case
                    else:
                        if isinstance(component, PLEXOSGenerator) and key == "max_active_power":
                            sienna_type = (getattr(component, "ext", None) or {}).get("sienna_type", "")
                            if sienna_type == "HydroDispatch":
                                # Must-dispatch available flow; attach to "Load" not "Rating"
                                property_names = ["Load"]
                            else:
                                property_names = ["Rating", "Load Subtracter"]
                        elif isinstance(component, PLEXOSGenerator) and key == "hydro_budget":
                            if resolved_ts is not None:
                                property_names = [get_hydro_budget_property_name(resolved_ts.resolution)]
                        else:
                            property_name = self._get_time_series_property_name(
                                component, ts_key_name=ts_key.name
                            )
                            if property_name:
                                property_names = [property_name]

                    if not property_names:
                        continue

                    csv_relative_path = str(output_dir.relative_to(output_dir.parent) / matched_file)

                    for property_name in property_names:
                        link_key = (target_component.name, property_name, csv_relative_path)
                        if link_key in seen_links:
                            continue
                        seen_links.add(link_key)

                        try:
                            self.db.add_property(
                                target_class_enum,
                                object_name=target_component.name,
                                name=property_name,
                                value=0,
                                datafile_text=csv_relative_path,
                                scenario=self.plexos_scenario,
                            )
                            logger.debug(
                                f"Linked {target_component.name}.{property_name} to {csv_relative_path}"
                            )
                        except Exception as e:
                            logger.error(
                                f"Failed to link {target_component.name}.{property_name} "
                                f"to {csv_relative_path}: {e}"
                            )

        self.db._db.execute(
            """
            UPDATE t_property SET is_dynamic = 1, is_enabled = 1
            WHERE property_id IN (
                SELECT DISTINCT d.property_id
                FROM t_data d
                JOIN t_text tx ON d.data_id = tx.data_id
            )
            """
        )
        logger.debug("Marked all DataFile-linked properties as is_dynamic=1, is_enabled=1")

    def _get_time_series_property_name(self, component: Any, ts_key_name: str | None = None) -> str | None:
        """
        Get the PLEXOS property name that should reference the time series CSV.

        This method determines the correct property name in PLEXOS that should be linked to a time series CSV
        for a given component and time series key. It uses specific mappings for common component types and
        time series variable names, and falls back to generic mappings if needed.

        Parameters
        ----------
        component : Any
            The component to get the property name for.
        ts_key_name : str | None, optional
            The specific time series key name (e.g., 'max_active_power').
            If None, will use the first time series key found for the component.

        Returns
        -------
        str | None
            The property name to use for the time series, or None if not applicable.
        """
        key = (ts_key_name or "").strip().lower().replace(" ", "_")

        fixed_property_by_type: dict[type[Any], str] = {
            PLEXOSReserve: "Min Provision",
            PLEXOSRegion: "Load",
            PLEXOSStorage: "Natural Inflow",
        }

        fixed = fixed_property_by_type.get(type(component))
        if fixed is not None:
            return fixed

        if isinstance(component, PLEXOSStorage):
            return STORAGE_TS_PROPERTY_MAP.get(key)

        if isinstance(component, PLEXOSGenerator):
            return GENERATOR_TS_PROPERTY_MAP.get(key)

        return None

    def _filter_ts_keys_by_weather_year(self, ts_keys: list[Any]) -> list[Any]:
        """Filter time series keys to those matching the configured weather_year.

        Keys that carry no ``horizon`` feature are always kept (they are not
        year-specific).  When ``weather_year`` is *None* the list is returned
        unchanged.
        """
        if self.weather_year is None:
            return ts_keys
        return [
            k
            for k in ts_keys
            if k.features.get("horizon") is None or k.features.get("horizon") == self.weather_year
        ]

    def _build_generator_to_storage_map(self) -> dict[str, PLEXOSStorage]:
        """Build a mapping from generator name to its associated storage component, if any."""
        mapping: dict[str, PLEXOSStorage] = {}
        memberships = self.system.get_supplemental_attributes(PLEXOSMembership)

        for m in memberships:
            parent = m.parent_object
            child = m.child_object
            if parent is None or child is None:
                continue

            if isinstance(parent, PLEXOSGenerator) and isinstance(child, PLEXOSStorage):
                mapping[parent.name] = child
            elif isinstance(parent, PLEXOSStorage) and isinstance(child, PLEXOSGenerator):
                mapping[child.name] = parent

        return mapping

    def _resolve_matching_time_series(self, component: Any, ts_key: Any) -> Any | None:
        """Resolve the best matching TS variant for a key using timestamp/resolution."""
        key = str(getattr(ts_key, "name", "")).strip().lower().replace(" ", "_")
        try:
            ts_list = self.system.list_time_series(component, name=ts_key.name, **ts_key.features)
        except Exception as e:
            logger.error("Failed to get time series for {}.{}: {}", component.name, ts_key.name, e)
            return None

        if not isinstance(ts_list, list):
            try:
                ts_list = list(ts_list)
            except TypeError:
                ts_list = [ts_list]

        if not ts_list:
            logger.warning("No time series found for {}.{}; skipping", component.name, ts_key.name)
            return None

        initial_ts_raw = getattr(ts_key, "initial_timestamp", None)
        initial_ts = initial_ts_raw if isinstance(initial_ts_raw, datetime) else None
        ts_resolution_raw = getattr(ts_key, "resolution", None)
        ts_resolution = ts_resolution_raw if isinstance(ts_resolution_raw, timedelta) else None

        if len(ts_list) > 1:
            if key == "hydro_budget":
                return max(
                    ts_list,
                    key=lambda t: getattr(getattr(t, "resolution", None), "total_seconds", lambda: 0)(),
                )

            matched = next(
                (
                    t
                    for t in ts_list
                    if getattr(t, "initial_timestamp", None) == initial_ts
                    and getattr(t, "resolution", None) == ts_resolution
                ),
                None,
            )
            if matched is None and ts_resolution is not None:
                matched = next(
                    (t for t in ts_list if getattr(t, "resolution", None) == ts_resolution),
                    None,
                )
            if matched is None and ts_resolution is None and initial_ts is not None:
                matched = next(
                    (t for t in ts_list if getattr(t, "initial_timestamp", None) == initial_ts),
                    None,
                )
            if matched is None:
                logger.warning(
                    "No matching TS variant for {}.{} (initial_ts={}, resolution={}); skipping",
                    component.name,
                    ts_key.name,
                    initial_ts,
                    ts_resolution,
                )
                return None
            return matched

        ts = ts_list[0]
        if ts_resolution is not None and getattr(ts, "resolution", None) != ts_resolution:
            logger.warning(
                "TS resolution mismatch for {}.{} (expected {}, got {}); skipping",
                component.name,
                ts_key.name,
                ts_resolution,
                getattr(ts, "resolution", None),
            )
            return None
        if initial_ts is not None and getattr(ts, "initial_timestamp", None) != initial_ts:
            logger.warning(
                "TS initial_timestamp mismatch for {}.{} (expected {}, got {}); skipping",
                component.name,
                ts_key.name,
                initial_ts,
                getattr(ts, "initial_timestamp", None),
            )
            return None
        return ts

    def export_time_series(self) -> Result[None, str]:
        """Export all time series data from the system to CSV files and update property references."""
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
            ts_keys = self._filter_ts_keys_by_weather_year(self.system.list_time_series_keys(component))
            ts_metadata.extend((component, ts_key) for ts_key in ts_keys)

        logger.debug(f"Found {len(ts_metadata)} time series keys total")

        def _grouping_key(item: tuple[Any, Any]) -> tuple:
            """Group by component class plus variable/time identity fields.

            Including component class avoids mixing identically named series
            across different component types.
            """
            component, ts_key = item
            initial_ts = getattr(ts_key, "initial_timestamp", None)
            resolution = getattr(ts_key, "resolution", None)
            return (
                type(component).__name__,
                ts_key.name,
                str(initial_ts),
                str(resolution),
                tuple(sorted(ts_key.features.items())),
            )

        ts_metadata_sorted = sorted(ts_metadata, key=_grouping_key)

        csv_filepaths: list[Path] = []
        output_dir = get_output_directory(self.config, self.system, output_path=self.output_path)

        for group_key, group_items in groupby(ts_metadata_sorted, key=_grouping_key):
            component_class, field_name, _initial_ts_str, _resolution_str, features_tuple = group_key
            metadata_dict = dict(features_tuple)
            if self.config.model_name is not None:
                metadata_dict["model_name"] = self.config.model_name
            if self.solve_year is not None:
                metadata_dict["horizon_year"] = self.solve_year
            elif getattr(self.config, "horizon_year", None) is not None:
                metadata_dict["horizon_year"] = self.config.horizon_year
            if self.weather_year is not None:
                metadata_dict["weather_year"] = self.weather_year
            group_list = list(group_items)

            filename = generate_csv_filename(field_name, component_class, metadata_dict)
            filepath = output_dir / filename
            csv_filepaths.append(filepath)

            time_series_data: list[tuple[str, Any]] = []
            for component, ts_key in group_list:
                ts = self._resolve_matching_time_series(component, ts_key)
                if ts is None:
                    continue
                time_series_data.append((component.name, ts))

            if not time_series_data:
                logger.debug("No time series data collected for group {}, skipping CSV.", field_name)
                continue

            result = export_time_series_csv(filepath, time_series_data)

            if result.is_err():
                assert isinstance(result, Err)
                logger.error(f"Failed to export time series: {result.error}")
                return Err(f"Time series export failed: {result.error}")

        logger.info(f"Exported {len(csv_filepaths)} time series files to {output_dir}")
        return Ok(None)

    def _create_datafile_objects(self) -> None:
        """
        Create PLEXOSDatafile objects for each CSV file being created.

        This method scans the output directory for all CSV files representing exported time series.
        For each CSV file found, it creates a corresponding PLEXOSDatafile object (if it does not already exist in the system)
        with the appropriate name and filename property. These DataFile objects are then available for linking to component
        properties that reference time series data.

        Steps performed:
        1. Determines the output directory for time series CSVs.
        2. Checks for the existence of the directory and logs if missing.
        3. Iterates over all CSV files in the directory.
        4. For each CSV, creates a PLEXOSDatafile object with the correct name and filename.
        5. Adds the DataFile object to the system if it does not already exist.

        Logs each DataFile object created.
        """
        logger.info("Creating DataFile objects...")

        output_dir = get_output_directory(self.config, self.system, output_path=self.output_path)
        time_series_dir = str(output_dir)

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
                    logger.debug(f"Created DataFile object: {datafile_obj.name}")

    def _validate_xml(self, xml_path: str) -> bool:
        """
        Validate the structure of an XML file.

        Attempts to parse the XML file at the given path using ElementTree.
        Returns True if the file is well-formed and can be parsed, otherwise False.

        Parameters
        ----------
        xml_path : str
            Path to the XML file to validate.

        Returns
        -------
        bool
            True if the XML is valid, False otherwise.
        """
        try:
            import xml.etree.ElementTree as ET

            tree = ET.parse(xml_path)
            _ = tree.getroot()
            return True
        except ET.ParseError:
            return False

    def _add_reports(self) -> None:
        """Add report definitions from plexos_reports.json to the PlexosDB."""
        report_objects = PLEXOSConfig.load_reports()
        for report_object in report_objects:
            report_object["collection"] = get_enum_from_string(report_object["collection"], CollectionEnum)
            report_object["parent_class"] = get_enum_from_string(report_object["parent_class"], ClassEnum)
            report_object["child_class"] = get_enum_from_string(report_object["child_class"], ClassEnum)
            self.db.add_report(**report_object)

    def _resolve_template_path(self) -> Path:
        """Resolve template from config.template as either a version key or a file path."""
        template_value = self.config.template

        if not template_value:
            resolved = self.config.get_config_path().joinpath(DEFAULT_XML_TEMPLATE)
            logger.debug(f"Using default XML template: {resolved}")
            return resolved

        # Treat known version keys as packaged template names
        if template_value in XML_TEMPLATE_MAP:
            resolved = self.config.get_config_path().joinpath(XML_TEMPLATE_MAP[template_value])
            logger.debug(f"Using XML template mapping for {template_value}: {resolved}")
            return resolved

        # Treat as a filesystem path
        template_path = Path(template_value).expanduser()

        if template_path.exists():
            resolved = template_path.resolve()
            logger.debug(f"Using XML template path from config: {resolved}")
            return resolved

        # Also allow bare filename in package config dir
        packaged_template = self.config.get_config_path().joinpath(template_value)
        if packaged_template.exists():
            logger.debug(f"Using packaged XML template by filename: {packaged_template}")
            return packaged_template

        raise FileNotFoundError(
            f"Template '{template_value}' is neither a known template key nor an existing file path."
        )
