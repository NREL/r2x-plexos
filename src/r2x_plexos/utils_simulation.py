"""Utilities for building PLEXOS simulation configurations."""

import calendar
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from loguru import logger
from plexosdb import ClassEnum, CollectionEnum, PlexosDB

from r2x_core import Err, Ok, Result
from r2x_plexos.models import PLEXOSHorizon, PLEXOSModel
from r2x_plexos.models.component import PLEXOSConfiguration
from r2x_plexos.models.simulation_config import (
    PLEXOSPASA,
    PLEXOSDiagnostic,
    PLEXOSMTSchedule,
    PLEXOSPerformance,
    PLEXOSProduction,
    PLEXOSReport,
    PLEXOSSTSchedule,
    PLEXOSTransmission,
)
from r2x_plexos.utils_plexosdb import validate_simulation_attribute


@dataclass
class SimulationConfig:
    """Result from build_plexos_simulation."""

    models: list[PLEXOSModel]
    horizons: list[PLEXOSHorizon]
    memberships: list[tuple[str, str]]  # (model_name, horizon_name) pairs
    simulation_configs: dict[str, PLEXOSConfiguration] | None = None


def datetime_to_ole_date(dt: datetime) -> float:
    """
    Convert Python datetime to OLE Automation Date.

    OLE Automation Date is the number of days since December 30, 1899.
    PLEXOS uses this format for date storage.

    Parameters
    ----------
    dt : datetime
        The datetime to convert

    Returns
    -------
    float
        The OLE Automation Date value

    Examples
    --------
    >>> datetime_to_ole_date(datetime(2012, 1, 1))
    40909.0
    """
    ole_epoch = datetime(1899, 12, 30, 0, 0, 0)
    delta = dt - ole_epoch
    return float(delta.days) + (delta.seconds / 86400.0)


def get_default_simulation_config() -> dict[str, Any]:
    """
    Get default simulation configuration template.

    Returns a dictionary with default simulation objects that can be
    customized by users. Each simulation type has sensible defaults.

    Returns
    -------
    dict[str, Any]
        Dictionary with keys:
        - "mt_schedule": PLEXOSMTSchedule instance or None
        - "st_schedule": PLEXOSSTSchedule instance or None
        - "production": PLEXOSProduction instance or None
        - "pasa": PLEXOSPASA instance or None
        - "performance": PLEXOSPerformance instance or None
        - "report": PLEXOSReport instance or None
        - "transmission": PLEXOSTransmission instance or None
        - "diagnostic": PLEXOSDiagnostic instance or None

    Examples
    --------
    >>> defaults = get_default_simulation_config()
    >>> defaults["performance"].solver  # Returns 4 (Gurobi)
    >>> defaults["st_schedule"].transmission_detail  # Returns 1 (nodal)
    """
    return {
        "mt_schedule": PLEXOSMTSchedule.example(),
        "st_schedule": PLEXOSSTSchedule.example(),
        "production": PLEXOSProduction.example(),
        "pasa": PLEXOSPASA.example(),
        "performance": PLEXOSPerformance.example(),
        "report": PLEXOSReport.example(),
        "transmission": PLEXOSTransmission.example(),
        "diagnostic": PLEXOSDiagnostic.example(),
    }


def convert_simulation_config_to_attributes(
    sim_config: PLEXOSConfiguration,
) -> Result[dict[str, Any], str]:
    """
    Convert Pydantic simulation config to attribute dictionary.

    Extracts all fields with aliases (attribute names) and their values
    from a simulation configuration object.

    Parameters
    ----------
    sim_config : PLEXOSConfiguration
        Any simulation config object (PLEXOSMTSchedule, PLEXOSPerformance, etc.)

    Returns
    -------
    Result[dict[str, Any], str]
        Ok: Dictionary mapping attribute names (aliases) to values
        Err: Error message if conversion fails

    Examples
    --------
    >>> perf = PLEXOSPerformance(name="MyPerf", solver=4)
    >>> result = convert_simulation_config_to_attributes(perf)
    >>> attrs = result.unwrap()
    >>> attrs["SOLVER"]  # Returns 4
    """
    try:
        # Base class fields to skip (from PLEXOSObject)
        base_fields = {"name", "category", "object_id", "uuid"}

        attributes = {}
        for field_name, field_info in sim_config.model_fields.items():
            # Skip base class fields
            if field_name in base_fields:
                continue

            value = getattr(sim_config, field_name)
            # Only include non-None values
            if value is not None:
                # Get the alias (attribute name) from field info
                alias = field_info.alias if field_info.alias else field_name
                attributes[alias] = value

        return Ok(attributes)
    except Exception as e:
        return Err(f"Failed to convert simulation config to attributes: {e!s}")


def validate_simulation_config(
    db: PlexosDB,
    class_enum: ClassEnum,
    sim_config: PLEXOSConfiguration,
) -> Result[None, str]:
    """
    Validate all attributes in simulation config are valid for the class.

    Parameters
    ----------
    db : PlexosDB
        PlexosDB instance to validate against
    class_enum : ClassEnum
        The simulation class enum (e.g., ClassEnum.Performance)
    sim_config : PLEXOSConfiguration
        The simulation configuration object

    Returns
    -------
    Result[None, str]
        Ok: If all attributes are valid
        Err: Error message listing invalid attributes

    Examples
    --------
    >>> perf = PLEXOSPerformance(name="MyPerf", solver=4)
    >>> result = validate_simulation_config(db, ClassEnum.Performance, perf)
    >>> if result.is_err():
    ...     print(f"Validation failed: {result.error}")
    """
    # Convert config to attributes
    attrs_result = convert_simulation_config_to_attributes(sim_config)
    if attrs_result.is_err():
        return Err(attrs_result.unwrap_err())

    attributes = attrs_result.unwrap()

    # Validate each attribute
    errors = []
    for attr_name in attributes:
        validation_result = validate_simulation_attribute(db, class_enum, attr_name)
        if validation_result.is_err():
            errors.append(validation_result.unwrap_err())

    if errors:
        return Err("Invalid attributes:\n" + "\n".join(errors))

    return Ok(None)


def ingest_simulation_config_to_plexosdb(
    db: PlexosDB,
    class_enum: ClassEnum,
    sim_config: PLEXOSConfiguration,
    validate: bool = True,
) -> Result[dict[str, Any], str]:
    """
    Ingest simulation configuration object to PlexosDB as attributes.

    Parameters
    ----------
    db : PlexosDB
        PlexosDB instance to ingest into
    class_enum : ClassEnum
        The simulation class enum
    sim_config : PLEXOSConfiguration
        The simulation configuration object with name attribute
    validate : bool, optional
        Whether to validate attributes before ingestion, by default True

    Returns
    -------
    Result[dict[str, Any], str]
        Ok: Dictionary with keys:
            - "object_name": Name of the simulation object
            - "class": ClassEnum name
            - "attributes_added": List of attribute names added
            - "attribute_count": Number of attributes added
        Err: Error message if ingestion fails

    Examples
    --------
    >>> perf = PLEXOSPerformance(name="MyPerf", solver=4, mip_relative_gap=0.01)
    >>> result = ingest_simulation_config_to_plexosdb(db, ClassEnum.Performance, perf)
    >>> info = result.unwrap()
    >>> print(f"Added {info['attribute_count']} attributes")
    """
    # Check that sim_config has a name
    if sim_config.name is None:
        return Err("Simulation config must have a name")

    # Validate if requested
    if validate:
        validation_result = validate_simulation_config(db, class_enum, sim_config)
        if validation_result.is_err():
            return Err(validation_result.unwrap_err())

    # Convert config to attributes
    attrs_result = convert_simulation_config_to_attributes(sim_config)
    if attrs_result.is_err():
        return Err(attrs_result.unwrap_err())

    attributes = attrs_result.unwrap()

    # Add the object first
    try:
        db.add_object(class_enum, sim_config.name)
    except Exception as e:
        return Err(f"Failed to add simulation object '{sim_config.name}': {e!s}")

    # Add each attribute
    for attr_name, attr_value in attributes.items():
        try:
            db.add_attribute(
                class_enum,
                sim_config.name,
                attribute_name=attr_name,
                attribute_value=attr_value,
            )
        except Exception as e:
            return Err(f"Failed to add attribute '{attr_name}': {e!s}")

    return Ok(
        {
            "object_name": sim_config.name,
            "class": class_enum.name,
            "attributes_added": list(attributes.keys()),
            "attribute_count": len(attributes),
        }
    )


def build_plexos_simulation(
    config: dict[str, Any],
    defaults: dict[str, Any] | None = None,
    simulation_config: dict[str, PLEXOSConfiguration | None] | None = None,
) -> Result[SimulationConfig, str]:
    """
    Build PLEXOS simulation configuration from user config.

    This function generates Model and Horizon objects based on templates or custom
    specifications. It handles common patterns like monthly splits, weekly splits,
    or fully custom configurations.

    Parameters
    ----------
    config : dict
        User configuration dictionary. Supported formats:

        Simple daily simulation:
            {"horizon_year": 2012, "resolution": "1D"}

        Template-based (monthly):
            {"horizon_year": 2012, "template": "monthly"}

        Template with overrides:
            {
                "horizon_year": 2012,
                "template": "monthly",
                "model_properties": {"Random Number Seed": "1234"},
                "horizon_properties": {"Periods per Day": 48}
            }

        Fully custom:
            {
                "models": [
                    {
                        "name": "Summer_2012",
                        "horizon": {
                            "name": "Summer_Horizon",
                            "start": "2012-06-01",
                            "end": "2012-08-31",
                            "chrono_step_type": 2,
                            "chrono_step_count": 91
                        }
                    }
                ]
            }

    defaults : dict, optional
        Default settings. If None, uses empty dict.

    simulation_config : dict[str, PLEXOSConfiguration | None], optional
        Dictionary of simulation configuration objects. Keys should be:
        "mt_schedule", "st_schedule", "production", "pasa", "performance",
        "report", "transmission", "diagnostic". Values can be None to skip.
        If None, no simulation configs are included in the result.
        Use get_default_simulation_config() to get a template.

    Returns
    -------
    Result[SimulationBuildResult, str]
        Ok with SimulationBuildResult containing lists of PLEXOSModel, PLEXOSHorizon objects,
        their memberships, and optional simulation_configs, or Err with error message.

    Examples
    --------
    >>> # Daily simulation for full horizon_year
    >>> result = build_plexos_simulation({"horizon_year": 2012, "resolution": "1D"})
    >>> if result.is_ok():
    ...     build_result = result.unwrap()
    ...     len(build_result.models)
    1

    >>> # Monthly models
    >>> result = build_plexos_simulation({"horizon_year": 2012, "template": "monthly"})
    >>> if result.is_ok():
    ...     build_result = result.unwrap()
    ...     len(build_result.models)
    12
    """
    if defaults is None:
        defaults = {}

    # Route to appropriate builder based on config
    result: Result[SimulationConfig, str]
    if "models" in config:
        # Fully custom configuration
        result = _build_custom_simulation(config, defaults)
    elif "template" in config:
        # Template-based configuration
        result = _build_from_template(config, defaults)
    else:
        # Simple configuration (infer template)
        result = _build_simple_simulation(config, defaults)

    # Add simulation_config to the result if provided
    if result.is_ok() and simulation_config is not None:
        build_result = result.unwrap()
        return Ok(
            SimulationConfig(
                models=build_result.models,
                horizons=build_result.horizons,
                memberships=build_result.memberships,
                simulation_configs=simulation_config,  # type: ignore[arg-type]
            )
        )

    return result


def _build_simple_simulation(
    config: dict[str, Any], defaults: dict[str, Any]
) -> Result[SimulationConfig, str]:
    """Build simulation from simple config (horizon_year + resolution)."""
    horizon_year = config.get("horizon_year")
    resolution = config.get("resolution", "1D")

    if not horizon_year:
        return Err("Configuration must specify 'horizon_year'")

    # Create single model for full horizon_year
    start_date = datetime(horizon_year, 1, 1)
    datetime(horizon_year, 12, 31, 23, 59, 59)

    # Determine chrono_step_count based on resolution
    if resolution == "1D":
        days_in_year = 366 if calendar.isleap(horizon_year) else 365
        chrono_step_count = days_in_year
        chrono_step_type = 2  # Daily
    elif resolution == "1H":
        days_in_year = 366 if calendar.isleap(horizon_year) else 365
        chrono_step_count = days_in_year
        chrono_step_type = 1  # Hourly
    else:
        return Err(f"Unsupported resolution: {resolution}")

    horizon = PLEXOSHorizon(
        name=f"Horizon_{horizon_year}",
        chrono_date_from=datetime_to_ole_date(start_date),
        date_from=datetime_to_ole_date(start_date),
        chrono_step_count=chrono_step_count,
        chrono_step_type=chrono_step_type,
        step_count=1,
        periods_per_day=24,
    )

    model = PLEXOSModel(
        name=f"Model_{horizon_year}",
        category=f"model_{horizon_year}",
    )

    return Ok(
        SimulationConfig(
            models=[model],
            horizons=[horizon],
            memberships=[(model.name, horizon.name)],
        )
    )


def _build_from_template(config: dict[str, Any], defaults: dict[str, Any]) -> Result[SimulationConfig, str]:
    """Build simulation from template specification."""
    template_name = config["template"]
    horizon_year = config.get("horizon_year")

    if not horizon_year:
        return Err("Configuration must specify 'horizon_year'")

    if template_name == "monthly":
        return _build_monthly_models(horizon_year, config, defaults)
    elif template_name == "weekly":
        return _build_weekly_models(horizon_year, config, defaults)
    elif template_name == "quarterly":
        return _build_quarterly_models(horizon_year, config, defaults)
    else:
        return Err(f"Unknown template: {template_name}")


def _build_monthly_models(
    horizon_year: int, config: dict[str, Any], defaults: dict[str, Any]
) -> Result[SimulationConfig, str]:
    """Generate 12 monthly models for the specified horizon_year."""
    models = []
    horizons = []
    memberships = []

    model_properties = config.get("model_properties", {})
    horizon_properties = config.get("horizon_properties", {})

    for month in range(1, 13):
        # Calculate month boundaries
        start_date = datetime(horizon_year, month, 1)
        _, last_day = calendar.monthrange(horizon_year, month)
        end_date = datetime(horizon_year, month, last_day, 23, 59, 59)
        days_in_month = (end_date - start_date).days + 1

        # Create horizon
        horizon_name = f"Horizon_{horizon_year}_M{month:02d}"
        horizon_data = {
            "name": horizon_name,
            "chrono_date_from": datetime_to_ole_date(start_date),
            "date_from": datetime_to_ole_date(datetime(horizon_year, 1, 1)),
            "chrono_step_count": days_in_month,
            "chrono_step_type": 2,  # Daily
            "step_count": 1,
            "periods_per_day": 24,
        }
        horizon_data.update(horizon_properties)
        horizon = PLEXOSHorizon(**horizon_data)
        horizons.append(horizon)

        # Create model
        model_name = f"Model_{horizon_year}_M{month:02d}"
        model_data = {
            "name": model_name,
            "category": f"model_{horizon_year}",
        }
        model_data.update(model_properties)
        model = PLEXOSModel(**model_data)
        models.append(model)

        # Track membership
        memberships.append((model_name, horizon_name))

    return Ok(
        SimulationConfig(
            models=models,
            horizons=horizons,
            memberships=memberships,
        )
    )


def _build_weekly_models(
    horizon_year: int, config: dict[str, Any], defaults: dict[str, Any]
) -> Result[SimulationConfig, str]:
    """Generate 52 weekly models for the specified horizon_year."""
    models = []
    horizons = []
    memberships = []

    model_properties = config.get("model_properties", {})
    horizon_properties = config.get("horizon_properties", {})

    start_of_year = datetime(horizon_year, 1, 1)

    for week in range(1, 53):
        # Calculate week boundaries
        start_date = start_of_year + timedelta(weeks=week - 1)
        end_date = start_date + timedelta(days=6, hours=23, minutes=59, seconds=59)

        # Don't go beyond horizon_year boundary
        if end_date.year > horizon_year:
            end_date = datetime(horizon_year, 12, 31, 23, 59, 59)

        days_in_week = (end_date - start_date).days + 1

        # Create horizon
        horizon_name = f"Horizon_{horizon_year}_W{week:02d}"
        horizon_data = {
            "name": horizon_name,
            "chrono_date_from": datetime_to_ole_date(start_date),
            "date_from": datetime_to_ole_date(start_of_year),
            "chrono_step_count": days_in_week,
            "chrono_step_type": 2,  # Daily
            "step_count": 1,
            "periods_per_day": 24,
        }
        horizon_data.update(horizon_properties)
        horizon = PLEXOSHorizon(**horizon_data)
        horizons.append(horizon)

        # Create model
        model_name = f"Model_{horizon_year}_W{week:02d}"
        model_data = {
            "name": model_name,
            "category": f"model_{horizon_year}",
        }
        model_data.update(model_properties)
        model = PLEXOSModel(**model_data)
        models.append(model)

        # Track membership
        memberships.append((model_name, horizon_name))

    return Ok(
        SimulationConfig(
            models=models,
            horizons=horizons,
            memberships=memberships,
        )
    )


def _build_quarterly_models(
    horizon_year: int, config: dict[str, Any], defaults: dict[str, Any]
) -> Result[SimulationConfig, str]:
    """Generate 4 quarterly models for the specified horizon_year."""
    models = []
    horizons = []
    memberships = []

    model_properties = config.get("model_properties", {})
    horizon_properties = config.get("horizon_properties", {})

    quarters = [
        (1, "Q1", [1, 2, 3]),
        (2, "Q2", [4, 5, 6]),
        (3, "Q3", [7, 8, 9]),
        (4, "Q4", [10, 11, 12]),
    ]

    for _quarter_num, quarter_name, months in quarters:
        # Calculate quarter boundaries
        start_date = datetime(horizon_year, months[0], 1)
        _, last_day = calendar.monthrange(horizon_year, months[-1])
        end_date = datetime(horizon_year, months[-1], last_day, 23, 59, 59)
        days_in_quarter = (end_date - start_date).days + 1

        # Create horizon
        horizon_name = f"Horizon_{horizon_year}_{quarter_name}"
        horizon_data = {
            "name": horizon_name,
            "chrono_date_from": datetime_to_ole_date(start_date),
            "date_from": datetime_to_ole_date(datetime(horizon_year, 1, 1)),
            "chrono_step_count": days_in_quarter,
            "chrono_step_type": 2,  # Daily
            "step_count": 1,
            "periods_per_day": 24,
        }
        horizon_data.update(horizon_properties)
        horizon = PLEXOSHorizon(**horizon_data)
        horizons.append(horizon)

        # Create model
        model_name = f"Model_{horizon_year}_{quarter_name}"
        model_data = {
            "name": model_name,
            "category": f"model_{horizon_year}",
        }
        model_data.update(model_properties)
        model = PLEXOSModel(**model_data)
        models.append(model)

        # Track membership
        memberships.append((model_name, horizon_name))

    return Ok(
        SimulationConfig(
            models=models,
            horizons=horizons,
            memberships=memberships,
        )
    )


def _build_custom_simulation(
    config: dict[str, Any], defaults: dict[str, Any]
) -> Result[SimulationConfig, str]:
    """Build simulation from fully custom specification."""
    models = []
    horizons = []
    memberships = []

    for model_config in config["models"]:
        model_name = model_config["name"]
        horizon_config = model_config["horizon"]

        # Parse dates - fromisoformat can raise ValueError
        start_str = horizon_config.get("start")
        end_str = horizon_config.get("end")

        if not start_str:
            return Err(f"Model '{model_name}' horizon missing required 'start' date")
        if not end_str:
            return Err(f"Model '{model_name}' horizon missing required 'end' date")

        start = datetime.fromisoformat(start_str)
        end = datetime.fromisoformat(end_str)
        days = (end - start).days + 1

        if days <= 0:
            return Err(
                f"Model '{model_name}' has invalid date range: start '{start_str}' must be before end '{end_str}'"
            )

        # Create horizon
        horizon_name = horizon_config.get("name", f"{model_name}_Horizon")
        horizon = PLEXOSHorizon(
            name=horizon_name,
            chrono_date_from=datetime_to_ole_date(start),
            date_from=datetime_to_ole_date(start),
            chrono_step_count=horizon_config.get("chrono_step_count", days),
            chrono_step_type=horizon_config.get("chrono_step_type", 2),
            step_count=horizon_config.get("step_count", 1),
            periods_per_day=horizon_config.get("periods_per_day", 24),
        )
        horizons.append(horizon)

        # Create model
        model = PLEXOSModel(
            name=model_name,
            category=model_config.get("category", "custom"),
        )
        models.append(model)

        # Track membership
        memberships.append((model_name, horizon_name))

    return Ok(
        SimulationConfig(
            models=models,
            horizons=horizons,
            memberships=memberships,
        )
    )


def _add_horizon_attributes(db: PlexosDB, horizon: PLEXOSHorizon) -> None:
    """
    Add horizon attributes to the database.

    Note: Horizon configuration uses attributes (not properties) in PLEXOS.
    Attributes are stored in t_attribute table, not t_property/t_data.

    Parameters
    ----------
    db : PlexosDB
        Database instance
    horizon : PLEXOSHorizon
        Horizon object with attributes to add
    """
    attribute_map = {
        "Chrono Date From": horizon.chrono_date_from,
        "Date From": horizon.date_from,
        "Chrono Step Count": horizon.chrono_step_count,
        "Chrono Step Type": horizon.chrono_step_type,
        "Step Count": horizon.step_count,
        "Periods per Day": horizon.periods_per_day,
    }

    for attr_name, attr_value in attribute_map.items():
        if attr_value is not None:
            db.add_attribute(
                ClassEnum.Horizon,
                horizon.name,
                attribute_name=attr_name,
                attribute_value=attr_value,
            )


def ingest_simulation_to_plexosdb(
    db: PlexosDB,
    result: SimulationConfig,
    validate: bool = True,
) -> Result[dict[str, Any], str]:
    """
    Write simulation configuration to plexosdb.

    This function ingests models, horizons, their memberships, and simulation
    configuration objects (Performance, Production, etc.) into a PlexosDB instance.

    Parameters
    ----------
    db : PlexosDB
        The PlexosDB instance to write to
    result : SimulationBuildResult
        Output from build_plexos_simulation containing models, horizons, and memberships
    validate : bool, optional
        Whether to validate simulation config attributes before ingestion, by default True

    Returns
    -------
    Result[dict[str, Any], str]
        Ok with dictionary containing:
            - 'models': Dict mapping model names to IDs
            - 'horizons': Dict mapping horizon names to IDs
            - 'simulation_objects': List of dicts with simulation object ingestion info
        or Err with error message.

    Examples
    --------
    >>> result = build_plexos_simulation({"horizon_year": 2012, "template": "monthly"})
    >>> if result.is_ok():
    ...     build_result = result.unwrap()
    ...     db = PlexosDB.from_xml("my_model.xml")
    ...     ingest_result = ingest_simulation_to_plexosdb(db, build_result)
    ...     if ingest_result.is_ok():
    ...         ids = ingest_result.unwrap()
    ...         print(f"Created {len(ids['models'])} models and {len(ids['horizons'])} horizons")
    """
    horizon_ids: dict[str, int] = {}
    model_ids: dict[str, int] = {}

    logger.info(f"Creating {len(result.horizons)} horizon object(s)...")
    for horizon in result.horizons:
        horizon_id = db.add_object(ClassEnum.Horizon, horizon.name)
        horizon_ids[horizon.name] = horizon_id

        _add_horizon_attributes(db, horizon)

        logger.debug(f"Created horizon '{horizon.name}' (ID: {horizon_id})")

    logger.info(f"Creating {len(result.models)} model object(s)...")
    for model in result.models:
        model_id = db.add_object(ClassEnum.Model, model.name, category=model.category)
        model_ids[model.name] = model_id
        logger.debug(f"Created model '{model.name}' (ID: {model_id})")

    logger.info(f"Creating {len(result.memberships)} model-horizon membership(s)...")
    for model_name, horizon_name in result.memberships:
        db.add_membership(
            ClassEnum.Model,
            ClassEnum.Horizon,
            model_name,
            horizon_name,
            CollectionEnum.Horizon,
        )
        logger.debug(f"Linked model '{model_name}' → horizon '{horizon_name}'")

    simulation_objects_added: list[dict[str, Any]] = []
    if not result.simulation_configs:
        logger.info("No simulation configuration objects to ingest")
    else:
        total_configs = len(result.simulation_configs)
        logger.info(f"Ingesting simulation configuration objects (0/{total_configs})...")

        config_class_map = {
            "mt_schedule": ClassEnum.MTSchedule,
            "st_schedule": ClassEnum.STSchedule,
            "production": ClassEnum.Production,
            "pasa": ClassEnum.PASA,
            "performance": ClassEnum.Performance,
            "report": ClassEnum.Report,
            "transmission": ClassEnum.Transmission,
            "diagnostic": ClassEnum.Diagnostic,
        }

        for config_key, sim_config in result.simulation_configs.items():
            if sim_config is None:
                continue

            class_enum = config_class_map.get(config_key)
            if class_enum is None:
                logger.warning(f"Skipping unknown simulation config type: '{config_key}'")
                continue

            ingest_result = ingest_simulation_config_to_plexosdb(
                db, class_enum, sim_config, validate=validate
            )

            if ingest_result.is_err():
                assert isinstance(ingest_result, Err)
                logger.warning(
                    f"Failed to ingest {config_key} config '{sim_config.name}': {ingest_result.error}"
                )
                continue

            simulation_objects_added.append(ingest_result.unwrap())
            logger.debug(f"Ingested {config_key}: '{sim_config.name}'")

        success_count = len(simulation_objects_added)
        logger.info(f"Successfully ingested {success_count}/{total_configs} simulation configuration objects")

    logger.info("Simulation configuration successfully ingested into PlexosDB")

    return Ok(
        {
            "models": model_ids,
            "horizons": horizon_ids,
            "simulation_objects": simulation_objects_added,
        }
    )
