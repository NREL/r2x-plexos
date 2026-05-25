"""Utilities for building PLEXOS simulation configurations."""

import calendar
import difflib
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from loguru import logger
from plexosdb import ClassEnum, PlexosDB
from plexosdb.enums import get_default_collection
from plexosdb.exceptions import NotFoundError

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
from r2x_plexos.utils_mappings import CONFIG_CLASS_MAP, MEMBERSHIP_TYPE_MAP
from r2x_plexos.utils_plexosdb import validate_simulation_attribute

BASE_DIAGNOSE_NAME = "base_diagnose"
BASE_DIAGNOSE_DEFAULT_ATTRIBUTE_CANDIDATES: dict[tuple[str, ...], int] = {
    ("Database Load",): -1,
    ("Summary Each Step", "Step Summary"): -1,
    ("Execution Times", "Times"): -1,
    ("Task Size",): -1,
    ("LP Solver Progress", "LP Progress"): -1,
    ("Computer Information",): -1,
    ("MIP Solver Progress", "MIP Progress"): -1,
}

# Transmission defaults for scenario membership objects (e.g., base_transmission).
# Values: -1 = enabled/checked, 1 = "Fixed Shift Factor" selection in Optimal Power Flow.
TRANSMISSION_DEFAULT_ATTRIBUTE_CANDIDATES: dict[tuple[str, ...], int] = {
    ("OF Method", "Optimal Power Flow"): 1,
    ("Formulate Upfront",): -1,
    ("Constraints Enabled", "Enforced Line and Transformer Limits"): -1,
    ("Interface Constraints Enabled", "Enforce Interface Limits"): -1,
    ("Enforce Limits On Lines In Interfaces",): -1,
    ("Losses Enabled", "Model Losses"): -1,
}

PERFORMANCE_DEFAULT_ATTRIBUTES: dict[str, float] = {
    # 1% relative MIP gap (template class default is 0.0001 = 0.01%).
    "MIP Relative Gap": 0.01,
}

PRODUCTION_DEFAULT_ATTRIBUTES: dict[str, int] = {
    # Integer unit commitment optimality.
    "Unit Commitment Optimality": 2,
}

ST_SCHEDULE_DEFAULT_ATTRIBUTES: dict[str, int] = {
    # Deterministic ST schedule mode.
    "Stochastic Method": 0,
}


@dataclass
class SimulationConfig:
    """Result from build_plexos_simulation."""

    models: list[PLEXOSModel]
    horizons: list[PLEXOSHorizon]
    memberships: list[tuple[str, str, str]]  # (model_name, child_name, membership_type) triples
    simulation_configs: dict[str, PLEXOSConfiguration | None] | None = None


def get_enum_from_string(string: str, enum_class, prefix: str | None = None):
    """
    Find the closest matching enum member for a given string.

    Uses sequence similarity to match the input string (optionally with a prefix)
    to the enum member names (case-insensitive). Returns the enum member with the
    highest similarity above a threshold, or raises KeyError if no match is found.

    Parameters
    ----------
    string : str
        The string to match to an enum member.
    enum_class : Enum
        The enum class to search.
    prefix : str | None, optional
        Optional prefix to prepend to the string before matching.

    Returns
    -------
    Enum
        The closest matching enum member.

    Raises
    ------
    KeyError
        If no matching enum member is found above the similarity threshold.

    Examples
    --------
    >>> get_enum_from_string("generator", ClassEnum)
    <ClassEnum.Generator: ...>
    """
    max_similarity = 0.95
    closest_enum = None
    if prefix is None:
        prefix = ""

    # We lower to do a caseinsensitive mapping.
    requested_string = (prefix + string).lower()
    for enum_member in enum_class:
        enum_member_string = enum_member.lower()
        similarity = difflib.SequenceMatcher(None, requested_string, enum_member_string).ratio()
        if similarity > max_similarity:
            max_similarity = similarity
            closest_enum = enum_member
    if closest_enum is None:
        raise KeyError(f"No matching enum found for string '{string}'")
    return closest_enum


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


def _replace_year_in_name(name: str, weather_year: int | None) -> str:
    """Replace the hardcoded template year in a static object name."""
    if weather_year is None:
        return name
    return name.replace("2012", str(weather_year))


def _shift_ole_date_to_year(ole_date: float, weather_year: int) -> float:
    """Shift a date to the same month/day in a target year."""
    base = datetime(1899, 12, 30)
    dt = base + timedelta(days=float(ole_date))

    if dt.month == 2 and dt.day == 29 and not calendar.isleap(weather_year):
        shifted = dt.replace(year=weather_year, day=28)
    else:
        shifted = dt.replace(year=weather_year)

    return datetime_to_ole_date(shifted)


def _rewrite_horizon_attributes_for_weather_year(
    attrs: dict[str, Any],
    horizon_name: str,
    weather_year: int | None,
    is_overlap: bool = False,
) -> dict[str, Any]:
    """
    Rewrite year-sensitive horizon attributes for a target weather year.

    Adjusts OLE date fields and leap year sensitive horizon counts while
    preserving the remaining static attributes.
    """
    if weather_year is None:
        return dict(attrs)

    updated = dict(attrs)
    is_leap = calendar.isleap(weather_year)

    # Shift OLE dates to the new weather year
    for key in ("Chrono Date From", "Date From"):
        value = updated.get(key)
        if value is not None:
            updated[key] = _shift_ole_date_to_year(float(value), weather_year)

    # Step Count typically represents full-year span but adjusted for leap year, if present
    if "Step Count" in updated:
        updated["Step Count"] = 366.0 if is_leap else 365.0

    # Adjust Chrono Step Count only where leap year changes the actual chrono span
    if "Chrono Step Count" in updated:
        lower_name = horizon_name.lower()

        # Fullyear horizon
        if (
            lower_name.startswith(f"base_{weather_year}".lower())
            and "_m" not in lower_name
            and "_d1" not in lower_name
        ):
            if is_overlap:
                updated["Chrono Step Count"] = 363.0 if is_leap else 362.0
            else:
                updated["Chrono Step Count"] = 365.0 if is_leap else 364.0

        # February monthly horizon
        elif "_m2_" in lower_name or lower_name.endswith("_m2"):
            if lower_name.endswith("_ov"):
                updated["Chrono Step Count"] = 32.0 if is_leap else 31.0
            else:
                updated["Chrono Step Count"] = 29.0 if is_leap else 28.0

    return updated


def _build_from_static_models(
    defaults: dict[str, Any],
    simulation_config: dict[str, PLEXOSConfiguration | None] | None = None,
    weather_year: int | None = None,
) -> Result[SimulationConfig, str]:
    """
    Build simulation configuration from static models and horizons.

    Creates model and horizon objects from the static definitions in the defaults
    dictionary and rewrites year-dependent names and horizon attributes when a
    weather_year is provided. Memberships between models and their child objects
    are rebuilt using the rewritten names.

    Parameters
    ----------
    defaults : dict[str, Any]
        Dictionary containing static model definitions and related defaults.
    simulation_config : dict[str, PLEXOSConfiguration | None], optional
        Optional simulation configuration objects to attach to the result.
    weather_year : int | None, optional
        Year used to rewrite static model names, horizon names, memberships,
        and year-sensitive horizon attributes.

    Returns
    -------
    Result[SimulationConfig, str]
        Ok with SimulationConfig containing models, horizons, memberships,
        and optional simulation configuration objects.
    """
    from r2x_plexos.plugin_config import PLEXOSConfig

    static_models = {}
    if "static_models" in defaults:
        static_models.update(defaults["static_models"])
    if "static_models_overlap" in defaults:
        static_models.update(defaults["static_models_overlap"])

    horizons_data = PLEXOSConfig.load_static_horizons()
    static_horizons = horizons_data.get("static_horizons", {})
    static_horizons_overlap = horizons_data.get("static_horizons_overlap", {})

    models = []
    horizons = []
    memberships = []

    for raw_horizon_name, horizon_data in static_horizons.items():
        horizon_name = _replace_year_in_name(raw_horizon_name, weather_year)
        attrs = _rewrite_horizon_attributes_for_weather_year(
            horizon_data["attributes"],
            horizon_name=horizon_name,
            weather_year=weather_year,
            is_overlap=False,
        )
        horizons.append(PLEXOSHorizon(name=horizon_name, **attrs))

    for raw_horizon_name, horizon_data in static_horizons_overlap.items():
        horizon_name = _replace_year_in_name(raw_horizon_name, weather_year)
        attrs = _rewrite_horizon_attributes_for_weather_year(
            horizon_data["attributes"],
            horizon_name=horizon_name,
            weather_year=weather_year,
            is_overlap=True,
        )
        horizons.append(PLEXOSHorizon(name=horizon_name, **attrs))

    for raw_model_name, model_data in static_models.items():
        model_name = _replace_year_in_name(raw_model_name, weather_year)
        model_category = (
            _replace_year_in_name(model_data.get("category", None), weather_year)
            if model_data.get("category", None) is not None
            else None
        )

        model = PLEXOSModel(name=model_name, category=model_category, **model_data.get("attributes", {}))
        models.append(model)

        if "memberships" in model_data:
            for membership_type, child_name in model_data["memberships"].items():
                rewritten_child_name = _replace_year_in_name(child_name, weather_year)
                memberships.append((model_name, rewritten_child_name, membership_type))

    return Ok(
        SimulationConfig(
            models=models,
            horizons=horizons,
            memberships=memberships,
            simulation_configs=dict(simulation_config) if simulation_config is not None else None,
        )
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

    if "models" not in config and "template" not in config and "horizon_year" not in config:
        return Err("Must specify 'horizon_year' in simulation config")

    # Route to appropriate builder based on config
    result: Result[SimulationConfig, str]
    if "static_models" in defaults and "static_horizons" in defaults:
        return _build_from_static_models(
            defaults,
            simulation_config,
            weather_year=config.get("horizon_year"),
        )

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
                simulation_configs=dict(simulation_config),
            )
        )

    return result


def _build_simple_simulation(
    config: dict[str, Any], defaults: dict[str, Any]
) -> Result[SimulationConfig, str]:
    """Build simulation from all models and horizons in defaults."""
    static_models = {}
    if "static_models" in defaults:
        static_models.update(defaults["static_models"])
    if "static_models_overlap" in defaults:
        static_models.update(defaults["static_models_overlap"])

    models = []
    horizons = {}
    memberships = []

    for model_name, model_data in static_models.items():
        model = PLEXOSModel(
            name=model_name, category=model_data.get("category", None), **model_data.get("attributes", {})
        )
        models.append(model)
        if "memberships" in model_data:
            for membership_type, child_name in model_data["memberships"].items():
                memberships.append((model_name, child_name, membership_type))
                if membership_type == "Horizon" and child_name not in horizons:
                    horizons[child_name] = PLEXOSHorizon(name=child_name)

    return Ok(
        SimulationConfig(
            models=models,
            horizons=list(horizons.values()),
            memberships=memberships,
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
    """
    Build a simulation configuration with monthly models and horizons for a given year.

    This function generates one model and one horizon for each month in the specified horizon_year.
    Each horizon covers the full month, and each model is associated with its corresponding horizon.
    Optional model and horizon properties can be provided via the config.

    Parameters
    ----------
    horizon_year : int
        The year for which to build monthly models and horizons.
    config : dict[str, Any]
        Configuration dictionary. Supports optional keys:
            - "model_properties": dict of properties to apply to each model.
            - "horizon_properties": dict of properties to apply to each horizon.
    defaults : dict[str, Any]
        Dictionary of default settings (not used in this function).

    Returns
    -------
    Result[SimulationConfig, str]
        Ok with SimulationConfig containing lists of models, horizons, and memberships,
        or Err with error message.

    Examples
    --------
    >>> result = _build_monthly_models(2012, {}, {})
    >>> build_result = result.unwrap()
    >>> len(build_result.models)
    12
    >>> len(build_result.horizons)
    12
    """
    models = []
    horizons = []
    memberships = []

    model_properties = config.get("model_properties", {})
    horizon_properties = config.get("horizon_properties", {})

    for month in range(1, 13):
        start_date = datetime(horizon_year, month, 1)
        _, last_day = calendar.monthrange(horizon_year, month)
        end_date = datetime(horizon_year, month, last_day, 23, 59, 59)
        days_in_month = (end_date - start_date).days + 1

        horizon_name = f"Horizon_{horizon_year}_M{month:02d}"
        horizon_data: dict[str, Any] = {
            "name": horizon_name,
            "chrono_date_from": datetime_to_ole_date(start_date),
            "date_from": datetime_to_ole_date(datetime(horizon_year, 1, 1)),
            "chrono_step_count": days_in_month,
            "chrono_step_type": 2,
            "step_count": 1,
            "periods_per_day": 24,
        }
        horizon_data.update(horizon_properties)
        horizon = PLEXOSHorizon(**horizon_data)
        horizons.append(horizon)

        model_name = f"Model_{horizon_year}_M{month:02d}"
        model_data: dict[str, Any] = {
            "name": model_name,
            "category": f"model_{horizon_year}",
        }
        model_data.update(model_properties)
        model = PLEXOSModel(**model_data)
        models.append(model)

        # Track membership with type
        memberships.append((model_name, horizon_name, "Horizon"))

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
    """
    Build a simulation configuration with weekly models and horizons for a given year.

    This function generates one model and one horizon for each week in the specified horizon_year.
    Each horizon covers a full week, and each model is associated with its corresponding horizon.
    Optional model and horizon properties can be provided via the config.

    Parameters
    ----------
    horizon_year : int
        The year for which to build weekly models and horizons.
    config : dict[str, Any]
        Configuration dictionary. Supports optional keys:
            - "model_properties": dict of properties to apply to each model.
            - "horizon_properties": dict of properties to apply to each horizon.
    defaults : dict[str, Any]
        Dictionary of default settings (not used in this function).

    Returns
    -------
    Result[SimulationConfig, str]
        Ok with SimulationConfig containing lists of models, horizons, and memberships,
        or Err with error message.

    Examples
    --------
    >>> result = _build_weekly_models(2012, {}, {})
    >>> build_result = result.unwrap()
    >>> len(build_result.models)
    52
    >>> len(build_result.horizons)
    52
    """
    models = []
    horizons = []
    memberships = []

    model_properties = config.get("model_properties", {})
    horizon_properties = config.get("horizon_properties", {})

    start_of_year = datetime(horizon_year, 1, 1)

    for week in range(1, 53):
        start_date = start_of_year + timedelta(weeks=week - 1)
        end_date = start_date + timedelta(days=6, hours=23, minutes=59, seconds=59)
        if end_date.year > horizon_year:
            end_date = datetime(horizon_year, 12, 31, 23, 59, 59)
        days_in_week = (end_date - start_date).days + 1

        horizon_name = f"Horizon_{horizon_year}_W{week:02d}"
        horizon_data: dict[str, Any] = {
            "name": horizon_name,
            "chrono_date_from": datetime_to_ole_date(start_date),
            "date_from": datetime_to_ole_date(start_of_year),
            "chrono_step_count": days_in_week,
            "chrono_step_type": 2,
            "step_count": 1,
            "periods_per_day": 24,
        }
        horizon_data.update(horizon_properties)
        horizon = PLEXOSHorizon(**horizon_data)
        horizons.append(horizon)

        model_name = f"Model_{horizon_year}_W{week:02d}"
        model_data: dict[str, Any] = {
            "name": model_name,
            "category": f"model_{horizon_year}",
        }
        model_data.update(model_properties)
        model = PLEXOSModel(**model_data)
        models.append(model)

        memberships.append((model_name, horizon_name, "Horizon"))

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
    """
    Build a simulation configuration with quarterly models and horizons for a given year.

    This function generates one model and one horizon for each quarter in the specified horizon_year.
    Each horizon covers the full quarter, and each model is associated with its corresponding horizon.
    Optional model and horizon properties can be provided via the config.

    Parameters
    ----------
    horizon_year : int
        The year for which to build quarterly models and horizons.
    config : dict[str, Any]
        Configuration dictionary. Supports optional keys:
            - "model_properties": dict of properties to apply to each model.
            - "horizon_properties": dict of properties to apply to each horizon.
    defaults : dict[str, Any]
        Dictionary of default settings (not used in this function).

    Returns
    -------
    Result[SimulationConfig, str]
        Ok with SimulationConfig containing lists of models, horizons, and memberships,
        or Err with error message.

    Examples
    --------
    >>> result = _build_quarterly_models(2012, {}, {})
    >>> build_result = result.unwrap()
    >>> len(build_result.models)
    4
    >>> len(build_result.horizons)
    4
    """
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
        start_date = datetime(horizon_year, months[0], 1)
        _, last_day = calendar.monthrange(horizon_year, months[-1])
        end_date = datetime(horizon_year, months[-1], last_day, 23, 59, 59)
        days_in_quarter = (end_date - start_date).days + 1

        horizon_name = f"Horizon_{horizon_year}_{quarter_name}"
        horizon_data: dict[str, Any] = {
            "name": horizon_name,
            "chrono_date_from": datetime_to_ole_date(start_date),
            "date_from": datetime_to_ole_date(datetime(horizon_year, 1, 1)),
            "chrono_step_count": days_in_quarter,
            "chrono_step_type": 2,
            "step_count": 1,
            "periods_per_day": 24,
        }
        horizon_data.update(horizon_properties)
        horizon = PLEXOSHorizon(**horizon_data)
        horizons.append(horizon)

        model_name = f"Model_{horizon_year}_{quarter_name}"
        model_data: dict[str, Any] = {
            "name": model_name,
            "category": f"model_{horizon_year}",
        }
        model_data.update(model_properties)
        model = PLEXOSModel(**model_data)
        models.append(model)

        memberships.append((model_name, horizon_name, "Horizon"))

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
    """
    Build a simulation configuration from a fully custom user specification.

    This function generates models and horizons based on the "models" key in the config,
    where each model specifies its own horizon with start/end dates and other attributes.
    Memberships between models and horizons are established automatically.

    Parameters
    ----------
    config : dict[str, Any]
        User configuration dictionary. Must include a "models" key, which is a list of model
        definitions. Each model definition must include:
            - "name": Name of the model
            - "horizon": Dictionary with keys:
                - "name": Name of the horizon (optional, defaults to "<model_name>_Horizon")
                - "start": Start date (ISO format string, required)
                - "end": End date (ISO format string, required)
                - "chrono_step_type": Step type (optional, default 2)
                - "chrono_step_count": Step count (optional, default is days in range)
                - "step_count": Step count (optional, default 1)
                - "periods_per_day": Periods per day (optional, default 24)
            - "category": Model category (optional, default "custom")
    defaults : dict[str, Any]
        Dictionary of default settings (not used in this function).

    Returns
    -------
    Result[SimulationConfig, str]
        Ok with SimulationConfig containing lists of models, horizons, and memberships,
        or Err with error message.

    Examples
    --------
    >>> config = {
    ...     "models": [
    ...         {
    ...             "name": "Summer_2012",
    ...             "horizon": {
    ...                 "name": "Summer_Horizon",
    ...                 "start": "2012-06-01",
    ...                 "end": "2012-08-31",
    ...                 "chrono_step_type": 2,
    ...                 "chrono_step_count": 91
    ...             }
    ...         }
    ...     ]
    ... }
    >>> result = _build_custom_simulation(config, {})
    >>> build_result = result.unwrap()
    >>> len(build_result.models)
    1
    >>> len(build_result.horizons)
    1
    """
    models = []
    horizons = []
    memberships = []

    for model_config in config["models"]:
        model_name = model_config["name"]
        horizon_config = model_config["horizon"]

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

        model = PLEXOSModel(
            name=model_name,
            category=model_config.get("category", "custom"),
        )
        models.append(model)

        memberships.append((model_name, horizon_name, "Horizon"))

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


def _add_model_attributes(db: PlexosDB, model: PLEXOSModel) -> None:
    """Add model attributes to the database."""
    metadata_fields = {"name", "category", "object_id", "uuid", "label", "description"}

    model_dict = model.model_dump(
        by_alias=True,
        exclude_none=True,
        exclude_defaults=True,
    )

    for attr_name, attr_value in model_dict.items():
        if attr_name in metadata_fields:
            continue

        db.add_attribute(
            ClassEnum.Model,
            model.name,
            attribute_name=attr_name,
            attribute_value=attr_value,
        )


def _ensure_base_diagnostic_defaults(db: PlexosDB) -> None:
    """Ensure default diagnostic checkboxes are enabled on base_diagnose.

    For each requested option, the first schema-supported name candidate is used
    and forced to checked (-1).

    Insert policy: upsert — existing values are overwritten by the defaults
    defined in BASE_DIAGNOSE_DEFAULT_ATTRIBUTE_CANDIDATES.
    """
    if not db.check_object_exists(ClassEnum.Diagnostic, BASE_DIAGNOSE_NAME):
        return

    for candidates, attr_value in BASE_DIAGNOSE_DEFAULT_ATTRIBUTE_CANDIDATES.items():
        attr_name: str | None = None
        for candidate in candidates:
            if validate_simulation_attribute(db, ClassEnum.Diagnostic, candidate).is_ok():
                attr_name = candidate
                break

        if attr_name is None:
            logger.debug("Skipping unsupported Diagnostic attribute candidates {}.", candidates)
            continue

        try:
            _set_or_add_attribute_value(
                db,
                class_enum=ClassEnum.Diagnostic,
                object_name=BASE_DIAGNOSE_NAME,
                attribute_name=attr_name,
                attribute_value=attr_value,
            )
        except Exception as exc:
            logger.debug(
                "Could not force Diagnostic attribute '{}' on '{}': {}",
                attr_name,
                BASE_DIAGNOSE_NAME,
                exc,
            )


def _ensure_transmission_defaults(db: PlexosDB, transmission_object_names: set[str]) -> None:
    """Ensure default Transmission options are enabled for selected objects.

    For each desired option, we try known attribute name candidates and set the
    first schema-supported one when missing.

    Insert policy: skip-if-exists — attributes that already carry a value are
    left untouched so that user-configured Transmission settings are preserved.
    Contrast with _ensure_performance_defaults / _ensure_production_defaults /
    _ensure_st_schedule_defaults / _ensure_base_diagnostic_defaults, which all
    upsert (overwrite) their target attributes unconditionally.
    """
    if not transmission_object_names:
        return

    for object_name in transmission_object_names:
        if not db.check_object_exists(ClassEnum.Transmission, object_name):
            continue

        for candidates, attr_value in TRANSMISSION_DEFAULT_ATTRIBUTE_CANDIDATES.items():
            selected_attr: str | None = None
            for attr_name in candidates:
                if validate_simulation_attribute(db, ClassEnum.Transmission, attr_name).is_ok():
                    selected_attr = attr_name
                    break

            if selected_attr is None:
                logger.debug("Skipping unsupported Transmission attributes {}.", candidates)
                continue

            try:
                current = db.get_attribute(
                    ClassEnum.Transmission,
                    object_name=object_name,
                    attribute_name=selected_attr,
                )
            except (NotFoundError, AssertionError):
                current = None

            if current is not None:
                continue

            try:
                db.add_attribute(
                    ClassEnum.Transmission,
                    object_name,
                    attribute_name=selected_attr,
                    attribute_value=attr_value,
                )
            except Exception as exc:
                logger.debug(
                    "Could not set default Transmission attribute '{}' on '{}': {}",
                    selected_attr,
                    object_name,
                    exc,
                )


def _set_or_add_attribute_value(
    db: PlexosDB,
    *,
    class_enum: ClassEnum,
    object_name: str,
    attribute_name: str,
    attribute_value: Any,
) -> None:
    """Upsert an object attribute value in t_attribute_data."""
    object_id = db.get_object_id(class_enum, object_name)
    attribute_id = db.get_attribute_id(class_enum, name=attribute_name)

    existing = db._db.fetchone(
        """
        SELECT 1
        FROM t_attribute_data
        WHERE object_id = ? AND attribute_id = ?
        """,
        (object_id, attribute_id),
    )

    if existing:
        db._db.execute(
            """
            UPDATE t_attribute_data
            SET value = ?
            WHERE object_id = ? AND attribute_id = ?
            """,
            (attribute_value, object_id, attribute_id),
        )
    else:
        db.add_attribute(
            class_enum,
            object_name,
            attribute_name=attribute_name,
            attribute_value=attribute_value,
        )


def _ensure_performance_defaults(db: PlexosDB, performance_object_names: set[str]) -> None:
    """Force default Performance options for selected objects.

    Insert policy: upsert — existing values are overwritten by the defaults
    defined in PERFORMANCE_DEFAULT_ATTRIBUTES.
    """
    if not performance_object_names:
        return

    for object_name in performance_object_names:
        if not db.check_object_exists(ClassEnum.Performance, object_name):
            continue

        for attr_name, attr_value in PERFORMANCE_DEFAULT_ATTRIBUTES.items():
            if validate_simulation_attribute(db, ClassEnum.Performance, attr_name).is_err():
                logger.debug("Skipping unsupported Performance attribute '{}'.", attr_name)
                continue

            try:
                _set_or_add_attribute_value(
                    db,
                    class_enum=ClassEnum.Performance,
                    object_name=object_name,
                    attribute_name=attr_name,
                    attribute_value=attr_value,
                )
            except Exception as exc:
                logger.debug(
                    "Could not set default Performance attribute '{}' on '{}': {}",
                    attr_name,
                    object_name,
                    exc,
                )


def _ensure_production_defaults(db: PlexosDB, production_object_names: set[str]) -> None:
    """Force default Production options for selected objects.

    Insert policy: upsert — existing values are overwritten by the defaults
    defined in PRODUCTION_DEFAULT_ATTRIBUTES.
    """
    if not production_object_names:
        return

    for object_name in production_object_names:
        if not db.check_object_exists(ClassEnum.Production, object_name):
            continue

        for attr_name, attr_value in PRODUCTION_DEFAULT_ATTRIBUTES.items():
            if validate_simulation_attribute(db, ClassEnum.Production, attr_name).is_err():
                logger.debug("Skipping unsupported Production attribute '{}'.", attr_name)
                continue

            try:
                _set_or_add_attribute_value(
                    db,
                    class_enum=ClassEnum.Production,
                    object_name=object_name,
                    attribute_name=attr_name,
                    attribute_value=attr_value,
                )
            except Exception as exc:
                logger.debug(
                    "Could not set default Production attribute '{}' on '{}': {}",
                    attr_name,
                    object_name,
                    exc,
                )


def _ensure_st_schedule_defaults(db: PlexosDB, st_schedule_object_names: set[str]) -> None:
    """Force default ST Schedule options for selected objects.

    Insert policy: upsert — existing values are overwritten by the defaults
    defined in ST_SCHEDULE_DEFAULT_ATTRIBUTES.
    """
    if not st_schedule_object_names:
        return

    for object_name in st_schedule_object_names:
        if not db.check_object_exists(ClassEnum.STSchedule, object_name):
            continue

        for attr_name, attr_value in ST_SCHEDULE_DEFAULT_ATTRIBUTES.items():
            if validate_simulation_attribute(db, ClassEnum.STSchedule, attr_name).is_err():
                logger.debug("Skipping unsupported ST Schedule attribute '{}'.", attr_name)
                continue

            try:
                _set_or_add_attribute_value(
                    db,
                    class_enum=ClassEnum.STSchedule,
                    object_name=object_name,
                    attribute_name=attr_name,
                    attribute_value=attr_value,
                )
            except Exception as exc:
                logger.debug(
                    "Could not set default ST Schedule attribute '{}' on '{}': {}",
                    attr_name,
                    object_name,
                    exc,
                )


def ingest_simulation_to_plexosdb(
    db: PlexosDB, result: SimulationConfig, validate: bool = True, scenario_name: str = "default"
) -> Result[dict[str, Any], str]:
    """
    Write simulation configuration to plexosdb.

    This function ingests models, horizons, their memberships, and simulation
    configuration objects (Performance, Production, etc.) into a PlexosDB instance.
    """
    horizon_ids: dict[str, int] = {}
    model_ids: dict[str, int] = {}

    logger.info(f"Creating {len(result.horizons)} horizon object(s)...")
    for horizon in result.horizons:
        # Check if horizon already exists
        if not db.check_object_exists(ClassEnum.Horizon, horizon.name):
            try:
                horizon_id = db.add_object(ClassEnum.Horizon, horizon.name)
            except Exception as e:
                return Err(f"Failed to add object {horizon.name}: {e}")
            horizon_ids[horizon.name] = horizon_id
            _add_horizon_attributes(db, horizon)
            logger.debug(f"Created horizon '{horizon.name}' (ID: {horizon_id})")
        else:
            horizon_ids[horizon.name] = db.get_object_id(ClassEnum.Horizon, horizon.name)
            logger.debug(f"Horizon '{horizon.name}' already exists.")

    logger.info(f"Creating {len(result.models)} model object(s)...")

    for model in result.models:
        if not db.check_object_exists(ClassEnum.Model, model.name):
            try:
                model_id = db.add_object(ClassEnum.Model, model.name, category=model.category)
            except Exception as e:
                return Err(f"Failed to add object {model.name}: {e}")
            model_ids[model.name] = model_id

            try:
                _add_model_attributes(db, model)
            except Exception as e:
                return Err(f"Failed to add attributes for model {model.name}: {e}")

            logger.debug(f"Created model '{model.name}' (ID: {model_id})")
        else:
            model_ids[model.name] = db.get_object_id(ClassEnum.Model, model.name)
            logger.debug(f"Model '{model.name}' already exists.")

    if not db.check_object_exists(ClassEnum.Scenario, scenario_name):
        db.add_object(ClassEnum.Scenario, scenario_name)

    for model_name in model_ids:
        try:
            db.add_membership(
                ClassEnum.Model,
                ClassEnum.Scenario,
                model_name,
                scenario_name,
                get_default_collection(ClassEnum.Scenario),
            )
            logger.debug(f"Linked scenario '{scenario_name}' → model '{model_name}'")
        except Exception as e:
            logger.warning(f"Failed to link scenario '{scenario_name}' to model '{model_name}': {e}")

    referenced_objects = {
        (child_name, membership_type) for _, child_name, membership_type in result.memberships
    }
    already_created = set(horizon_ids) | set(model_ids)
    for obj_name, membership_type in referenced_objects:
        if obj_name not in already_created:
            child_class_enum, _ = MEMBERSHIP_TYPE_MAP[membership_type]
            try:
                db.add_object(child_class_enum, obj_name)
                logger.debug(f"Created simulation config object '{obj_name}' as {child_class_enum.name}")
            except Exception:
                continue

    _ensure_base_diagnostic_defaults(db)

    transmission_object_names = {
        child_name
        for _, child_name, membership_type in result.memberships
        if membership_type == "Transmission"
    }
    _ensure_transmission_defaults(db, transmission_object_names)

    performance_object_names = {
        child_name
        for _, child_name, membership_type in result.memberships
        if membership_type == "Performance"
    }
    _ensure_performance_defaults(db, performance_object_names)

    production_object_names = {
        child_name for _, child_name, membership_type in result.memberships if membership_type == "Production"
    }

    st_schedule_object_names = {
        child_name
        for _, child_name, membership_type in result.memberships
        if membership_type == "ST Schedule"
    }

    logger.info(f"Creating {len(result.memberships)} model memberships...")
    seen_memberships: set[tuple[str, str, Any]] = set()
    for model_name, child_name, membership_type in result.memberships:
        child_class_enum, collection_enum = MEMBERSHIP_TYPE_MAP[membership_type]
        key = (model_name, child_name, collection_enum)
        if key in seen_memberships:
            continue
        seen_memberships.add(key)

        try:
            db.add_membership(
                ClassEnum.Model,
                child_class_enum,
                model_name,
                child_name,
                collection_enum,
            )
            logger.debug(f"Linked model '{model_name}' → '{child_name}' ({membership_type})")
        except Exception as exc:
            # Duplicate memberships can happen on retries/re-runs; skip them cheaply
            # instead of pre-checking existence with a database query per row.
            msg = str(exc).lower()
            if any(token in msg for token in ("unique", "duplicate", "already exists")):
                logger.debug(f"Membership already exists: {model_name} → {child_name} ({membership_type})")
                continue
            raise

    simulation_objects_added: list[dict[str, Any]] = []
    if not result.simulation_configs:
        logger.info("No simulation configuration objects to ingest")
    else:
        total_configs = len(result.simulation_configs)
        logger.info(f"Ingesting simulation configuration objects (0/{total_configs})...")

        for config_key, sim_config in result.simulation_configs.items():
            if sim_config is None:
                continue

            class_enum = CONFIG_CLASS_MAP.get(config_key)
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

    # Include simulation-config Production objects (e.g., Production_Example)
    # in addition to membership-linked Production objects (e.g., base MIP).
    if result.simulation_configs:
        production_config = result.simulation_configs.get("production")
        if production_config is not None and production_config.name:
            production_object_names.add(production_config.name)

        st_schedule_config = result.simulation_configs.get("st_schedule")
        if st_schedule_config is not None and st_schedule_config.name:
            st_schedule_object_names.add(st_schedule_config.name)

    _ensure_production_defaults(db, production_object_names)
    _ensure_st_schedule_defaults(db, st_schedule_object_names)

    logger.info("Simulation configuration successfully ingested into PlexosDB")

    return Ok(
        {
            "models": model_ids,
            "horizons": horizon_ids,
            "simulation_objects": simulation_objects_added,
        }
    )
