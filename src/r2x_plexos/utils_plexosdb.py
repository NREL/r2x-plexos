"""Helper functions to interact with PlexosDB."""

from dataclasses import dataclass
from datetime import datetime, timedelta

from loguru import logger
from plexosdb import ClassEnum, CollectionEnum, PlexosDB

from r2x_core import Err, Ok, Result


def get_collection_name(db: PlexosDB, collection_id: int) -> str | None:
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
    collection_name_result = db._db.fetchone(
        "SELECT name from t_collection where collection_id = ?",
        (collection_id,),
    )
    if collection_name_result is None:
        logger.debug("Collection not found for ID {}", collection_id)
        return None

    collection_name: str = collection_name_result[0]
    return collection_name.replace(" ", "")


def get_collection_enum(collection_name: str) -> CollectionEnum | None:
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
    # Check if collection_name is a valid enum member name
    if collection_name not in CollectionEnum.__members__:
        logger.warning(
            "Collection={} not found on `CollectionEnum`. Skipping it.",
            collection_name,
        )
        return None
    return CollectionEnum(collection_name)


@dataclass
class DateTimeRange:
    """Class to sabe a datetime range."""

    start: datetime
    end: datetime
    resolution: datetime


CHRONO_STEP_DELTAS = {
    -1: timedelta(seconds=1),
    0: timedelta(minutes=1),
    1: timedelta(hours=1),
    2: timedelta(days=1),
    3: timedelta(weeks=1),
}


def ole_date_to_datetime(ole_date: float) -> datetime:
    """Convert OLE Automation Date to Python datetime.

    OLE Automation Date is a floating-point value representing the number of days
    since December 30, 1899 at midnight. This format is used by PLEXOS for date storage.

    Parameters
    ----------
    ole_date : float
        The OLE Automation Date value

    Returns
    -------
    datetime
        The corresponding Python datetime object

    Examples
    --------
    >>> ole_date_to_datetime(47484.0)  # January 1, 2030
    datetime.datetime(2030, 1, 1, 0, 0)
    """
    ole_epoch = datetime(1899, 12, 30, 0, 0, 0)
    return ole_epoch + timedelta(days=ole_date)


def validate_simulation_attribute(
    db: PlexosDB,
    class_enum: ClassEnum,
    attribute_name: str,
) -> Result[None, str]:
    """
    Validate that an attribute name is valid for a simulation class.

    Uses db.list_attributes() to retrieve the valid attribute names for the
    specified class and checks if the given attribute name is in that list.

    Parameters
    ----------
    db : PlexosDB
        PlexosDB instance to validate against
    class_enum : ClassEnum
        The simulation class enum (e.g., ClassEnum.Performance)
    attribute_name : str
        The attribute name to validate

    Returns
    -------
    Result[None, str]
        Ok(None): If the attribute is valid
        Err(str): Error message with details if attribute is invalid

    Examples
    --------
    >>> result = validate_simulation_attribute(db, ClassEnum.Performance, "SOLVER")
    >>> assert result.is_ok()
    >>> result = validate_simulation_attribute(db, ClassEnum.Performance, "InvalidAttr")
    >>> assert result.is_err()
    """
    try:
        valid_attrs = db.list_attributes(class_enum)
        if attribute_name in valid_attrs:
            return Ok(None)
        else:
            # Show first 5 valid attributes as a hint
            hint = ", ".join(valid_attrs[:5])
            if len(valid_attrs) > 5:
                hint += f"... ({len(valid_attrs)} total)"
            return Err(
                f"Invalid attribute '{attribute_name}' for {class_enum.name}. "
                f"Valid attributes include: {hint}"
            )
    except Exception as e:
        return Err(f"Failed to validate attribute: {e!s}")


def resolve_horizon_for_model(db: PlexosDB, model_name: str) -> tuple[datetime, datetime] | None:
    """Return a date time range for the given horizon."""
    memberships = db.list_object_memberships(ClassEnum.Model, model_name, collection=CollectionEnum.Horizon)

    horizon_name = memberships[0]["child_name"]
    assert len(memberships) == 1, f"Multiple horizons attached to {model_name}"

    try:
        date_from = db.get_attribute(
            ClassEnum.Horizon, object_name=horizon_name, attribute_name="Chrono Date From"
        )[0]
    except AssertionError:
        date_from = None
    try:
        date_to = db.get_attribute(
            ClassEnum.Horizon, object_name=horizon_name, attribute_name="Chrono Date To"
        )[0]
    except AssertionError:
        date_to = None
    try:
        step_type = db.get_attribute(
            ClassEnum.Horizon, object_name=horizon_name, attribute_name="Chrono Step Type"
        )[0]
    except AssertionError:
        step_type = None
    try:
        step_count = db.get_attribute(
            ClassEnum.Horizon, object_name=horizon_name, attribute_name="Chrono Step Count"
        )[0]
    except AssertionError:
        step_count = None

    if not date_from and not date_to:
        return None

    date_from = ole_date_to_datetime(date_from)
    date_to = ole_date_to_datetime(date_to) if date_to else None
    step_type = int(step_type) if step_type is not None else 1
    step_count = int(step_count) if step_count is not None else 1
    step_delta = CHRONO_STEP_DELTAS.get(step_type, timedelta(hours=1))
    date_to = date_to or date_from + (step_delta * step_count)
    logger.info(
        "Horizon resolved: {} to {} (step_type={}, step_count={})", date_from, date_to, step_type, step_count
    )
    return (date_from, date_to)
