"""Helper functions to interact with PlexosDB."""

from loguru import logger
from plexosdb import CollectionEnum, PlexosDB


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
    if collection_name not in CollectionEnum:
        logger.warning(
            "Collection={} not found on `CollectionEnum`. Skipping it.",
            collection_name,
        )
        return None
    return CollectionEnum(collection_name)


def apply_action(base_value: float, new_value: float, action: str | None) -> float:
    """Apply a PLEXOS action operation to combine values.

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
