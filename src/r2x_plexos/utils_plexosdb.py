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
    # Check if collection_name is a valid enum member name
    if collection_name not in CollectionEnum.__members__:
        logger.warning(
            "Collection={} not found on `CollectionEnum`. Skipping it.",
            collection_name,
        )
        return None
    return CollectionEnum(collection_name)
