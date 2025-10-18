"""Collection properties supplemental attribute."""

from typing import Annotated

from infrasys import SupplementalAttribute
from pydantic import Field

from .membership import PLEXOSMembership
from .property import PLEXOSPropertyValue


class CollectionProperties(SupplementalAttribute):
    """Properties defined on a collection membership."""

    membership: Annotated[PLEXOSMembership, Field(description="The membership this property belongs to.")]
    collection_name: Annotated[str, Field(description="Name of the collection.")]
    properties: Annotated[
        dict[str, PLEXOSPropertyValue],
        Field(description="Dictionary of property names to their values."),
    ]
