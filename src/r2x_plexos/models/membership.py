"""Class to define memberships between various plexos objects."""

from typing import Annotated

from infrasys import SupplementalAttribute
from plexosdb.enums import CollectionEnum
from pydantic import Field

from .component import PLEXOSObject


# NOTE: This should be a supplemental attribute
class PLEXOSMembership(SupplementalAttribute):
    """PLEXOS membership."""

    membership_id: int | None = None
    parent_object: Annotated[PLEXOSObject, Field(description="Parent object of the membership.")]
    child_object: Annotated[PLEXOSObject, Field(description="Child object of the membership.")]
    collection: Annotated[CollectionEnum | None, Field(description="Collection of the membership.")] = None
