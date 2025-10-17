"""The following file contains Pydantic models for a PLEXOS data files."""

from typing import Annotated

from pydantic import Field

from .component import PLEXOSObject
from .property import PLEXOSPropertyValue
from .property_specification import PLEXOSProperty


class PLEXOSDatafile(PLEXOSObject):
    """Class that holds attributes about PLEXOS datafiles."""

    filename: Annotated[
        PLEXOSPropertyValue | None,
        PLEXOSProperty,
        Field(alias="Filename", description="Data file used in the simulation"),
    ] = None

    @classmethod
    def example(cls) -> "PLEXOSDatafile":
        """Create an example PLEXOSDatafile."""
        return PLEXOSDatafile(
            name="ExampleFuel",
        )
