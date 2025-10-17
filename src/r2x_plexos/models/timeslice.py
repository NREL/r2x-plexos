"""The following file contains Pydantic models for a PLEXOS timeslice model."""

from typing import Annotated

from pydantic import Field

from .component import PLEXOSConfiguration
from .property_specification import PLEXOSProperty


class PLEXOSTimeslice(PLEXOSConfiguration):
    """Class that holds attributes about PLEXOS Fuels for thermal generators."""

    include: Annotated[
        int,
        PLEXOSProperty(is_enum=True),
        Field(
            alias="Include",
            description="If the timeslice includes the period. (-1 for True, 0 for False)",
        ),
    ] = -1

    @classmethod
    def example(cls) -> "PLEXOSTimeslice":
        """Create an example PLEXOSTimeslice."""
        return PLEXOSTimeslice(name="timeslice_01", include=-1)
