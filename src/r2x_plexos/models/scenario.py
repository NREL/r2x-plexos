"""The following file contains Pydantic models for a PLEXOS Scenario model."""

from typing import Annotated

from pydantic import Field

from .component import PLEXOSObject


class PLEXOSScenario(PLEXOSObject):
    """PLEXOS Scenario class."""

    locked: Annotated[
        int,
        Field(
            description="If the scenario is locked (cannot be modified)",
            ge=0,
            le=1,
        ),
    ] = 0
    read_order: Annotated[
        int | None,
        Field(description="Order in which to read scenario data (last read scenario has highest priority)"),
    ] = None

    @classmethod
    def example(cls) -> "PLEXOSScenario":
        """Create an example PLEXOSFuel."""
        return PLEXOSScenario(
            name="scenario-01",
        )
