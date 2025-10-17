"""PLEXOS configuration class."""

from typing import Annotated

from pydantic import Field

from r2x_core.plugin_config import PluginConfig


class PLEXOSConfig(PluginConfig):
    """PLEXOS configuration class."""

    model_name: Annotated[str, Field(description="Name of the PLEXOS model.")]
    timeseries_dir: Annotated[
        str | None, Field(description="Optional subdirectory containing time series files.", default=None)
    ]
    reference_year: Annotated[
        int | None, Field(description="Reference year for time series parsing", default=None)
    ]
