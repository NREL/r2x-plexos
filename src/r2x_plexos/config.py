"""PLEXOS configuration class."""

from typing import Annotated

from pydantic import DirectoryPath, Field, FilePath

from r2x_core.plugin_config import PluginConfig


class PLEXOSConfig(PluginConfig):
    """PLEXOS configuration class."""

    model_name: Annotated[str, Field(description="Name of the PLEXOS model.")]
    timeseries_dir: Annotated[
        DirectoryPath | None,
        Field(
            description="Optional subdirectory containing time series files. If passed it must exist.",
            default=None,
        ),
    ]
    horizon_year: Annotated[int | None, Field(description="Horizon year", default=None)]
    template: Annotated[
        FilePath | None, Field(description="File to the XML to use as template. If passed it must exist.")
    ] = None
