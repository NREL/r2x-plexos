"""PLEXOS configuration class."""

from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any

from pydantic import DirectoryPath, Field, FilePath

from r2x_core.plugin_config import PluginConfig
from r2x_plexos.utils_simulation import SimulationConfig


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
    simulation_config: Annotated[SimulationConfig | None, Field(description="Simulation configuration")] = (
        None
    )

    @classmethod
    def get_config_path(cls) -> Path:
        """Return the plugin's configuration directory path."""
        resolve_method: Callable[[Any], Path] | None = getattr(cls, "_resolve_config_path", None)
        if resolve_method:
            return resolve_method(None)
        return cls._package_config_path()
