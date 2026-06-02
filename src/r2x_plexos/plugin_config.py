"""PLEXOS configuration class."""

import json
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any

from pydantic import DirectoryPath, Field

from r2x_core.plugin_config import PluginConfig
from r2x_plexos.utils_simulation import SimulationConfig


class PLEXOSConfig(PluginConfig):
    """Configuration for PLEXOS model parser.

    This configuration class defines all parameters needed to parse
    PLEXOS model data, including model identification, time series handling,
    and simulation settings. Model-specific defaults and constants should be
    loaded using the `load_defaults()` class method and used in parser logic.

    Parameters
    ----------
    fpath : str, optional
        Path to the PLEXOS run directory or XML file. If not provided, the parser will attempt to locate the model using default paths or configuration.
    model_name : str, optional
        Name of the PLEXOS model. Defaults to "default".
    timeseries_dir : DirectoryPath, optional
        Optional subdirectory containing time series files. If passed it must exist.
    horizon_year : int, optional
        Horizon year for the model simulation
    solve_year : int, optional
        Solve year for simulation configuration. If not provided, it will be set to the same value
    output_path : str, optional
        Alias for output directory. If provided, it will override `timeseries_dir` for time
    template : str, optional
        Can be either:
        - a file path to an XML template, or
        - a supported version key (e.g. "PLEXOS9.2")
    simulation_config : SimulationConfig, optional
        Simulation configuration parameters

    Examples
    --------
    Basic configuration with model name:

    >>> config = PLEXOSConfig(
    ...     model_name="MyPLEXOSModel",
    ...     horizon_year=2030,
    ... )

    Full configuration with time series and simulation:

    >>> config = PLEXOSConfig(
    ...     model_name="MyPLEXOSModel",
    ...     timeseries_dir=Path("./timeseries"),
    ...     horizon_year=2030,
    ...     template="PLEXOS9.2",
    ...     simulation_config=SimulationConfig(...),
    ... )

    See Also
    --------
    r2x_core.plugin_config.PluginConfig : Base configuration class
    r2x_plexos.utils_simulation.SimulationConfig : Simulation configuration class
    load_defaults : Class method to load default constants from JSON
    """

    fpath: Annotated[
        str | None, Field(description="Path to the PLEXOS run directory or XML file", default=None)
    ] = None
    model_name: Annotated[
        str,
        Field(description="Name of the PLEXOS model.", default="default"),
    ]
    timeseries_dir: Annotated[
        DirectoryPath | None,
        Field(
            description="Optional subdirectory containing time series files. If passed it must exist.",
            default=None,
        ),
    ] = None
    horizon_year: Annotated[int | None, Field(description="Horizon year", default=None)] = None
    weather_year: Annotated[int | None, Field(description="Weather year", default=None)] = None
    output_path: Annotated[str | None, Field(description="Alias for output directory", default=None)] = None
    template: Annotated[
        str | None,
        Field(
            description=(
                "Template selector. Can be either an existing XML file path "
                "or a supported version key such as 'PLEXOS9.2' or 'PLEXOS10.0'."
            ),
            default=None,
        ),
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

    @classmethod
    def load_defaults(cls) -> dict[str, Any]:
        """Load default configuration from defaults.json."""
        config_path = Path(__file__).parent / "config" / "defaults.json"
        with open(config_path) as f:
            return dict(json.load(f))

    @classmethod
    def load_static_models(cls) -> dict[str, Any]:
        """Load static models and horizons from JSON."""
        config_path = Path(__file__).parent / "config" / "plexos_models.json"
        with open(config_path) as f:
            return dict(json.load(f))

    @classmethod
    def load_static_horizons(cls) -> dict[str, Any]:
        """Load static horizons from JSON."""
        config_path = Path(__file__).parent / "config" / "plexos_horizons.json"
        with open(config_path) as f:
            return dict(json.load(f))

    @classmethod
    def load_reports(cls) -> list[dict[str, Any]]:
        """Load report definitions from plexos_reports.json."""
        config_path = Path(__file__).parent / "config" / "plexos_reports.json"
        with open(config_path) as f:
            return list(json.load(f))
