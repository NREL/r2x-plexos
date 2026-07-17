"""PLEXOS configuration class."""

import json
from pathlib import Path
from typing import Annotated, Any

import plexosdb as _plexosdb_pkg
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
        Selects the base XML template used to initialise the PLEXOS database.
        Accepts either:

        - A supported PLEXOS version key.  The corresponding master XML is
          taken from the ``plexosdb`` package (``plexosdb/config/``):

          ==================  =============================
          Key                 File (from plexosdb)
          ==================  =============================
          ``PLEXOS9.0``       master_9.2R6_btu.xml
          ``PLEXOS10.0``      master_10.0R2_btu.xml
          ``PLEXOS11.0``      master_11.0R4_btu.xml
          ``PLEXOS12.0``      master_12.0R3_btu.xml
          ==================  =============================

        - An absolute or relative file path to a custom XML template.

        When omitted the default template (``PLEXOS10.0``) is used.
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
    ...     template="PLEXOS9.0",
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
                "Selects the base XML template from the plexosdb package. "
                "Accepted version keys: 'PLEXOS9.0', 'PLEXOS10.0', 'PLEXOS11.0', 'PLEXOS12.0'. "
                "May also be a path to a custom XML file. "
                "Defaults to 'PLEXOS10.0' when omitted."
            ),
            default=None,
        ),
    ] = None
    simulation_config: Annotated[SimulationConfig | None, Field(description="Simulation configuration")] = (
        None
    )

    @classmethod
    def get_config_path(cls) -> Path:
        """Return the plexosdb config directory where XML templates live.

        If the class (or a subclass) provides a ``_resolve_config_path``
        method it takes precedence, allowing tests and subclasses to override
        the location without modifying this implementation.
        """
        resolve_method = getattr(cls, "_resolve_config_path", None)
        if resolve_method is not None:
            return Path(resolve_method(None))
        return Path(_plexosdb_pkg.__file__).parent / "config"

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
