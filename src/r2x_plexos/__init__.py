"""R2X PLEXOS Plugin.

A plugin for parsing PLEXOS model data into the R2X framework using infrasys components.
"""

from importlib.metadata import version

from loguru import logger

from .config import PLEXOSConfig
from .parser import PLEXOSParser

__version__ = version("r2x_plexos")


# Disable default loguru handler for library usage
# Applications using this library should configure their own handlers
logger.disable("r2x_plexos")


__all__ = [
    "PLEXOSConfig",
    "PLEXOSParser",
    "__version__",
]


def register_plugin() -> None:
    """Register plugin to R2X framework."""
    from r2x_core.plugins import PluginManager

    PluginManager.register_model_plugin(
        name="plexos",
        config=PLEXOSConfig,
        parser=PLEXOSParser,
        exporter=None,  # Will be implemented later
    )
