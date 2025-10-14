"""Entry point for plugin system."""

from r2x_core.plugins import PluginManager

from .config import PLEXOSConfig
from .parser import PLEXOSParser


def register_plugin() -> None:
    """Register plugin to R2X framework."""
    PluginManager.register_model_plugin(
        name="plexos",
        config=PLEXOSConfig,
        parser=PLEXOSParser,
        exporter=None,
    )
