"""Plugin manifest for the r2x-plexos package."""

from __future__ import annotations

from r2x_core import PluginManifest, PluginSpec

from .config import PLEXOSConfig
from .exporter import PLEXOSExporter
from .parser import PLEXOSParser

manifest = PluginManifest(package="r2x-plexos")

manifest.add(
    PluginSpec.parser(
        name="r2x_plexos.parser",
        entry=PLEXOSParser,
        config=PLEXOSConfig,
        description="Parse PLEXOS XML models into an infrasys.System.",
    )
)

manifest.add(
    PluginSpec.exporter(
        name="r2x_plexos.exporter",
        entry=PLEXOSExporter,
        config=PLEXOSConfig,
        description="Export an infrasys.System to PLEXOS XML format.",
    )
)

__all__ = ["manifest"]
