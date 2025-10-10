"""PLEXOS parser implementation for r2x-core framework."""

from infrasys import Component
from loguru import logger

from r2x_core import BaseParser, DataStore

from . import __version__
from .config import PLEXOSConfig


class PLEXOSParser(BaseParser):
    """PLEXOS parser."""

    def __init__(
        self,
        config: PLEXOSConfig,
        data_store: DataStore,
        *,
        name: str | None = None,
        auto_add_composed_components: bool = True,
        skip_validation: bool = False,
    ) -> None:
        """Initialize PLEXOS parser."""
        super().__init__(
            config,
            data_store,
            name=name,
            auto_add_composed_components=auto_add_composed_components,
            skip_validation=skip_validation,
        )

        self.config: PLEXOSConfig = config

    def build_system_components(self) -> None:
        """Create PLEXOS components."""
        logger.info("Building PLEXOS system components...")

        total_components = len(list(self.system.get_components(Component)))
        logger.info(
            "Built {} components",
            total_components,
        )

    def build_time_series(self) -> None:
        """Attach time series data to components."""
        logger.info("Building time series data...")
        logger.info("Time series attachment complete")

    def post_process_system(self) -> None:
        """Perform post-processing on the built system."""
        logger.info("Post-processing PLEXOS system...")

        self.system.data_format_version = __version__
        self.system.description = f"PLEXOS system for model'{self.config.model_name}"

        total_components = len(list(self.system.get_components(Component)))
        logger.info("System name: {}", self.system.name)
        logger.info("Total components: {}", total_components)
        logger.info("Post-processing complete")
