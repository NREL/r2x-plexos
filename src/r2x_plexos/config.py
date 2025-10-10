from typing import Annotated

from pydantic import Field

from r2x_core.plugin_config import PluginConfig


class PLEXOSConfig(PluginConfig):
    model_name: Annotated[str, Field(description="Name of the model to parse.")]
