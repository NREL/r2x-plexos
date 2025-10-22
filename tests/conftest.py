import pathlib
import sys
from pathlib import Path

import pytest
from loguru import logger

from r2x_plexos.models.context import set_horizon, set_scenario_priority

DATA_FOLDER = "tests/data"
SIMPLE_XML = "5_bus_system_variables.xml"

ROOT = pathlib.Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytest_plugins = [
    "fixtures.example_dbs",
    "fixtures.data_files",
]


@pytest.fixture
def caplog(caplog):
    logger.enable("r2x_plexos")
    handler_id = logger.add(caplog.handler, format="{message}")
    yield caplog
    logger.remove(handler_id)


@pytest.fixture(autouse=True)
def reset_global_context():
    """Reset global scenario priority and horizon context between tests."""
    set_scenario_priority(None)
    set_horizon(None)
    yield
    set_scenario_priority(None)
    set_horizon(None)


@pytest.fixture(scope="session")
def data_folder(pytestconfig: pytest.Config) -> Path:
    return pytestconfig.rootpath.joinpath(DATA_FOLDER)


@pytest.fixture(scope="session")
def simple_xml(data_folder: Path) -> Path:
    xml_path = data_folder.joinpath(SIMPLE_XML)
    return xml_path
