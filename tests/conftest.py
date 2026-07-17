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


@pytest.fixture(scope="session", autouse=True)
def cleanup_repo_data_folder(pytestconfig: pytest.Config):
    """Delete the Data/ folder created in the repo root as a side-effect of tests."""
    yield
    import shutil

    data_dir = pytestconfig.rootpath / "Data"
    if data_dir.exists() and data_dir.is_dir():
        shutil.rmtree(data_dir)


@pytest.fixture(scope="session")
def data_folder(pytestconfig: pytest.Config) -> Path:
    return pytestconfig.rootpath.joinpath(DATA_FOLDER)


@pytest.fixture(scope="session")
def simple_xml(data_folder: Path) -> Path:
    xml_path = data_folder.joinpath(SIMPLE_XML)
    return xml_path


# ---------------------------------------------------------------------------
# Shared exporter fixtures and helpers
# ---------------------------------------------------------------------------
from typing import TYPE_CHECKING, cast  # noqa: E402

from plexosdb import CollectionEnum, PlexosDB  # noqa: E402

if TYPE_CHECKING:
    from r2x_core import System

from r2x_plexos import PLEXOSConfig  # noqa: E402
from r2x_plexos.exporter import DEFAULT_XML_TEMPLATE  # noqa: E402


def is_valid_class_enum(class_enum):
    """Check if a ClassEnum has a corresponding CollectionEnum."""
    try:
        _ = CollectionEnum[class_enum.name]
        return True
    except KeyError:
        return False


@pytest.fixture
def plexos_config():
    from r2x_plexos import PLEXOSConfig

    return PLEXOSConfig(model_name="Base", horizon_year=2024)


@pytest.fixture
def template_db(plexos_config: PLEXOSConfig) -> PlexosDB:
    """Create a PlexosDB from the default template."""
    template_path = plexos_config.get_config_path().joinpath(DEFAULT_XML_TEMPLATE)
    return PlexosDB.from_xml(template_path)


@pytest.fixture
def serialized_plexos_system(tmp_path, db_all_gen_types, plexos_config) -> "System":
    from r2x_core import DataStore, PluginContext
    from r2x_plexos import PLEXOSParser

    store = DataStore(path=tmp_path)

    ctx = PluginContext(config=plexos_config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db_all_gen_types

    result = parser.run()
    sys = result.system
    assert sys is not None

    serialized_sys_fpath = tmp_path / "test_plexos_system.json"
    sys.to_json(serialized_sys_fpath)
    return sys
