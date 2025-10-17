from pathlib import Path

import pytest

DATA_FOLDER = "tests/data"
SIMPLE_XML = "5_bus_system_variables.xml"


@pytest.fixture
def data_folder(pytestconfig: pytest.Config) -> Path:
    return pytestconfig.rootpath.joinpath(DATA_FOLDER)


@pytest.fixture
def simple_xml(data_folder: Path) -> Path:
    xml_path = data_folder.joinpath(SIMPLE_XML)
    return xml_path
