# r2x-plexos

> R2X plug-in for translating to and from PLEXOS XML databases.
>
> [![image](https://img.shields.io/pypi/v/r2x.svg)](https://pypi.python.org/pypi/r2x-plexos)
> [![image](https://img.shields.io/pypi/l/r2x.svg)](https://pypi.python.org/pypi/r2x-plexos)
> [![image](https://img.shields.io/pypi/pyversions/r2x.svg)](https://pypi.python.org/pypi/r2x-plexos)
> [![CI](https://github.com/NREL/r2x/actions/workflows/CI.yaml/badge.svg)](https://github.com/NREL/r2x/actions/workflows/ci.yaml)
> [![codecov](https://codecov.io/gh/NREL/r2x-plexos/branch/main/graph/badge.svg)](https://codecov.io/gh/NREL/r2x-plexos)
> [![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
> [![Documentation](https://github.com/NREL/r2x-plexos/actions/workflows/docs.yaml/badge.svg?branch=main)](https://nrel.github.io/r2x-plexos/)
> [![Docstring Coverage](https://nrel.github.io/r2x-plexos/_static/docstr_coverage_badge.svg)](https://nrel.github.io/r2x-plexos/)


R2X PLEXOS plugin for parsing and exporting PLEXOS power system models.

## Quick Start

### Installation

```bash
uv add r2x-plexos
```

### Parsing PLEXOS XML

```python
from pathlib import Path
from r2x_core import DataFile, DataStore
from r2x_plexos import PLEXOSParser, PLEXOSConfig

# Parse PLEXOS XML
config = PLEXOSConfig(model_name="Base", reference_year=2024)
store = DataStore(path=Path("data"))
store.add_data(DataFile(name="xml_file", glob="*.xml"))

parser = PLEXOSParser(config, store)
system = parser.build_system()
```

## Documentation Sections

- [Tutorials](https://nrel.github.io/r2x-plexos/tutorials/) - Step-by-step learning guides
- [How-To Guides](https://nrel.github.io/r2x-plexos/how-tos/) - Task-focused recipes
- [Explanations](https://nrel.github.io/r2x-plexos/explanations/) - Architecture and design
- [References](https://nrel.github.io/r2x-plexos/explanations/references/) - API and configuration reference

## Roadmap

If you're curious about what we're working on, check out the roadmap:

- [Active issues](https://github.com/NREL/r2x-core/issues?q=is%3Aopen+is%3Aissue+label%3A%22Working+on+it+%F0%9F%92%AA%22+sort%3Aupdated-asc): Issues that we are actively working on.
- [Prioritized backlog](https://github.com/NREL/r2x-core/issues?q=is%3Aopen+is%3Aissue+label%3ABacklog): Issues we'll be working on next.
- [Nice-to-have](https://github.com/NREL/r2x-core/labels/Optional): Nice to have features or Issues to fix. Anyone can start working on (please let us know before you do).
- [Ideas](https://github.com/NREL/r2x-core/issues?q=is%3Aopen+is%3Aissue+label%3AIdea): Future work or ideas for R2X Core.
