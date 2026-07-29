# r2x-plexos

> R2X plug-in for translating to and from PLEXOS XML databases.
>
> [![PyPI](https://img.shields.io/pypi/v/r2x-plexos.svg)](https://pypi.org/project/r2x-plexos/)
> [![License](https://img.shields.io/pypi/l/r2x-plexos.svg)](https://pypi.org/project/r2x-plexos/)
> [![Python](https://img.shields.io/pypi/pyversions/r2x-plexos.svg)](https://pypi.org/project/r2x-plexos/)
> [![CI](https://github.com/NatLabRockies/r2x-plexos/actions/workflows/ci.yaml/badge.svg)](https://github.com/NatLabRockies/r2x-plexos/actions/workflows/ci.yaml)
> [![codecov](https://codecov.io/gh/NatLabRockies/r2x-plexos/branch/main/graph/badge.svg)](https://codecov.io/gh/NatLabRockies/r2x-plexos)
> [![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
> [![Documentation](https://github.com/NatLabRockies/r2x-plexos/actions/workflows/docs.yaml/badge.svg?branch=main)](https://natlabrockies.github.io/r2x-plexos/)
> [![Docstring Coverage](https://natlabrockies.github.io/r2x-plexos/_static/docstr_coverage_badge.svg)](https://natlabrockies.github.io/r2x-plexos/)


R2X PLEXOS plugin for parsing and exporting PLEXOS power system models.

## Quick Start

### Installation

```bash
uv add r2x-plexos
```

### Parsing PLEXOS XML

```python
from pathlib import Path
from r2x_core import (
    DataFile,
    DataStore,
    PluginContext,
    Rule,
    System,
    apply_rules_to_context,
)
from r2x_plexos import PLEXOSParser, PLEXOSConfig

xml_path = /path/to/xml
db = PlexosDB.from_xml(xml_path)
model_names = db.list_objects_by_class(ClassEnum.Model)
model_name = model_names[1] if model_names else "Base"

plexos_config = PLEXOSConfig(
    model_name=model_name,
    horizon_year=2012,
    models=("r2x_plexos.models", "r2x_sienna.models"),)
data_file = DataFile(name="xml_file", fpath=xml_path)
store = DataStore(path=folder_path)
store.add_data([data_file])

context = PluginContext(
    config=plexos_config,
    store=store
)
parser = PLEXOSParser.from_context(context)
plexos_sys_result = parser.run()
plexos_sys = plexos_sys_result.system
context.source_system = plexos_sys
```

## Documentation Sections

- [Tutorials](https://natlabrockies.github.io/r2x-plexos/tutorials/) - Step-by-step learning guides
- [How-To Guides](https://natlabrockies.github.io/r2x-plexos/how-tos/) - Task-focused recipes
- [Explanations](https://natlabrockies.github.io/r2x-plexos/explanations/) - Architecture and design
- [References](https://natlabrockies.github.io/r2x-plexos/references/) - API and configuration reference

## Roadmap

If you're curious about what we're working on, check out the roadmap:

- [Active issues](https://github.com/NREL/r2x-core/issues?q=is%3Aopen+is%3Aissue+label%3A%22Working+on+it+%F0%9F%92%AA%22+sort%3Aupdated-asc): Issues that we are actively working on.
- [Prioritized backlog](https://github.com/NREL/r2x-core/issues?q=is%3Aopen+is%3Aissue+label%3ABacklog): Issues we'll be working on next.
- [Nice-to-have](https://github.com/NREL/r2x-core/labels/Optional): Nice to have features or Issues to fix. Anyone can start working on (please let us know before you do).
- [Ideas](https://github.com/NREL/r2x-core/issues?q=is%3Aopen+is%3Aissue+label%3AIdea): Future work or ideas for R2X Core.
