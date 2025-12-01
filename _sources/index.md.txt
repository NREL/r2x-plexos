```{toctree}
:maxdepth: 2
:hidden:

install
tutorials/index
how-tos/index
explanations/index
references/index
```

# R2X PLEXOS Documentation

R2X PLEXOS is an R2X Core plugin for parsing and exporting PLEXOS power system models.

## About R2X PLEXOS

R2X PLEXOS provides comprehensive parser and exporter functionality for PLEXOS models, enabling seamless data exchange with other power system modeling platforms through the R2X Core framework.

**Key Features:**
- Parse PLEXOS XML files to translate to other R2X supported modeling platforms
- Export R2X supported models into PLEXOS XML database
- Round-trip data conversion

## Quick Start

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

- [Tutorials](tutorials/index.md) - Step-by-step learning guides
- [How-To Guides](how-tos/index.md) - Task-focused recipes
- [Explanations](explanations/index.md) - Architecture and design
- [References](references/index.md) - API and configuration reference

## Resources

- [API Reference](references/api.md) - Complete API documentation
- [R2X Core](https://github.com/NREL/r2x-core) - Core framework documentation

## Indices and Tables

- {ref}`genindex`
- {ref}`modindex`
- {ref}`search`
