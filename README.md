<h1 align="center">r2x-plexos</h1>

<p align="center">
    <strong>Translate power-system models between PLEXOS XML and the R2X ecosystem.</strong>
</p>

<p align="center">
    <a href="https://pypi.org/project/r2x-plexos/"><img src="https://img.shields.io/pypi/v/r2x-plexos.svg" alt="PyPI version"></a>
    <a href="https://pypi.org/project/r2x-plexos/"><img src="https://img.shields.io/pypi/pyversions/r2x-plexos.svg" alt="Supported Python versions"></a>
    <a href="https://github.com/NatLabRockies/r2x-plexos/actions/workflows/ci.yaml"><img src="https://github.com/NatLabRockies/r2x-plexos/actions/workflows/ci.yaml/badge.svg" alt="CI status"></a>
    <a href="https://natlabrockies.github.io/r2x-plexos/"><img src="https://github.com/NatLabRockies/r2x-plexos/actions/workflows/docs.yaml/badge.svg?branch=main" alt="Documentation build"></a>
    <a href="https://codecov.io/gh/NatLabRockies/r2x-plexos"><img src="https://codecov.io/gh/NatLabRockies/r2x-plexos/branch/main/graph/badge.svg" alt="Coverage"></a>
</p>

`r2x-plexos` is an [R2X Core](https://github.com/NatLabRockies/r2x-core)
plugin for reading PLEXOS XML databases into typed R2X/Infrasys systems and
writing those systems back to PLEXOS XML. It supports both Python workflows
and repeatable `r2x` CLI pipelines.

## What it does

- **Parse PLEXOS models**: Read model objects, properties, memberships, scenarios, horizons, and referenced time series.
- **Export R2X systems**: Create PLEXOS XML databases from R2X systems, including memberships and time-series CSV files.
- **Round-trip models**: Parse, inspect or transform a system, then export a new PLEXOS model.
- **Configure simulations**: Build models, horizons, and simulation settings for the exported database.
- **Compose pipelines**: Connect the PLEXOS parser and exporter with other R2X plugins through YAML pipelines.

## Installation

Using [uv](https://docs.astral.sh/uv/):

```console
uv add r2x-plexos
```

Or using pip:

```console
python -m pip install r2x-plexos
```

The package supports Python 3.11, 3.12, and 3.13. To verify installation and
plugin discovery:

```console
python -c "import r2x_plexos; print(r2x_plexos.__version__)"
r2x list
```

The CLI executable is `r2x`. Its discovered plugin names include
`plexos-parser` and `plexos-exporter`.

## Python quick start

### Parse a PLEXOS XML model

The parser is initialized with an R2X Core `PluginContext`. `fpath` can point
to an XML file or to a directory containing one.

```python
from pathlib import Path
from typing import cast

from r2x_core import DataFile, DataStore, PluginContext
from r2x_plexos import PLEXOSConfig, PLEXOSParser

xml_path = Path("input/model.xml")
store = DataStore(path=xml_path.parent)
store.add_data(DataFile(name="xml_file", fpath=xml_path))

config = PLEXOSConfig(
        fpath=str(xml_path),
        model_name="Base",
        horizon_year=2024,
        timeseries_dir=xml_path.parent,
)
context = PluginContext(config=config, store=store)
parser = cast(PLEXOSParser, PLEXOSParser.from_context(context))

result = parser.run()
if result.is_err() or result.system is None:
        raise RuntimeError(result.error if result.is_err() else "No system returned")

system = result.system
print(system.name)
```

Parsed components can be queried through the R2X system:

```python
from r2x_plexos.models import PLEXOSGenerator

for generator in system.get_components(PLEXOSGenerator):
        print(generator.name, generator.max_capacity)
```

### Export a system to PLEXOS

Pass the parsed or transformed system to the exporter through a context. The
exporter creates the simulation configuration and writes XML output using a
bundled PLEXOS template.

```python
from typing import cast

from r2x_core import PluginContext
from r2x_plexos import PLEXOSConfig, PLEXOSExporter

export_config = PLEXOSConfig(
        model_name="Base_2024",
        horizon_year=2024,
        template="PLEXOS10.0",
        output_path="output",
)
export_context = PluginContext(config=export_config, system=system)
exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(export_context))

result = exporter.run()
if result.is_err():
        raise RuntimeError(result.error)
```

Supported templates include `PLEXOS9.0`, `PLEXOS10.0`, `PLEXOS11.0`, and
`PLEXOS12.0`. A custom XML template path can also be supplied through
`PLEXOSConfig.template`.

## R2X CLI pipelines

Use a pipeline when several plugins should run as one reproducible workflow.
Create a starter file with:

```console
r2x init plexos-pipeline.yaml
```

The following example parses a PLEXOS model and exports it again:

```yaml
variables:
    input_xml: /data/input/model.xml
    output_dir: /data/output
    model_name: Base
    horizon_year: 2024
    template: PLEXOS10.0

pipelines:
    round_trip:
        - r2x-plexos.plexos-parser
        - r2x-plexos.plexos-exporter

config:
    r2x-plexos.plexos-parser:
        fpath: ${input_xml}
        model_name: ${model_name}
        horizon_year: ${horizon_year}

    r2x-plexos.plexos-exporter:
        model_name: ${model_name}
        horizon_year: ${horizon_year}
        template: ${template}
        output_path: ${output_dir}

output_folder: ${output_dir}
```

Inspect and run the pipeline with:

```console
r2x list
r2x run plexos-pipeline.yaml round_trip --print
r2x run plexos-pipeline.yaml round_trip --dry-run
r2x run plexos-pipeline.yaml round_trip
```

Transformations from other R2X plugins can be inserted between the parser and
exporter. For example:

```yaml
pipelines:
    p2s:
        - r2x-plexos.plexos-parser
        - r2x-plexos-to-sienna.plexos-to-sienna
        - r2x-sienna.sienna-exporter
```

Use `${variable}` references for shared paths and years, and use `r2x list` to
find installed plugin references.

## Documentation

The complete documentation includes tutorials, task-focused recipes,
architecture explanations, API details, and pipeline examples:

- [Documentation site](https://natlabrockies.github.io/r2x-plexos/)
- [Complete usage guide](https://natlabrockies.github.io/r2x-plexos/tutorials/using-r2x-plexos.html)
- [Tutorials](https://natlabrockies.github.io/r2x-plexos/tutorials/)
- [How-to guides](https://natlabrockies.github.io/r2x-plexos/how-tos/)
- [API reference](https://natlabrockies.github.io/r2x-plexos/references/)

## Contributing

- [Issues](https://github.com/NatLabRockies/r2x-plexos/issues)
- [Pull requests](https://github.com/NatLabRockies/r2x-plexos/pulls)
- [Labels](https://github.com/NatLabRockies/r2x-plexos/labels)

## License

This project is distributed under the [BSD 3-Clause License](LICENSE.txt).
