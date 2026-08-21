# Using r2x-plexos

`r2x-plexos` is the R2X Core plugin that translates between a PLEXOS XML
model and an R2X/Infrasys `System`. It can be used in two ways:

- as a Python library, when a workflow needs programmatic inspection or edits;
- as an R2X CLI plugin, when a repeatable pipeline connects PLEXOS to another
  R2X plugin.

The parser and exporter share the same `PLEXOSConfig` model. A parser reads an
existing XML model and its referenced data files. An exporter starts from a
PLEXOS template, writes the R2X system, creates a simulation configuration, and
writes XML and time-series CSV output.

## Install

Install the plugin and the R2X CLI in the same environment:

```console
uv add r2x-plexos
# or
python -m pip install r2x-plexos
```

Check that the plugin is discoverable:

```console
r2x list
```

The installed command is `r2x`. The plugin entry points are
`plexos-parser` and `plexos-exporter`. Some older workflows refer to this tool
as `r2x-cli`; use the command name provided by your R2X installation.

## PLEXOS inputs

A PLEXOS run normally consists of an XML database and, optionally, CSV files
referenced by PLEXOS datafile or variable objects. The parser needs:

- `model_name`: the PLEXOS Model object to read;
- `fpath`: either the XML file or a directory containing the XML file;
- `timeseries_dir`: an existing directory containing referenced time-series
  files, when they are not next to the XML file.

`horizon_year` and `weather_year` help the parser resolve simulation and weather
data. The parser also reads the horizon and scenario order from the PLEXOS
model itself.

## Python: parse a model

R2X plugins are initialized from a `PluginContext`. The parser returns a R2X
`Result`; the parsed system is available as `result.system` after a successful
run.

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
if result.is_err():
    raise RuntimeError(result.error)

system = result.system
if system is None:
    raise RuntimeError("Parser returned no system")
print(system.name)
```

When `fpath` is a directory, the parser selects the first XML file in that
directory if the store does not contain an `xml_file` entry. Supplying an
explicit `DataFile` is clearer when a directory contains multiple models.

## Inspect the parsed system

Components are typed Infrasys objects. Import the PLEXOS component class you
want to query and use the system accessors:

```python
from r2x_plexos.models import PLEXOSGenerator, PLEXOSNode

for generator in system.get_components(PLEXOSGenerator):
    print(generator.name, generator.max_capacity)

large_generators = list(
    system.get_components(
        PLEXOSGenerator,
        filter_func=lambda generator: generator.max_capacity > 500,
    )
)
node = system.get_component(PLEXOSNode, "Bus1")
```

The available component families include generators, storage, nodes, lines,
interfaces, regions, zones, fuels, reserves, purchasers, variables, datafiles,
and simulation objects. See the [data model explanation](../explanations/data-model.md)
for how these map to PLEXOS classes.

### Memberships and collection properties

PLEXOS relationships are represented as supplemental attributes. Memberships
can be inspected without changing the component objects:

```python
from r2x_plexos.models import PLEXOSMembership

for membership in system.get_supplemental_attributes(PLEXOSMembership):
    print(membership.parent_object, membership.collection, membership.child_object)
```

Properties attached to a PLEXOS collection membership are exposed through
`CollectionProperties`:

```python
from r2x_plexos.models.collection_property import CollectionProperties

collection_properties = system.get_supplemental_attributes_with_component(
    node, CollectionProperties
)
for item in collection_properties:
    print(item.collection_name, item.properties)
```

### Time series

The parser resolves time series from direct CSV references, PLEXOS datafile
objects, and PLEXOS variables. It attaches them to the corresponding system
component and trims them to the selected horizon when one is available:

```python
if system.has_time_series(generator):
    series = system.get_time_series(generator, "max_capacity")
    print(len(series.data), series.data[0])
```

Set `timeseries_dir` when the CSV files are stored outside the XML directory.
A missing or malformed reference is reported by the parser result and logs.

## Python: export a system

The exporter receives a system in its context. For a new XML database,
`horizon_year` is required because the exporter creates the model, horizon, and
simulation configuration before writing component data.

```python
from pathlib import Path
from typing import cast

from r2x_core import PluginContext
from r2x_plexos import PLEXOSConfig, PLEXOSExporter

config = PLEXOSConfig(
    model_name="Base_2024",
    horizon_year=2024,
    template="PLEXOS10.0",
    output_path="output",
)
context = PluginContext(config=config, system=system)
exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(context))

result = exporter.run()
if result.is_err():
    raise RuntimeError(result.error)

print(Path("output") / "Base_2024_2024.xml")
```

The exact XML filename includes configured model, horizon, and weather metadata.
Use the exporter logs or inspect the output directory rather than relying on a
hard-coded filename in downstream code.

Supported template keys are `PLEXOS9.0`, `PLEXOS10.0` (the default),
`PLEXOS11.0`, and `PLEXOS12.0`. `template` can also be a path to a custom XML
template. Time-series CSV files are written under the configured output
location and referenced by the generated XML.

By default, properties equal to their PLEXOS defaults are omitted to reduce
output size. To include them, set the exporter runtime option
`exclude_defaults = False` before calling `run()`.

## Round-trip conversion

A round trip parses an XML model, optionally changes the in-memory system, and
exports a new model:

```python
from pathlib import Path
from typing import cast

from r2x_core import DataFile, DataStore, PluginContext
from r2x_plexos import PLEXOSConfig, PLEXOSExporter, PLEXOSParser

input_path = Path("input/model.xml")
store = DataStore(path=input_path.parent)
store.add_data(DataFile(name="xml_file", fpath=input_path))
parse_context = PluginContext(
    config=PLEXOSConfig(fpath=str(input_path), model_name="Base", horizon_year=2024),
    store=store,
)
parser = cast(PLEXOSParser, PLEXOSParser.from_context(parse_context))
parse_result = parser.run()
if parse_result.is_err() or parse_result.system is None:
    raise RuntimeError(parse_result.error if parse_result.is_err() else "No parsed system")

system = parse_result.system
# Modify typed components or memberships here.

export_context = PluginContext(
    config=PLEXOSConfig(
        model_name="Modified",
        horizon_year=2024,
        output_path="output",
    ),
    system=system,
)
exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(export_context))
export_result = exporter.run()
if export_result.is_err():
    raise RuntimeError(export_result.error)
```

## Simulation configuration

The exporter creates simulation objects automatically from the configured
`horizon_year` and `resolution` (default `"1D"`). For reusable or custom
configurations, use the simulation utilities before integrating objects into a
PLEXOS database:

```python
from r2x_plexos.utils_simulation import build_plexos_simulation

simulation = build_plexos_simulation(
    {
        "horizon_year": 2024,
        "resolution": "1H",
    }
)
if simulation.is_err():
    raise RuntimeError(simulation.error)
built = simulation.unwrap()
print(len(built.models), len(built.horizons), len(built.memberships))
```

For most exports, prefer `PLEXOSConfig` and let `PLEXOSExporter.setup_configuration`
apply the package defaults. See `docs/simulation_builder.md` in the repository
when constructing custom models, horizons, or simulation configuration objects.

## R2X CLI: direct plugin mode

The CLI can invoke a plugin using JSON input. This is useful for testing a
single plugin or integrating it into a shell script:

```console
r2x run plugin plexos-parser --input parser-config.json --output system.json
```

Use `--show-help` to ask the installed plugin for its supported configuration
schema. `--input` reads JSON from a file, `--output` writes the plugin result to
a file, and `--dry-run` is available in pipeline mode.

For production workflows, use a pipeline so the parser output is passed
in-memory to the next plugin rather than serialized and manually reloaded.

## R2X CLI: create and run a pipeline

Create a starter file, then edit it with the parser, transformation, and
exporter steps:

```console
r2x init plexos-to-plexos.yaml
r2x run plexos-to-plexos.yaml round_trip --print
r2x run plexos-to-plexos.yaml round_trip --dry-run
r2x run plexos-to-plexos.yaml round_trip
```

A minimal parse/export pipeline is:

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

The parser produces the R2X system consumed by the exporter. To insert a
transformation, add its fully qualified plugin reference between those two
steps and add that plugin's configuration under `config`, for example:

```yaml
pipelines:
  p2s:
    - r2x-plexos.plexos-parser
    - r2x-plexos-to-sienna.plexos-to-sienna
    - r2x-sienna.sienna-exporter
```

Plugin references use the form `<installed-plugin>.<plugin-name>`. Discover
available references with `r2x list`. Keep shared paths and years in
`variables`, then use `${name}` in plugin configuration so one edit updates the
whole pipeline.

### Pipeline troubleshooting

- Run `r2x list` when a plugin reference cannot be found.
- Run with `--print` to inspect the resolved pipeline and `--dry-run` to check
  configuration without executing it.
- Ensure `fpath` points to the XML file or its containing run directory.
- Ensure `horizon_year` is set for an exporter creating a new database.
- Set `timeseries_dir` when referenced CSV files are not colocated with the
  XML input.
- Use `-v` or `-vv` for more detail; parser and exporter failures are returned
  as Result errors and also logged by the CLI.

## Related guides

- [Parsing PLEXOS data](parsing-plexos-data.md)
- [Exporting PLEXOS data](exporting-plexos-data.md)
- [Working with memberships](../how-tos/working-with-memberships.md)
- [Parser architecture](../explanations/parser-architecture.md)
- [Exporter architecture](../explanations/exporter-architecture.md)
- [API reference](../references/api.md)
