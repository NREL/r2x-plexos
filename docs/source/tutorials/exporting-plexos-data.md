# Exporting PLEXOS Data

This tutorial shows how to export an R2X system to PLEXOS XML format.

## Basic Export

```python
from r2x_plexos import PLEXOSExporter, PLEXOSConfig

# Configure the exporter
config = PLEXOSConfig(
    model_name="Base",
    horizon_year=2024,
)

# Create exporter with an existing system
exporter = PLEXOSExporter(config, system)

# Export to XML
result = exporter.export()
if result.is_ok():
    print("Export successful!")
else:
    print(f"Export failed: {result.error}")
```

## Simulation Configuration

User may specify a custom simulation configuration using the simulation_config keyword argument shown below. This example currently calls a function that sets the defaults.

```python
from r2x_plexos.utils_simulation import get_default_simulation_config

config = PLEXOSConfig(
    model_name="MyModel",
    horizon_year=2024,
    simulation_config=get_default_simulation_config(),
)

exporter = PLEXOSExporter(config, system)
```

## Populating Default Values in exported Database

By default, the exporter excludes properties with default values. However, if the user wishes, they can include properties with default values. Note that this affects the performance when populating the XML database. 

```python
# Include all properties, even defaults
exporter = PLEXOSExporter(
    config,
    system,
    exclude_defaults=False
)
```


## Round-Trip Example

Parse and re-export PLEXOS data:

```python
from pathlib import Path
from r2x_core import DataFile, DataStore
from r2x_plexos import PLEXOSParser, PLEXOSExporter, PLEXOSConfig

# Parse original XML
config = PLEXOSConfig(model_name="Base", horizon_year=2024)
store = DataStore(folder=Path("input"))
store.add_data_file(DataFile(name="xml_file", glob="*.xml"))

parser = PLEXOSParser(config, store)
system = parser.build_system()

# Modify system as needed
# ... add/modify components ...

# Export to new XML
export_config = PLEXOSConfig(
    model_name="Modified",
    horizon_year=2024
)
exporter = PLEXOSExporter(export_config, system)
result = exporter.export()
```

## Export Workflow

The exporter follows these steps:

1. **Setup Configuration**: Creates models, horizons, and simulation config
2. **Prepare Export**: Adds component objects to database
3. **Export Time Series**: Writes time series to CSV files
4. **Postprocess Export**: Adds properties, memberships, and exports XML
5. **Validate Export**: Optional validation step

## Working with Memberships

Memberships are automatically exported:

```python
from r2x_plexos.models import PLEXOSMembership

# Memberships are stored as supplemental attributes
memberships = list(system.get_supplemental_attributes(PLEXOSMembership))

# Export includes all memberships automatically
exporter = PLEXOSExporter(config, system)
result = exporter.export()
```

## Output Files

The exporter generates:

- **XML file**: Main PLEXOS model file (`{model_name}.xml`)
- **CSV files**: Time series data files (if time series exist)
- **Directory structure**: Organized in output folder

## Configuration Options

Key exporter parameters:

- `model_name`: Name for the exported model
- `horizon_year`: Required for new databases
- `resolution`: Time resolution (e.g., "1D", "1H")
- `exclude_defaults`: Skip properties with default values (default: True)
- `plexos_scenario`: Scenario name (default: uses model_name)

## Error Handling

The exporter uses Result types for error handling:

```python
result = exporter.export()

if result.is_ok():
    print("Success!")
else:
    error = result.error
    print(f"Failed: {error}")
```

## Next Steps

- Review [parsing tutorial](parsing-plexos-data.md)
- Check [API reference](../references/api.md) for detailed options
