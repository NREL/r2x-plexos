# Exporting PLEXOS Data

## Export System to XML

```python
from r2x_plexos import PLEXOSExporter, PLEXOSConfig

config = PLEXOSConfig(model_name="Base", horizon_year=2024)
exporter = PLEXOSExporter(config, system)

result = exporter.export()
assert result.is_ok()
```

## Export with Custom Configuration

```python
from r2x_plexos.models.simulation_config import PLEXOSPerformance, PLEXOSSTSchedule

config = PLEXOSConfig(
    model_name="MyModel",
    horizon_year=2024,
    simulation_config={
        "performance": PLEXOSPerformance(
            name="Test_Perf",
            solver=4,
            mip_relative_gap=0.02,
        ),
        "st_schedule": PLEXOSSTSchedule(
            name="Test_ST",
            transmission_detail=1,
            heat_rate_detail=2,
        ),
        "diagnostic": None,  # Test that None values are skipped
    }
)

exporter = PLEXOSExporter(config, system)
result = exporter.export()
```

## Include Default Properties

```python
# By default, exporter excludes properties with default values
# Include all properties:
exporter = PLEXOSExporter(
    config,
    system,
    exclude_defaults=False
)
```

## Set Custom Scenario Name

```python
exporter = PLEXOSExporter(
    config,
    system,
    plexos_scenario="custom_scenario"
)
```


## Handle Export Errors

```python
result = exporter.export()

if result.is_err():
    print(f"Export failed: {result.error}")
    # Handle error
else:
    print("Export successful")
```

## Round-Trip Export

```python
from pathlib import Path
from r2x_core import DataStore, DataFile
from r2x_plexos import PLEXOSParser, PLEXOSExporter, PLEXOSConfig

# Parse
config = PLEXOSConfig(model_name="Base", horizon_year=2024)
store = DataStore(folder=Path("input"))
store.add_data_file(DataFile(name="xml", glob="*.xml"))

parser = PLEXOSParser(config, store)
system = parser.build_system()

# Export
export_config = PLEXOSConfig(model_name="Modified", horizon_year=2024)
exporter = PLEXOSExporter(export_config, system)
result = exporter.export()
```

## Specify Output Directory

```python
# Output location is derived from config
config = PLEXOSConfig(
    model_name="MyModel",
    horizon_year=2024,
    timeseries_dir=Path("output/timeseries")
)

exporter = PLEXOSExporter(config, system)
# XML will be in output/, CSV files in output/timeseries/
```

## Validate Export

```python
# Validation is built into export workflow
result = exporter.export()

# Or validate separately
validation = exporter.validate_export()
```

## Use Existing PlexosDB Object instead of creating a new one

This is for testing only

```python
from plexosdb import PlexosDB

# Create or load existing database
db = PlexosDB(xml_fname="template.xml")

# Use with exporter
exporter = PLEXOSExporter(config, system, db=db)
```
