# Parsing PLEXOS Data

This tutorial shows how to parse PLEXOS XML files into an R2X system.

## Basic Setup

```python
from pathlib import Path
from r2x_core import DataFile, DataStore
from r2x_plexos import PLEXOSParser, PLEXOSConfig

# Configure the parser
config = PLEXOSConfig(
    model_name="Base",
    reference_year=2024,
    timeseries_dir=None  # Optional: specify custom directory
)

# Set up data store
data_folder = Path("path/to/plexos/files")
store = DataStore(folder=data_folder)

# Add PLEXOS XML file
data_file = DataFile(name="xml_file", glob="*.xml")
store.add_data_file(data_file)

# Create parser and build system
parser = PLEXOSParser(config, store)
system = parser.build_system()
```

## Working with Components

Once parsed, access components from the system:

```python
from r2x_plexos.models import PLEXOSGenerator, PLEXOSNode

# Get all generators
generators = list(system.get_components(PLEXOSGenerator))
print(f"Found {len(generators)} generators")

# Get specific component by name
gen = system.get_component(PLEXOSGenerator, "Generator1")
print(f"Capacity: {gen.max_capacity}")

# Get all nodes
nodes = list(system.get_components(PLEXOSNode))
```

## Accessing Memberships

PLEXOS memberships define relationships between different PLEXOS objects:

```python
from r2x_plexos.models import PLEXOSMembership

# Get all memberships
memberships = list(system.get_supplemental_attributes(PLEXOSMembership))

for membership in memberships:
    print(f"Parent: {membership.parent_object}")
    print(f"Collection: {membership.collection}")
    print(f"Child: {membership.child_object}")
```

## Working with Time Series

Access time series data attached to component properties:

```python
# Check if component has time series
if system.has_time_series(gen):
    # Get time series for a specific property
    ts = system.get_time_series(gen, "max_capacity")
    print(f"Time series length: {len(ts.data)}")
    print(f"First value: {ts.data[0]}")
```

## Collection Properties

These are properties defined on a collection membership:

```python
from r2x_plexos.models.collection_property import CollectionProperties
from r2x_plexos.models import PLEXOSRegion, PLEXOSReserve

# Get region component
region = system.get_component(PLEXOSRegion, "region-01")

# Get collection properties for this component
coll_props_list = system.get_supplemental_attributes_with_component(
    region, CollectionProperties
)

for coll_props in coll_props_list:
    print(f"Collection: {coll_props.collection_name}")
    for prop_name, prop_value in coll_props.properties.items():
        print(f"  {prop_name}: {prop_value.get_value()}")
```

## Configuration Options

Key configuration parameters:

- `model_name`: Name of the PLEXOS model to parse
- `reference_year`: Base year for time series data
- `horizon_year`: Simulation horizon year (alternative to reference_year)
- `timeseries_dir`: Custom directory for time series files
- `resolution`: Time resolution (e.g., "1D", "1H")

## Next Steps

- Learn about [exporting PLEXOS data](exporting-plexos-data.md)
- See [API reference](../references/api.md) for complete details
