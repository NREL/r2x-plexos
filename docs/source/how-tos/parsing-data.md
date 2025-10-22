# Parsing PLEXOS Data

## Parse a PLEXOS XML File

```python
from pathlib import Path
from r2x_core import DataFile, DataStore
from r2x_plexos import PLEXOSParser, PLEXOSConfig

config = PLEXOSConfig(model_name="Base", reference_year=2024)
store = DataStore(folder=Path("data"))
store.add_data_file(DataFile(name="xml_file", glob="*.xml"))

parser = PLEXOSParser(config, store)
system = parser.build_system()
```

## Access Specific Components

R2X PLEXOS is built upon Infrasys, and therefore, the way one interacts with
the system is the same.

```python
from r2x_plexos.models import PLEXOSGenerator

# Get all generators
generators = list(system.get_components(PLEXOSGenerator))

# Get specific generator
gen = system.get_component(PLEXOSGenerator, "Gen1")
print(f"Max capacity: {gen.max_capacity}")
```

## Filter Components

```python
# Get all generators above 500 MW
large_gens = list(
    system.get_components(
        PLEXOSGenerator,
        filter_func=lambda g: g.max_capacity > 500
    )
)
```

## Parse with Custom XML Path

```python
from pathlib import Path

config = PLEXOSConfig(model_name="Base", reference_year=2024)
data_file = DataFile(name="xml_file", fpath=Path("model.xml"))
store = DataStore(folder=Path("data"))
store.add_data_file(data_file)

parser = PLEXOSParser(config, store)
system = parser.build_system()
```


## Work with Collection Properties

```python
from r2x_plexos.models.collection_property import CollectionProperties
from r2x_plexos.models import PLEXOSRegion

region = system.get_component(PLEXOSRegion, "region-01")

# Get collection properties
coll_props = system.get_supplemental_attributes_with_component(
    region, CollectionProperties
)

for cp in coll_props:
    print(f"Collection: {cp.collection_name}")
    for name, prop in cp.properties.items():
        print(f"  {name}: {prop.get_value()}")
```

## Handle Time Series During Parsing

```python
# Parser automatically loads time series if referenced
config = PLEXOSConfig(
    model_name="Base",
    reference_year=2024,
    timeseries_dir=Path("timeseries")  # Custom TS location
)

parser = PLEXOSParser(config, store)
system = parser.build_system()

# Check for time series
gen = system.get_component(PLEXOSGenerator, "Gen1")
if system.has_time_series(gen):
    ts = system.get_time_series(gen, "max_capacity")
    print(f"Time series points: {len(ts.data)}")
```

## Skip Validation

Skip Pydantic validation when creating component instances. Useful for performance 
optimization or when handling incomplete/legacy data. Use with caution as it bypasses
type and constraint checking.

```python
parser = PLEXOSParser(config, store, skip_validation=True)
system = parser.build_system()
```
