# Data Model

## Component Types

R2X PLEXOS supports the following objects:

- **PLEXOSGenerator**: Power generation units
- **PLEXOSNode**: Electrical buses/nodes
- **PLEXOSLine**: Transmission lines
- **PLEXOSInterface**: Inter-regional interfaces
- **PLEXOSStorage**: Energy storage systems
- **PLEXOSBattery**: Battery storage units
- **PLEXOSRegion**: Geographical regions
- **PLEXOSZone**: Operating zones
- **PLEXOSReserve**: Reserve requirements
- **PLEXOSFuel**: Fuel types and pricing
- **PLEXOSTimeslice**: Time period definitions
- **PLEXOSVariable**: Dynamic variables
- **PLEXOSDatafile**: References to CSV data files
- **PLEXOSModel**: Simulation models
- **PLEXOSHorizon**: Time horizon definitions
- **PLEXOSMembership**: Component relationships

## Property Types

### Simple Properties
Direct attribute values on components:
```python
generator.max_capacity = 500.0
generator.min_stable_level = 100.0
```

### Property Values
Structured property objects that support multiple scenarios and actions:
```python
from r2x_plexos.models.property import PLEXOSPropertyValue

prop = PLEXOSPropertyValue(
    value=500.0,
    scenario="Base",
    action=1  # Add, Subtract, Multiply, etc.
)
```

### Collection Properties
Properties that relate one component to another:
```python
from r2x_plexos.models.collection_property import CollectionProperties

coll_prop = CollectionProperties(
    collection_name="Regions",
    properties={"load_risk": PLEXOSPropertyValue(value=6.0)}
)
```

### Time Series Properties
Properties with time-varying values:
```python
# Time series attached via R2X system
system.add_time_series(
    ts_data,
    component,
    variable_name="max_capacity"
)
```

## Memberships

Memberships define relationships between components:

- Parent object: The containing component
- Collection: The relationship type
- Child object: The contained component
- Membership ID: Unique identifier

Common membership patterns:
- Generator → Node: Physical connection
- Component → Region: Regional assignment
- Component → Category: Categorization
- Reserve → Generator: Reserve provision
