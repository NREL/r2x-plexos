# Reference

R2X PLEXOS provides parser and exporter plugins for PLEXOS power system models.

## Core Classes

### Parser & Exporter
- {py:class}`~r2x_plexos.PLEXOSParser` - Parse PLEXOS XML files
- {py:class}`~r2x_plexos.PLEXOSExporter` - Export to PLEXOS XML format
- {py:class}`~r2x_plexos.PLEXOSConfig` - Configuration model

### Component Models

#### Base Classes
- {py:class}`~r2x_plexos.models.PLEXOSObject` - Base class for all PLEXOS components
- {py:class}`~r2x_plexos.models.PLEXOSComponent` - Component with properties

#### Generation & Storage
- {py:class}`~r2x_plexos.models.PLEXOSGenerator` - Generator components
- {py:class}`~r2x_plexos.models.PLEXOSBattery` - Battery storage
- {py:class}`~r2x_plexos.models.PLEXOSStorage` - Storage components

#### Network Components
- {py:class}`~r2x_plexos.models.PLEXOSNode` - Network nodes
- {py:class}`~r2x_plexos.models.PLEXOSLine` - Transmission lines
- {py:class}`~r2x_plexos.models.PLEXOSInterface` - Network interfaces

#### Regions & Zones
- {py:class}`~r2x_plexos.models.PLEXOSRegion` - Regional components
- {py:class}`~r2x_plexos.models.PLEXOSZone` - Zone components
- {py:class}`~r2x_plexos.models.PLEXOSReserve` - Reserve requirements

#### Other Components
- {py:class}`~r2x_plexos.models.PLEXOSFuel` - Fuel types
- {py:class}`~r2x_plexos.models.PLEXOSVariable` - Variables and expressions
- {py:class}`~r2x_plexos.models.PLEXOSTimeslice` - Time slice definitions

### Data Models
- {py:class}`~r2x_plexos.models.PLEXOSPropertyValue` - Property values (scalar or time series)
- {py:class}`~r2x_plexos.models.PLEXOSMembership` - Component relationships
- {py:class}`~r2x_plexos.models.CollectionProperties` - Collection-level properties
- {py:class}`~r2x_plexos.models.PLEXOSDatafile` - Data file references

### Simulation Models
- {py:class}`~r2x_plexos.models.PLEXOSModel` - PLEXOS model configuration
- {py:class}`~r2x_plexos.models.PLEXOSHorizon` - Time horizon configuration

For detailed API documentation with examples and method signatures, see the [Complete API Documentation](./api.md).

## Documentation Coverage

```{eval-rst}
.. report:doc-coverage::
   :reportid: src
```

```{toctree}
:maxdepth: 1
:hidden:

api
```
