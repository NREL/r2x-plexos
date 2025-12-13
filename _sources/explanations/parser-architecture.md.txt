# Parser Architecture

## Overview

The PLEXOSParser converts PLEXOS XML files into R2X Core/Infrasys system. It uses the plexosdb library to read XML data and creates corresponding Infrasys-derived PLEXOSObject objects.

## Parsing Workflow

1. **Build Components**: Create R2X components from PLEXOS objects
2. **Build Time Series**: Attach time series data to components
3. **Post Process**: Add memberships and finalize relationships

## Component Creation

The parser reads PLEXOS objects and maps them to infrasys-inherited PLEXOSObject objects:

- Generator → PLEXOSGenerator
- Node → PLEXOSNode
- Region → PLEXOSRegion
- Line → PLEXOSLine
- And more...

## Property Handling

Properties are parsed from the PLEXOS database and attached to components:

- **Simple properties**: Direct attribute assignment
- **Collection properties**: Component-to-component properties
- **Time series properties**: References to CSV data files

## Membership Relationships

Memberships define relationships between components and are stored as supplemental attributes rather than components themselves. This allows flexible querying and export.

## Caching Strategy

The parser uses multiple caches for performance:

- Component cache: Avoid duplicate component creation
- Datafile cache: Reuse parsed CSV files
- Property cache: Store property values
- Collection property cache: Handle component relationships

## Time Series Resolution

Time series data is resolved from multiple sources:

1. **Direct datafiles**: CSV files referenced in properties
2. **Datafile components**: PLEXOS Datafile objects
3. **Variables**: PLEXOS Variable objects with profiles
4. **Nested variables**: Variables referencing other variables

## Horizon Management

The parser respects horizon definitions from the PLEXOS model, trimming time series data to match the specified simulation period.
