# Exporter Architecture

## Overview

The PLEXOSExporter converts an Infrasys system back to PLEXOS XML format. It manages the creation of PLEXOS database objects, properties, memberships, and time series files.

## Export Workflow

1. **Setup Configuration**: Create models, horizons, and simulation config
2. **Prepare Export**: Add component objects to database
3. **Export Time Series**: Write time series to CSV files
4. **Postprocess Export**: Add properties, memberships, and generate XML
5. **Validate Export**: Optional validation step

## Database Management

The exporter leverages plexosdb to create and populate a PLEXOS database:

- Object creation: Add components as PLEXOS objects
- Property assignment: Set component properties and attributes
- Membership creation: Define component relationships
- XML generation: Export database to XML file

## XML Template Selection

The exporter loads a master PLEXOS XML database from the `plexosdb` package
(`plexosdb/config/`) to serve as the starting point for every export.  Four
templates are shipped, covering PLEXOS 9 through 12:

- `master_9.2R6_btu.xml` — PLEXOS 9.x (`"PLEXOS9.0"`)
- `master_10.0R2_btu.xml` — PLEXOS 10.x (`"PLEXOS10.0"`, default)
- `master_11.0R4_btu.xml` — PLEXOS 11.x (`"PLEXOS11.0"`)
- `master_12.0R3_btu.xml` — PLEXOS 12.x (`"PLEXOS12.0"`)

The active template is resolved by `PLEXOSExporter._resolve_template_path()`
which accepts a version key, a bare filename, or an absolute path via
`PLEXOSConfig.template`.

## Configuration Setup

For new databases, the exporter creates simulation configuration:

- Models: Simulation model objects
- Horizons: Time period definitions
- Memberships: Model-horizon relationships
- Simulation objects: Performance, Production, etc.

For existing databases (loaded from template), configuration is preserved.

## Time Series Export

Time series data is exported to CSV files:

- Grouped by component type and metadata
- References created in PLEXOS properties
- Datafile objects track CSV file locations

## Property Handling

Properties are added to the database:

- **Simple properties**: Direct property values
- **Collection properties**: Component-to-component relationships
- **Time series properties**: References to CSV files
- **Default exclusion**: Optional filtering of default values

## Membership Management

Memberships are stored as supplemental attributes in an Infrasys system and exported to the PLEXOS `t_membership` table, preserving all relationship data.

## Result Types

The exporter uses Result types for error handling:

- `Ok(None)`: Successful operation
- `Err(ExporterError)`: Operation failed with error details
