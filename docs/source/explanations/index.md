# Explanations

Understanding the architecture and design of r2x-plexos.

```{toctree}
:maxdepth: 1

parser-architecture
exporter-architecture
data-model
supplemental-attributes
```

## Overview

R2X PLEXOS is built on the R2X Core framework and provides bidirectional conversion between PLEXOS XML format and R2X systems. The package consists of two main components:

- **Parser**: Reads PLEXOS XML files and converts them to R2X system components
- **Exporter**: Converts R2X systems back to PLEXOS XML format

## Design Principles

1. **Preservation**: Round-trip conversion maintains data fidelity
2. **Flexibility**: Support for custom configurations and extensions
3. **Validation**: Built-in validation for data integrity
4. **Performance**: Efficient handling of large models and time series data
