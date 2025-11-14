# PLEXOS Simulation Configuration Builder

A utility module for quickly creating PLEXOS simulation configurations (Models, Horizons, and their relationships) with minimal user input.

## Features

- **Template-based generation**: Create common patterns (monthly, weekly, quarterly) with a single line
- **Simple API**: One function call to build complete configurations
- **Reuses existing models**: Leverages `PLEXOSModel` and `PLEXOSHorizon` with Pydantic validation
- **Direct plexosdb integration**: Write configurations directly to PLEXOS XML/database
- **Flexible overrides**: Customize properties at model or horizon level
- **Fully custom configurations**: Define exact date ranges and properties when needed

## Quick Start

```python
from r2x_plexos.utils_simulation import build_plexos_simulation, ingest_simulation_to_plexosdb
from plexosdb import PlexosDB

# Build monthly models for 2012
config = {"year": 2012, "template": "monthly"}
result = build_plexos_simulation(config)

# Write to PLEXOS database
db = PlexosDB.from_xml("my_model.xml")
ingest_simulation_to_plexosdb(db, result)
db.to_xml("my_model_with_simulations.xml")
```

## Usage Examples

### 1. Simple Daily Simulation

Run simulation for full year with daily resolution:

```python
config = {"year": 2012, "resolution": "1D"}
result = build_plexos_simulation(config)
# Creates: 1 model, 1 horizon (366 days for leap year)
```

### 2. Monthly Models

Create 12 models, one per month:

```python
config = {"year": 2012, "template": "monthly"}
result = build_plexos_simulation(config)
# Creates: 12 models (Model_2012_M01 through Model_2012_M12)
#          12 horizons (Horizon_2012_M01 through Horizon_2012_M12)
```

### 3. Weekly Models

Create 52 models, one per week:

```python
config = {"year": 2012, "template": "weekly"}
result = build_plexos_simulation(config)
# Creates: 52 models, 52 horizons
```

### 4. Quarterly Models

Create 4 models, one per quarter:

```python
config = {"year": 2012, "template": "quarterly"}
result = build_plexos_simulation(config)
# Creates: 4 models (Model_2012_Q1 through Model_2012_Q4)
```

### 5. Template with Property Overrides

Customize model and horizon properties:

```python
config = {
    "year": 2012,
    "template": "monthly",
    "model_properties": {
        "category": "production_runs",
    },
    "horizon_properties": {
        "periods_per_day": 48,  # 30-minute intervals instead of hourly
    },
}
result = build_plexos_simulation(config)
```

### 6. Fully Custom Configuration

Define exact date ranges and properties:

```python
config = {
    "models": [
        {
            "name": "Summer_Peak_2012",
            "category": "seasonal",
            "horizon": {
                "name": "Summer_Horizon",
                "start": "2012-06-01",
                "end": "2012-08-31",
                "chrono_step_type": 2,  # Daily
                "periods_per_day": 24,
            },
        },
        {
            "name": "Winter_Base_2012",
            "category": "seasonal",
            "horizon": {
                "start": "2012-12-01",
                "end": "2012-12-31",
            },
        },
    ]
}
result = build_plexos_simulation(config)
```

## Configuration Schema

### Simple Configuration

```python
{
    "year": int,              # Required: Year to simulate
    "resolution": str,        # Optional: "1D" (daily) or "1H" (hourly), default "1D"
}
```

### Template Configuration

```python
{
    "year": int,                          # Required: Year to simulate
    "template": str,                      # Required: "monthly", "weekly", or "quarterly"
    "model_properties": dict,             # Optional: Override model properties
    "horizon_properties": dict,           # Optional: Override horizon properties
}
```

### Custom Configuration

```python
{
    "models": [
        {
            "name": str,                  # Required: Model name
            "category": str,              # Optional: Model category
            "horizon": {
                "name": str,              # Optional: Horizon name (auto-generated if not provided)
                "start": str,             # Required: ISO date string "YYYY-MM-DD"
                "end": str,               # Required: ISO date string "YYYY-MM-DD"
                "chrono_step_type": int,  # Optional: 2=daily, 1=hourly, default 2
                "chrono_step_count": int, # Optional: Auto-calculated from start/end
                "periods_per_day": int,   # Optional: Default 24
                "step_count": int,        # Optional: Default 1
            }
        },
        # ... more models
    ]
}
```

## API Reference

### `build_plexos_simulation(config, defaults=None)`

Build PLEXOS simulation configuration from user config.

**Parameters:**
- `config` (dict): Configuration dictionary (see schemas above)
- `defaults` (dict, optional): Default settings (loaded from `defaults.json` if None)

**Returns:**
- `SimulationBuildResult`: Contains lists of `PLEXOSModel` and `PLEXOSHorizon` objects, plus memberships

**Example:**
```python
result = build_plexos_simulation({"year": 2012, "template": "monthly"})
print(f"Created {len(result.models)} models")
print(f"Created {len(result.horizons)} horizons")
print(f"Model-Horizon pairs: {result.memberships}")
```

### `ingest_simulation_to_plexosdb(db, result)`

Write simulation configuration to PlexosDB.

**Parameters:**
- `db` (PlexosDB): PlexosDB instance to write to
- `result` (SimulationBuildResult): Output from `build_plexos_simulation`

**Returns:**
- `dict`: Dictionary with `"models"` and `"horizons"` keys mapping names to IDs

**Example:**
```python
db = PlexosDB.from_xml("my_model.xml")
ids = ingest_simulation_to_plexosdb(db, result)
print(f"Created model IDs: {ids['models']}")
print(f"Created horizon IDs: {ids['horizons']}")
db.to_xml("updated_model.xml")
```

### `datetime_to_ole_date(dt)`

Convert Python datetime to OLE Automation Date (used by PLEXOS).

**Parameters:**
- `dt` (datetime): Python datetime object

**Returns:**
- `float`: OLE Automation Date

**Example:**
```python
from datetime import datetime
ole_date = datetime_to_ole_date(datetime(2012, 1, 1))
# Returns: 40909.0
```

### `load_simulation_defaults()`

Load default simulation templates and settings from `defaults.json`.

**Returns:**
- `dict`: Defaults dictionary (empty dict if file not found)

## Integration with r2x_core Plugin System

This utility integrates seamlessly with the r2x_core plugin system:

```python
from r2x_core import PluginModel

class MyPlugin(PluginModel):
    def get_config(self):
        # Configuration can come from plugin
        return {
            "year": 2012,
            "template": "monthly",
            "model_properties": self.get_model_defaults(),
        }

    def process(self):
        config = self.get_config()
        result = build_plexos_simulation(config)
        # ... use result
```

## Testing

Run the test suite:

```bash
pytest tests/test_utils_simulation.py -v
```

Run the example:

```bash
python examples/simulation_builder_example.py
```

## Implementation Details

- **Model-Horizon Membership**: All models are automatically linked to their horizons via the `Horizons` collection
- **OLE Date Conversion**: PLEXOS uses OLE Automation Dates (days since Dec 30, 1899) for date storage
- **Leap Year Handling**: Automatically accounts for leap years when calculating day counts
- **Validation**: All objects use Pydantic models (`PLEXOSModel`, `PLEXOSHorizon`) for validation

## Extending

To add new templates, modify the `_build_from_template` function in `utils_simulation.py`:

```python
def _build_from_template(config: dict, defaults: dict) -> SimulationBuildResult:
    template_name = config["template"]
    year = config.get("year")

    if template_name == "my_custom_template":
        return _build_my_custom_template(year, config, defaults)
    # ... existing templates
```

Then implement your builder function following the pattern of `_build_monthly_models`.
