# Supplemental Attributes in R2X PLEXOS

## Overview

Supplemental attributes in [`infrasys`](https://nrel.github.io/infrasys/index.html) are classes that inherit from `SupplementalAttribute` and store information about a system that doesn't fit the traditional component model. Unlike components:

- **Many-to-many relationships**: A supplemental attribute can be associated with multiple components, and a component can have multiple supplemental attributes
- **Not standalone**: They must be attached to at least one component
- **Have UUIDs**: Each supplemental attribute instance has a unique identifier
- **Can have time series**: Like components, supplemental attributes can have time series data attached

## Why R2X PLEXOS Uses Supplemental Attributes

In PLEXOS, relationships between objects (memberships) are modeled as supplemental attributes because:

1. **Bidirectional associations**: A membership connects two objects (parent and child), and both objects need access to the relationship
2. **Not independent entities**: Memberships only exist in the context of the objects they connect
3. **Excluded from component operations**: Memberships are explicitly filtered out when iterating over components for export

## Supplemental Attributes in R2X PLEXOS

R2X PLEXOS defines two supplemental attribute classes:

### 1. PLEXOSMembership

Defines functional and logical relationships between PLEXOS objects.

**Attributes:**
- `parent_object`: The parent component in the relationship
- `child_object`: The child component in the relationship
- `collection`: The PLEXOS collection name defining the relationship type
- `membership_id`: Unique identifier for the membership

**Example relationships:**
- Generators belonging to regions
- Buses belonging to nodes
- Fuels used by generators

**How it's stored:**
```python
# A membership is associated with BOTH parent and child objects
system.add_supplemental_attribute(parent_object, membership)
system.add_supplemental_attribute(child_object, membership)
```

### 2. CollectionProperties

Stores properties associated with a specific membership-collection pair.

**Attributes:**
- `membership`: Reference to the associated `PLEXOSMembership`
- `collection_name`: Name of the collection (e.g., "Generators", "Nodes")
- `properties`: Dictionary mapping property names to lists of `PropertyValue` objects

**How it's stored:**
```python
# Collection properties are associated with the child object
system.add_supplemental_attribute(child_object, collection_properties)
```

## Working with Supplemental Attributes

### Querying Supplemental Attributes

```python
# Get all memberships in the system
memberships = system.get_supplemental_attributes(PLEXOSMembership)

# Get all supplemental attributes of a specific type attached to a component
memberships = system.get_supplemental_attributes_with_component(
    component,
    supplemental_attribute_type=PLEXOSMembership
)

# Get all components associated with a supplemental attribute
components = system.get_components_with_supplemental_attribute(membership)

# Check if a component has supplemental attributes of a specific type
has_attrs = system.has_supplemental_attribute(
    component,
    supplemental_attribute_type=PLEXOSMembership
)

# Get counts of supplemental attributes by type
counts = system.get_supplemental_attribute_counts_by_type()

# Get total number of supplemental attributes
total = system.get_num_supplemental_attributes()
```

### Retrieving by UUID

```python
# Get a supplemental attribute by its UUID
attr = system.get_supplemental_attribute_by_uuid(uuid)
```

## Key Differences from Components

| Aspect | Component | Supplemental Attribute |
|--------|-----------|----------------------|
| **Storage** | Component manager | Supplemental attribute manager |
| **Relationships** | One-to-many (composed in other components) | Many-to-many (associated with multiple components) |
| **Iteration** | `system.get_components()` | `system.get_supplemental_attributes()` |
| **Export behavior** | Included in component exports | Excluded from component iteration (see `exporter.py`) |
| **Attachment** | Exists independently in system | Must be attached to at least one component |

## Implementation Notes

- Supplemental attributes are stored separately from components in the system
- When exporting, `PLEXOSMembership` is explicitly excluded from component type iteration
- Both `PLEXOSMembership` and `CollectionProperties` can be queried through the standard `infrasys` supplemental attribute API
- The system tracks bidirectional associations between components and supplemental attributes
