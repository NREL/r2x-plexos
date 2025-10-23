# Working with Memberships

Memberships represent relationships between PLEXOS components. With an Infrasys system
with PLEXOS components you can query memberships as below

## Get All Memberships

```python
from r2x_plexos.models import PLEXOSMembership

memberships = list(system.get_supplemental_attributes(PLEXOSMembership))
print(f"Total memberships: {len(memberships)}")
```

## Access Membership Details

```python
for membership in memberships:
    print(f"ID: {membership.membership_id}")
    print(f"Parent: {membership.parent_object}")
    print(f"Collection: {membership.collection}")
    print(f"Child: {membership.child_object}")
```

## Filter Memberships by Parent

```python
# Get memberships for a specific component
generator = system.get_component(PLEXOSGenerator, "Gen1")

gen_memberships = [
    m for m in system.get_supplemental_attributes(PLEXOSMembership)
    if m.parent_object == generator.name
]
```

## Filter by Collection Type

```python
# Get all generator-node memberships
node_memberships = [
    m for m in system.get_supplemental_attributes(PLEXOSMembership)
    if m.collection == "Nodes"
]
```

## Create New Membership

```python
# Add membership as supplemental attribute
new_membership = PLEXOSMembership(
    membership_id=None,  # Will be assigned
    parent_object="Gen1",
    collection="Nodes",
    child_object="Node1"
)

system.add_supplemental_attributes(new_membership)
```
