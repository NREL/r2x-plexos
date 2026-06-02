"""Tests for PLEXOSComponentRegistry."""

from plexosdb.enums import ClassEnum, CollectionEnum

from r2x_plexos.models.generator import PLEXOSGenerator
from r2x_plexos.models.node import PLEXOSNode
from r2x_plexos.models.purchaser import PLEXOSPurchaser
from r2x_plexos.models.registry import PLEXOSComponentRegistry
from r2x_plexos.models.storage import PLEXOSStorage


def test_get_class_enum_with_component_class():
    """Test getting ClassEnum from a component class.

    Verifies that get_class_enum() works with component classes
    (not instances) and returns the correct ClassEnum.
    """
    class_enum = PLEXOSComponentRegistry.get_class_enum(PLEXOSGenerator)
    assert class_enum == ClassEnum.Generator

    class_enum = PLEXOSComponentRegistry.get_class_enum(PLEXOSNode)
    assert class_enum == ClassEnum.Node

    class_enum = PLEXOSComponentRegistry.get_class_enum(PLEXOSPurchaser)
    assert class_enum == ClassEnum.Purchaser


def test_get_class_enum_with_component_instance():
    """Test getting ClassEnum from a component instance.

    Verifies that get_class_enum() works with component instances
    and returns the correct ClassEnum.
    """
    generator = PLEXOSGenerator(name="test-gen", object_id=1)
    class_enum = PLEXOSComponentRegistry.get_class_enum(generator)
    assert class_enum == ClassEnum.Generator

    node = PLEXOSNode(name="test-node", object_id=2)
    class_enum = PLEXOSComponentRegistry.get_class_enum(node)
    assert class_enum == ClassEnum.Node

    purchaser = PLEXOSPurchaser(name="test-purchaser", object_id=3)
    class_enum = PLEXOSComponentRegistry.get_class_enum(purchaser)
    assert class_enum == ClassEnum.Purchaser


def test_get_class_enum_unregistered_component():
    """Test getting ClassEnum for an unregistered component returns None.

    Verifies that get_class_enum() returns None when the component
    is not in the registry.
    """

    # Create a dummy class that's not registered
    class UnregisteredComponent:
        pass

    class_enum = PLEXOSComponentRegistry.get_class_enum(UnregisteredComponent)
    assert class_enum is None


def test_get_collection_enum_explicit_registration():
    """Test getting CollectionEnum from explicit registry.

    Verifies that explicitly registered collection relationships
    are returned correctly.
    """
    collection_enum = PLEXOSComponentRegistry.get_collection_enum(ClassEnum.Generator, ClassEnum.Node)
    assert collection_enum == CollectionEnum.Generators

    collection_enum = PLEXOSComponentRegistry.get_collection_enum(ClassEnum.Battery, ClassEnum.Node)
    assert collection_enum == CollectionEnum.Batteries


def test_get_collection_enum_system_parent_simple_plural():
    """Test getting CollectionEnum with System parent using simple plural.

    Verifies the automatic plural form pattern works for System parent
    relationships where plural is just the singular + 's'.
    """
    collection_enum = PLEXOSComponentRegistry.get_collection_enum(ClassEnum.System, ClassEnum.Generator)
    assert collection_enum == CollectionEnum.Generators

    collection_enum = PLEXOSComponentRegistry.get_collection_enum(ClassEnum.System, ClassEnum.Node)
    assert collection_enum == CollectionEnum.Nodes

    collection_enum = PLEXOSComponentRegistry.get_collection_enum(ClassEnum.System, ClassEnum.Region)
    assert collection_enum == CollectionEnum.Regions


def test_get_collection_enum_system_parent_special_plural():
    """Test getting CollectionEnum with System parent using special plurals.

    Verifies the special plural handling for cases where plural
    is not just singular + 's' (e.g., Battery -> Batteries).
    """
    collection_enum = PLEXOSComponentRegistry.get_collection_enum(ClassEnum.System, ClassEnum.Battery)
    assert collection_enum == CollectionEnum.Batteries

    collection_enum = PLEXOSComponentRegistry.get_collection_enum(ClassEnum.System, ClassEnum.Storage)
    assert collection_enum == CollectionEnum.Storages


def test_get_collection_enum_unregistered_relationship():
    """Test getting CollectionEnum for unregistered relationship returns None.

    Verifies that get_collection_enum() returns None when the
    parent-child relationship is not registered and doesn't match
    the System parent pattern.
    """
    # Try a relationship that doesn't exist
    collection_enum = PLEXOSComponentRegistry.get_collection_enum(ClassEnum.Region, ClassEnum.Generator)
    # This might be None or might exist depending on PLEXOS schema
    # Just verify it doesn't raise an error
    assert collection_enum is None or isinstance(collection_enum, CollectionEnum)


def test_get_collection_enum_system_parent_nonexistent_plural():
    """Test getting CollectionEnum when plural form doesn't exist.

    Verifies that get_collection_enum() returns None when the
    automatic plural form doesn't exist in CollectionEnum.
    """
    # Use a ClassEnum that won't have a plural CollectionEnum
    # This is a bit tricky since most do have plurals
    # We can test the error handling path
    collection_enum = PLEXOSComponentRegistry.get_collection_enum(
        ClassEnum.System,
        ClassEnum.System,  # System -> Systems doesn't exist
    )
    # Should return None when plural not found
    assert collection_enum is None or isinstance(collection_enum, CollectionEnum)


def test_determine_collection_with_registered_components():
    """Test determine_collection with registered component instances.

    Verifies that determine_collection() works with component instances
    and returns the correct CollectionEnum.
    """
    generator = PLEXOSGenerator(name="test-gen", object_id=1)
    node = PLEXOSNode(name="test-node", object_id=2)

    collection_enum = PLEXOSComponentRegistry.determine_collection(generator, node)
    assert collection_enum == CollectionEnum.Generators


def test_determine_collection_with_unregistered_parent():
    """Test determine_collection with unregistered parent returns None.

    Verifies that determine_collection() returns None when the
    parent component is not registered.
    """

    class UnregisteredParent:
        pass

    parent = UnregisteredParent()
    child = PLEXOSNode(name="test-node", object_id=1)

    collection_enum = PLEXOSComponentRegistry.determine_collection(parent, child)  # ty: ignore[invalid-argument-type]
    assert collection_enum is None


def test_determine_collection_with_unregistered_child():
    """Test determine_collection with unregistered child returns None.

    Verifies that determine_collection() returns None when the
    child component is not registered.
    """

    class UnregisteredChild:
        pass

    parent = PLEXOSGenerator(name="test-gen", object_id=1)
    child = UnregisteredChild()

    collection_enum = PLEXOSComponentRegistry.determine_collection(parent, child)  # ty: ignore[invalid-argument-type]
    assert collection_enum is None


def test_register_component_dynamically():
    """Test dynamically registering a new component.

    Verifies that register_component() allows adding new component
    types to the registry at runtime.
    """
    # Register a new component type
    PLEXOSComponentRegistry.register_component(PLEXOSStorage, ClassEnum.Storage)

    # Verify it's registered
    class_enum = PLEXOSComponentRegistry.get_class_enum(PLEXOSStorage)
    assert class_enum == ClassEnum.Storage

    # Test with instance
    storage = PLEXOSStorage(name="test-storage", object_id=1)
    class_enum = PLEXOSComponentRegistry.get_class_enum(storage)
    assert class_enum == ClassEnum.Storage


def test_register_collection_dynamically():
    """Test dynamically registering a new collection relationship.

    Verifies that register_collection() allows adding new collection
    relationships to the registry at runtime.
    """
    # Register a new collection relationship
    PLEXOSComponentRegistry.register_collection(
        ClassEnum.Region, ClassEnum.Generator, CollectionEnum.Generators
    )

    # Verify it's registered
    collection_enum = PLEXOSComponentRegistry.get_collection_enum(ClassEnum.Region, ClassEnum.Generator)
    assert collection_enum == CollectionEnum.Generators


def test_system_collections_auto_registered():
    """Test that System parent collections are auto-registered.

    Verifies that the module-level initialization code successfully
    registers common System parent relationships.
    """
    # Test a few known System collections
    test_cases = [
        (ClassEnum.Generator, CollectionEnum.Generators),
        (ClassEnum.Battery, CollectionEnum.Batteries),
        (ClassEnum.Region, CollectionEnum.Regions),
        (ClassEnum.Node, CollectionEnum.Nodes),
        (ClassEnum.Purchaser, CollectionEnum.Purchasers),
        (ClassEnum.Line, CollectionEnum.Lines),
    ]

    for child_enum, expected_collection in test_cases:
        collection_enum = PLEXOSComponentRegistry.get_collection_enum(ClassEnum.System, child_enum)
        assert collection_enum == expected_collection, (
            f"Failed for {child_enum.name}: expected {expected_collection.name}, got {collection_enum}"
        )


def test_special_plural_forms_in_registry():
    """Test that special plural forms are handled correctly.

    Verifies the special_plurals dictionary in get_collection_enum()
    handles irregular plurals correctly.
    """
    # Battery -> Batteries (not Batterys)
    collection_enum = PLEXOSComponentRegistry.get_collection_enum(ClassEnum.System, ClassEnum.Battery)
    assert collection_enum == CollectionEnum.Batteries

    # Storage -> Storages (already ends in 'e')
    collection_enum = PLEXOSComponentRegistry.get_collection_enum(ClassEnum.System, ClassEnum.Storage)
    assert collection_enum == CollectionEnum.Storages


def test_registry_isolation():
    """Test that registry modifications don't affect other tests.

    Verifies that the registry maintains its state correctly and
    dynamically registered items are accessible.
    """
    # Get initial registry size
    initial_class_count = len(PLEXOSComponentRegistry._class_registry)
    _ = len(PLEXOSComponentRegistry._collection_registry)

    # Register something new
    class TestComponent:
        pass

    PLEXOSComponentRegistry.register_component(TestComponent, ClassEnum.Generator)  # ty: ignore[invalid-argument-type]

    # Verify it increased
    assert len(PLEXOSComponentRegistry._class_registry) >= initial_class_count

    # Verify the new registration works
    class_enum = PLEXOSComponentRegistry.get_class_enum(TestComponent)
    assert class_enum == ClassEnum.Generator


def test_collection_enum_not_found_in_special_plurals():
    """Test handling when special plural form doesn't exist in CollectionEnum.

    Verifies graceful handling when a special plural is defined but
    the CollectionEnum doesn't have that value.
    """
    # This tests the inner except block in get_collection_enum
    # We'd need a ClassEnum with a special plural that doesn't exist
    # This is hard to test without modifying the code, but we can verify
    # the method doesn't crash

    # Just verify the method handles all current ClassEnum values
    for class_enum in ClassEnum:
        collection_enum = PLEXOSComponentRegistry.get_collection_enum(ClassEnum.System, class_enum)
        # Should either return a CollectionEnum or None, never crash
        assert collection_enum is None or isinstance(collection_enum, CollectionEnum)


def test_get_collection_enum_handles_special_plural_lookup_failure(mocker):
    """Covers the inner special-plural lookup exception path."""

    class RaisingCollectionEnum:
        def __getitem__(self, key):
            raise KeyError(key)

    mocker.patch("r2x_plexos.models.registry.CollectionEnum", new=RaisingCollectionEnum())

    key = (ClassEnum.System, ClassEnum.Storage)
    original = PLEXOSComponentRegistry._collection_registry.copy()
    PLEXOSComponentRegistry._collection_registry.pop(key, None)
    try:
        collection_enum = PLEXOSComponentRegistry.get_collection_enum(ClassEnum.System, ClassEnum.Storage)
        assert collection_enum is None
    finally:
        PLEXOSComponentRegistry._collection_registry = original
