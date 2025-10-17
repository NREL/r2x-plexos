"""Registry for PLEXOS components and their corresponding enums."""

from typing import Any, ClassVar

from infrasys.component import Component
from plexosdb.enums import ClassEnum, CollectionEnum

from .generator import PLEXOSGenerator
from .node import PLEXOSNode


class PLEXOSComponentRegistry:
    """Registry for mapping components to their PLEXOS class enums."""

    # Class registry mapping component classes to ClassEnum values
    _class_registry: ClassVar[dict[type[Component], ClassEnum]] = {
        PLEXOSGenerator: ClassEnum.Generator,
        PLEXOSNode: ClassEnum.Node,
    }

    # Collection registry mapping (parent_class_enum, child_class_enum) to CollectionEnum
    _collection_registry: ClassVar[dict[tuple[ClassEnum, ClassEnum], CollectionEnum]] = {}

    @classmethod
    def register_component(cls, component_class: type[Component], class_enum: ClassEnum) -> None:
        """Register a component class with its corresponding ClassEnum."""
        cls._class_registry[component_class] = class_enum

    @classmethod
    def register_collection(
        cls,
        parent_enum: ClassEnum,
        child_enum: ClassEnum,
        collection_enum: CollectionEnum,
    ) -> None:
        """Register a collection relationship between parent and child class enums."""
        cls._collection_registry[(parent_enum, child_enum)] = collection_enum

    @classmethod
    def get_class_enum(cls, component: Any) -> ClassEnum | None:
        """Get the ClassEnum for a component or component class."""
        if isinstance(component, type):
            return cls._class_registry.get(component)
        return cls._class_registry.get(type(component))

    @classmethod
    def get_collection_enum(cls, parent_enum: ClassEnum, child_enum: ClassEnum) -> CollectionEnum | None:
        """Get the CollectionEnum for a parent-child enum pair."""
        # First check explicit registrations
        if (parent_enum, child_enum) in cls._collection_registry:
            return cls._collection_registry[(parent_enum, child_enum)]

        # Handle System as parent with the plural form pattern
        if parent_enum == ClassEnum.System:
            # Try to find a plural form of the child class name
            child_name = child_enum.value
            plural_name = f"{child_name}s"

            try:
                return CollectionEnum[plural_name]
            except (KeyError, ValueError):
                # Handle special cases where plural isn't just adding 's'
                special_plurals = {
                    "Storage": "Storages",
                    "Battery": "Batteries",
                    # Add other special cases here
                }
                if child_name in special_plurals:
                    try:
                        return CollectionEnum[special_plurals[child_name]]
                    except (KeyError, ValueError):
                        pass

        return None

    @classmethod
    def determine_collection(cls, parent: Component, child: Component) -> CollectionEnum | None:
        """Determine the appropriate CollectionEnum based on component instances."""
        parent_enum = cls.get_class_enum(type(parent))
        child_enum = cls.get_class_enum(type(child))

        if parent_enum and child_enum:
            return cls.get_collection_enum(parent_enum, child_enum)
        return None


for child_enum in [
    ClassEnum.Generator,
    ClassEnum.Fuel,
    ClassEnum.Battery,
    ClassEnum.Storage,
    ClassEnum.Emission,
    ClassEnum.Reserve,
    ClassEnum.Region,
    ClassEnum.Zone,
    ClassEnum.Node,
    ClassEnum.Line,
    ClassEnum.Transformer,
    ClassEnum.Interface,
    ClassEnum.DataFile,
    ClassEnum.Constraint,
]:
    collection_enum = PLEXOSComponentRegistry.get_collection_enum(ClassEnum.System, child_enum)
    if collection_enum:
        PLEXOSComponentRegistry.register_collection(
            ClassEnum.System,
            child_enum,
            collection_enum,
        )

# Register other common parent-child relationships
# For example:
# ComponentRegistry.register_collection(ClassEnum.Region, ClassEnum.Generator, CollectionEnum.Region)
PLEXOSComponentRegistry.register_collection(ClassEnum.Generator, ClassEnum.Node, CollectionEnum.Generators)
PLEXOSComponentRegistry.register_collection(ClassEnum.Battery, ClassEnum.Node, CollectionEnum.Batteries)
