import pytest

from r2x_core import System
from r2x_plexos.models.generator import PLEXOSGenerator
from r2x_plexos.models.property import PLEXOSPropertyValue

pytestmark = pytest.mark.serialize


def test_serialize_component():
    """Test basic component serialization."""
    component = PLEXOSGenerator.example()

    assert component.model_dump()


def test_serialize_component_complex():
    """Test component with multi-band property serialization."""
    heat_rate_with_bands = PLEXOSPropertyValue()
    heat_rate_with_bands.add_entry(value=10, band=1)
    heat_rate_with_bands.add_entry(value=12, band=2)
    heat_rate_with_bands.add_entry(value=14, band=3)

    component = PLEXOSGenerator(name="Test", heat_rate=heat_rate_with_bands)

    result = component.model_dump()
    assert result
    assert "heat_rate" in result


def test_serialize_round_trip():
    """Test round-trip serialization: serialize then deserialize."""
    # Create a component with property value with multiple bands
    original_prop = PLEXOSPropertyValue()
    original_prop.add_entry(value=10, band=1)
    original_prop.add_entry(value=12, band=2)
    original_prop.add_entry(value=14, band=3)

    component = PLEXOSGenerator(name="Test", heat_rate=original_prop)

    # Serialize
    serialized = component.model_dump(mode="json")

    # heat_rate should be a list of entries
    heat_rate_serialized = serialized["heat_rate"]
    assert isinstance(heat_rate_serialized, list)
    assert len(heat_rate_serialized) == 3

    # Check structure of entries
    values = {entry["band"]: entry["value"] for entry in heat_rate_serialized}
    assert values == {1: 10, 2: 12, 3: 14}

    # Check all expected fields are present in each entry
    for entry in heat_rate_serialized:
        assert "value" in entry
        assert "band" in entry
        assert "scenario_name" in entry
        assert "timeslice_name" in entry
        assert "date_from" in entry
        assert "date_to" in entry


def test_property_value_serialization_format():
    """Test that serialization format matches plexosdb_from_records structure."""
    prop = PLEXOSPropertyValue()
    prop.add_entry(
        value=100.0,
        scenario="Base",
        band=1,
        timeslice="Peak",
        date_from="2024-01-01",
        date_to="2024-12-31",
        units="MW",
        action="=",
    )

    component = PLEXOSGenerator(name="Test", max_capacity=prop)
    serialized = component.model_dump(mode="json")

    max_cap_serialized = serialized["max_capacity"]
    assert isinstance(max_cap_serialized, list)
    assert len(max_cap_serialized) == 1

    entry = max_cap_serialized[0]

    # Check all expected fields are present
    assert entry["value"] == 100.0
    assert entry["scenario_name"] == "Base"
    assert entry["band"] == 1
    assert entry["timeslice_name"] == "Peak"
    assert entry["date_from"] == "2024-01-01"
    assert entry["date_to"] == "2024-12-31"
    assert entry["units"] == "MW"
    assert entry["action"] == "="

    # Check optional fields exist (may be None)
    assert "datafile_name" in entry
    assert "datafile_id" in entry
    assert "column_name" in entry
    assert "variable_name" in entry
    assert "variable_id" in entry
    assert "text" in entry
    assert "text_class_name" in entry


def test_serialize_property_with_scenarios():
    """Test serialization of property with multiple scenarios."""
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=100, scenario="Base", band=1)
    prop.add_entry(value=120, scenario="High", band=1)
    prop.add_entry(value=80, scenario="Low", band=1)

    component = PLEXOSGenerator(name="Test", max_capacity=prop)
    serialized = component.model_dump(mode="json")

    max_cap_serialized = serialized["max_capacity"]
    assert len(max_cap_serialized) == 3

    scenarios = {entry["scenario_name"] for entry in max_cap_serialized}
    assert scenarios == {"Base", "High", "Low"}

    values = {entry["scenario_name"]: entry["value"] for entry in max_cap_serialized}
    assert values["Base"] == 100
    assert values["High"] == 120
    assert values["Low"] == 80


def test_serialize_property_with_datafile():
    """Test serialization preserves datafile references."""
    prop = PLEXOSPropertyValue()
    prop.add_entry(
        value=0,  # Datafile references often have placeholder value
        datafile_name="load_profile.csv",
        datafile_id=42,
        column_name="Load",
        band=1,
    )

    component = PLEXOSGenerator(name="Test", heat_rate=prop)
    serialized = component.model_dump(mode="json")

    heat_rate_serialized = serialized["heat_rate"]
    assert len(heat_rate_serialized) == 1
    entry = heat_rate_serialized[0]

    assert entry["datafile_name"] == "load_profile.csv"
    assert entry["datafile_id"] == 42
    assert entry["column_name"] == "Load"


def test_serialize_property_with_variable():
    """Test serialization preserves variable references."""
    prop = PLEXOSPropertyValue()
    prop.add_entry(
        value=0,  # Variable references often have placeholder value
        variable_name="MaxCapacity",
        variable_id=123,
        action="*",
        band=1,
    )

    component = PLEXOSGenerator(name="Test", heat_rate=prop)
    serialized = component.model_dump(mode="json")

    heat_rate_serialized = serialized["heat_rate"]
    assert len(heat_rate_serialized) == 1
    entry = heat_rate_serialized[0]

    assert entry["variable_name"] == "MaxCapacity"
    assert entry["variable_id"] == 123
    assert entry["action"] == "*"


def test_serialize_property_with_text():
    """Test serialization preserves text field."""
    prop = PLEXOSPropertyValue()
    prop.add_entry(
        value=0,
        text="/path/to/datafile.csv",
        text_class_name="Data File",
        band=1,
    )

    component = PLEXOSGenerator(name="Test", heat_rate=prop)
    serialized = component.model_dump(mode="json")

    heat_rate_serialized = serialized["heat_rate"]
    assert len(heat_rate_serialized) == 1
    entry = heat_rate_serialized[0]

    assert entry["text"] == "/path/to/datafile.csv"
    assert entry["text_class_name"] == "Data File"


def test_deserialize_component():
    """Test deserialization of component from JSON."""
    # Create and serialize a component
    original_prop = PLEXOSPropertyValue()
    original_prop.add_entry(value=10, band=1)
    original_prop.add_entry(value=12, band=2)
    original_prop.add_entry(value=14, band=3)

    original = PLEXOSGenerator(name="TestGen", heat_rate=original_prop)
    serialized = original.model_dump(mode="json")

    # Deserialize
    deserialized = PLEXOSGenerator(**serialized)

    # Verify basic properties
    assert deserialized.name == "TestGen"

    # Verify heat_rate was reconstructed correctly
    # Use get_property_value() to bypass __getattribute__ auto-resolution
    heat_rate = deserialized.get_property_value("heat_rate")
    assert isinstance(heat_rate, PLEXOSPropertyValue)
    assert len(heat_rate.entries) == 3

    # Verify values are correct
    assert heat_rate.get_value_for(band=1) == 10
    assert heat_rate.get_value_for(band=2) == 12
    assert heat_rate.get_value_for(band=3) == 14

    # Verify bands are correct
    assert set(heat_rate.get_bands()) == {1, 2, 3}


def test_deserialize_component_with_scenarios():
    """Test deserialization preserves scenarios."""
    # Create component with scenarios
    original_prop = PLEXOSPropertyValue()
    original_prop.add_entry(value=100, scenario="Base", band=1)
    original_prop.add_entry(value=120, scenario="High", band=1)
    original_prop.add_entry(value=80, scenario="Low", band=1)

    original = PLEXOSGenerator(name="TestGen", max_capacity=original_prop)
    serialized = original.model_dump(mode="json")

    # Deserialize
    deserialized = PLEXOSGenerator(**serialized)

    # Verify scenarios
    # Use get_property_value() to bypass __getattribute__ auto-resolution
    max_capacity = deserialized.get_property_value("max_capacity")
    assert isinstance(max_capacity, PLEXOSPropertyValue)
    scenarios = max_capacity.get_scenarios()
    assert set(scenarios) == {"Base", "High", "Low"}

    # Verify values
    assert max_capacity.get_value_for(scenario="Base") == 100
    assert max_capacity.get_value_for(scenario="High") == 120
    assert max_capacity.get_value_for(scenario="Low") == 80


def test_deserialize_component_with_metadata():
    """Test deserialization preserves all metadata fields."""
    # Create component with complex property
    original_prop = PLEXOSPropertyValue()
    original_prop.add_entry(
        value=100.0,
        scenario="Base",
        band=1,
        timeslice="Peak",
        date_from="2024-01-01",
        date_to="2024-12-31",
        datafile_name="test.csv",
        datafile_id=42,
        column_name="Value",
        variable_name="TestVar",
        variable_id=123,
        action="*",
        units="MW",
        text="/path/to/file.csv",
        text_class_name="Data File",
    )

    original = PLEXOSGenerator(name="TestGen", max_capacity=original_prop)
    serialized = original.model_dump(mode="json")

    # Deserialize
    deserialized = PLEXOSGenerator(**serialized)

    # Verify property was reconstructed
    # Use get_property_value() to bypass __getattribute__ auto-resolution
    prop = deserialized.get_property_value("max_capacity")
    assert isinstance(prop, PLEXOSPropertyValue)
    assert len(prop.entries) == 1

    # Get the entry
    entry = next(iter(prop.entries.values()))

    # Verify all metadata
    assert entry.value == 100.0
    assert entry.scenario_name == "Base"
    assert entry.band == 1
    assert entry.timeslice_name == "Peak"
    assert entry.date_from == "2024-01-01"
    assert entry.date_to == "2024-12-31"
    assert entry.datafile_name == "test.csv"
    assert entry.datafile_id == 42
    assert entry.column_name == "Value"
    assert entry.variable_name == "TestVar"
    assert entry.variable_id == 123
    assert entry.action == "*"
    assert entry.units == "MW"
    assert entry.text == "/path/to/file.csv"
    assert entry.text_class_name == "Data File"


def test_system_serialization(tmp_path):
    """Test serialization of components in infrasys System."""
    # Create a System
    system = System(auto_add_composed_components=True)

    # Create generators with complex properties
    gen1_prop = PLEXOSPropertyValue()
    gen1_prop.add_entry(value=100, band=1)
    gen1_prop.add_entry(value=120, band=2)
    gen1 = PLEXOSGenerator(name="Gen1", max_capacity=gen1_prop)

    gen2_prop = PLEXOSPropertyValue()
    gen2_prop.add_entry(value=50, scenario="Base", band=1)
    gen2_prop.add_entry(value=60, scenario="High", band=1)
    gen2 = PLEXOSGenerator(name="Gen2", max_capacity=gen2_prop)

    # Add to system
    system.add_component(gen1)
    system.add_component(gen2)

    # Serialize to JSON file
    json_file = tmp_path / "system.json"
    system.to_json(json_file)

    # Verify file was created
    assert json_file.exists()

    # Deserialize from JSON
    system2 = System.from_json(json_file, auto_add_composed_components=True)

    # Verify components were deserialized
    assert len(list(system2.get_components(PLEXOSGenerator))) == 2

    # Get components by name (label format is "ClassName.name")
    gen1_restored = system2.get_component_by_label("PLEXOSGenerator.Gen1")
    gen2_restored = system2.get_component_by_label("PLEXOSGenerator.Gen2")

    # Verify Gen1
    assert gen1_restored is not None
    # Use get_property_value() to bypass __getattribute__ auto-resolution
    max_cap1 = gen1_restored.get_property_value("max_capacity")
    assert isinstance(max_cap1, PLEXOSPropertyValue)
    assert len(max_cap1.entries) == 2
    assert max_cap1.get_value_for(band=1) == 100
    assert max_cap1.get_value_for(band=2) == 120

    # Verify Gen2
    assert gen2_restored is not None
    max_cap2 = gen2_restored.get_property_value("max_capacity")
    assert isinstance(max_cap2, PLEXOSPropertyValue)
    assert len(max_cap2.entries) == 2
    assert max_cap2.get_value_for(scenario="Base") == 50
    assert max_cap2.get_value_for(scenario="High") == 60


def test_system_serialization_complex_properties(tmp_path):
    """Test System serialization with properties containing all metadata."""
    system = System(auto_add_composed_components=True)

    # Create generator with complex property including all metadata
    prop = PLEXOSPropertyValue()
    prop.add_entry(
        value=100.0,
        scenario="Base",
        band=1,
        timeslice="Peak",
        units="MW",
        action="=",
    )
    prop.add_entry(
        value=80.0,
        scenario="Base",
        band=1,
        timeslice="OffPeak",
        units="MW",
        action="=",
    )

    gen = PLEXOSGenerator(name="ComplexGen", max_capacity=prop)
    system.add_component(gen)

    # Serialize and deserialize
    json_file = tmp_path / "system_complex.json"
    system.to_json(json_file)
    system2 = System.from_json(json_file, auto_add_composed_components=True)

    # Get restored component
    gen_restored = system2.get_component_by_label("PLEXOSGenerator.ComplexGen")

    # Verify property reconstruction
    assert gen_restored is not None
    # Use get_property_value() to bypass __getattribute__ auto-resolution
    max_cap = gen_restored.get_property_value("max_capacity")
    assert isinstance(max_cap, PLEXOSPropertyValue)
    assert len(max_cap.entries) == 2

    # Verify timeslices
    timeslices = max_cap.get_timeslices()
    assert set(timeslices) == {"Peak", "OffPeak"}

    # Verify scenarios
    scenarios = max_cap.get_scenarios()
    assert scenarios == ["Base"]

    # Verify all values are preserved (check the actual entries)
    values = [row.value for row in max_cap.entries.values()]
    assert set(values) == {100.0, 80.0}

    # Verify timeslice associations
    timeslice_values = {row.timeslice_name: row.value for row in max_cap.entries.values()}
    assert timeslice_values["Peak"] == 100.0
    assert timeslice_values["OffPeak"] == 80.0
