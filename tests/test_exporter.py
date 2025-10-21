from typing import TYPE_CHECKING

import pytest
from plexosdb import ClassEnum

from r2x_core import PluginConfig
from r2x_plexos.exporter import PLEXOSExporter
from r2x_plexos.utils_mappings import PLEXOS_TYPE_MAP_INVERTED

if TYPE_CHECKING:
    from r2x_core import System
pytestmark = pytest.mark.export


@pytest.fixture
def plexos_config():
    from r2x_plexos import PLEXOSConfig

    return PLEXOSConfig(model_name="Base", horizon_year=2024)


@pytest.fixture
def serialized_plexos_system(tmp_path, db_all_gen_types, plexos_config) -> "System":
    from r2x_core import DataStore
    from r2x_plexos import PLEXOSParser

    store = DataStore(folder=tmp_path)

    parser = PLEXOSParser(plexos_config, store, db=db_all_gen_types)
    sys = parser.build_system()
    serialized_sys_fpath = tmp_path / "test_plexos_system.json"
    sys.to_json(serialized_sys_fpath)
    return sys


def test_setup_configuration_creates_simulation(plexos_config, serialized_plexos_system, caplog):
    """Test that setup_configuration creates models, horizons, and memberships."""
    sys = serialized_plexos_system

    exporter = PLEXOSExporter(plexos_config, sys)

    # Clear any existing models/horizons from the template
    for model_name in exporter.db.list_objects_by_class(ClassEnum.Model):
        exporter.db.delete_object(ClassEnum.Model, name=model_name)
    for horizon_name in exporter.db.list_objects_by_class(ClassEnum.Horizon):
        exporter.db.delete_object(ClassEnum.Horizon, name=horizon_name)

    # Verify database is now empty
    models_before = exporter.db.list_objects_by_class(ClassEnum.Model)
    horizons_before = exporter.db.list_objects_by_class(ClassEnum.Horizon)
    assert len(models_before) == 0
    assert len(horizons_before) == 0

    # Run setup_configuration
    result = exporter.setup_configuration()
    if result.is_err():
        print(f"\nError: {result.error}")
    assert result.is_ok(), f"setup_configuration failed: {result.error if result.is_err() else result}"

    # Verify models were created
    models_after = exporter.db.list_objects_by_class(ClassEnum.Model)
    assert len(models_after) > 0, "No models were created"
    print(f"\n✓ Created {len(models_after)} model(s): {models_after}")

    # Verify horizons were created
    horizons_after = exporter.db.list_objects_by_class(ClassEnum.Horizon)
    assert len(horizons_after) > 0, "No horizons were created"
    print(f"✓ Created {len(horizons_after)} horizon(s): {horizons_after}")

    # Verify model-horizon memberships exist
    # Get object IDs for the first model and horizon
    model_name = models_after[0]
    horizon_name = horizons_after[0]

    model_id = exporter.db.get_object_id(ClassEnum.Model, model_name)
    horizon_id = exporter.db.get_object_id(ClassEnum.Horizon, horizon_name)

    # Check memberships - models should be connected to horizons
    query = """
    SELECT COUNT(*)
    FROM t_membership
    WHERE parent_object_id = ? AND child_object_id = ?
    """
    result = exporter.db.query(query, (model_id, horizon_id))
    membership_count = result[0][0] if result else 0
    assert membership_count > 0, "No model-horizon memberships were created"
    print("✓ Created model-horizon memberships")

    # Verify horizon attributes were set (not properties - horizons use attributes!)
    # Check for at least one of the common horizon attributes
    try:
        chrono_date_from = exporter.db.get_attribute(
            ClassEnum.Horizon, object_name=horizon_name, attribute_name="Chrono Date From"
        )
        assert chrono_date_from is not None, "Horizon attributes were not set"
        print(f"✓ Set horizon attributes (Chrono Date From: {chrono_date_from})")
    except AssertionError as e:
        # If get_attribute fails, it means no attributes were set
        raise AssertionError("No horizon attributes were set") from e


def test_setup_configuration_skips_existing(plexos_config, serialized_plexos_system, caplog):
    """Test that setup_configuration skips if models/horizons already exist."""
    sys = serialized_plexos_system
    exporter = PLEXOSExporter(plexos_config, sys)

    # Clear any existing models/horizons from the template
    for model_name in exporter.db.list_objects_by_class(ClassEnum.Model):
        exporter.db.delete_object(ClassEnum.Model, name=model_name)
    for horizon_name in exporter.db.list_objects_by_class(ClassEnum.Horizon):
        exporter.db.delete_object(ClassEnum.Horizon, name=horizon_name)

    # First call - should create simulation
    result1 = exporter.setup_configuration()
    assert result1.is_ok()

    models_count = len(exporter.db.list_objects_by_class(ClassEnum.Model))
    horizons_count = len(exporter.db.list_objects_by_class(ClassEnum.Horizon))

    # Second call - should skip
    result2 = exporter.setup_configuration()
    assert result2.is_ok()

    # Verify counts didn't change
    models_count2 = len(exporter.db.list_objects_by_class(ClassEnum.Model))
    horizons_count2 = len(exporter.db.list_objects_by_class(ClassEnum.Horizon))

    assert models_count == models_count2, "Models were created on second call"
    assert horizons_count == horizons_count2, "Horizons were created on second call"
    assert "using existing database configuration" in caplog.text.lower()
    print("\n✓ Correctly skipped duplicate simulation setup")


def test_setup_configuration_missing_reference_year():
    """Test that missing horizon_year returns error."""

    from r2x_core import System
    from r2x_plexos import PLEXOSConfig

    # Create config without horizon_year
    config = PLEXOSConfig(model_name="Base")
    # Verify horizon_year is None (it's optional with default=None)

    # Create a minimal system (no need for full fixtures)
    sys = System(name="test_system")
    exporter = PLEXOSExporter(config, sys)

    # Clear any existing models/horizons from the template
    for model_name in exporter.db.list_objects_by_class(ClassEnum.Model):
        exporter.db.delete_object(ClassEnum.Model, name=model_name)
    for horizon_name in exporter.db.list_objects_by_class(ClassEnum.Horizon):
        exporter.db.delete_object(ClassEnum.Horizon, name=horizon_name)

    result = exporter.setup_configuration()
    assert result.is_err(), "Should fail without horizon_year"
    assert "horizon_year" in str(result.error).lower()
    print(f"\n✓ Correctly rejected missing horizon_year: {result.error}")


def test_exporter(plexos_config, serialized_plexos_system, caplog):
    sys = serialized_plexos_system

    exporter = PLEXOSExporter(plexos_config, sys)

    exporter.export()
    assert 0


def test_exporter_with_wrong_config(mocker, caplog):
    class InvalidConfig(PluginConfig):
        name: str

    bad_config = InvalidConfig(name="Test")
    mock_system = mocker.Mock()
    mocker.patch("r2x_core.System", return_value=mock_system)
    with pytest.raises(TypeError):
        PLEXOSExporter(config=bad_config, system=mock_system)


def test_memberships_are_supplemental_attributes_not_components(serialized_plexos_system):
    """Test that memberships are stored as supplemental attributes, not components."""
    from r2x_plexos.models import PLEXOSMembership

    sys = serialized_plexos_system

    # Verify memberships ARE stored as supplemental attributes
    supp_memberships = list(sys.get_supplemental_attributes(PLEXOSMembership))
    assert len(supp_memberships) > 0, "Memberships should be stored as supplemental attributes"

    print(f"\n✓ Correctly stored {len(supp_memberships)} memberships as supplemental attributes")

    # Verify membership structure
    for membership in supp_memberships[:3]:  # Check first 3
        assert isinstance(membership, PLEXOSMembership)
        assert membership.membership_id is not None
        assert membership.parent_object is not None
        assert membership.collection is not None
        print(
            f"✓ Membership {membership.membership_id}: {membership.parent_object.name} -> collection={membership.collection}"
        )


def test_memberships_exported_correctly(plexos_config, serialized_plexos_system):
    """Test that memberships are exported as supplemental attributes and added to database."""
    from r2x_plexos.models import PLEXOSMembership

    sys = serialized_plexos_system
    exporter = PLEXOSExporter(plexos_config, sys)

    # Verify memberships exist as supplemental attributes (not components)
    memberships = list(sys.get_supplemental_attributes(PLEXOSMembership))
    assert len(memberships) > 0, "System should have memberships as supplemental attributes"

    print(f"\n✓ Found {len(memberships)} membership supplemental attributes")

    # Export the system
    result = exporter.export()
    assert result.is_ok(), f"Export failed: {result.error if result.is_err() else ''}"

    # Verify memberships were added to database
    # Exclude System memberships (parent_object_id = 1)
    query = "SELECT COUNT(*) FROM t_membership WHERE parent_object_id != 1"
    result = exporter.db.query(query)
    membership_count = result[0][0] if result else 0

    assert membership_count > 0, "No memberships were added to database"
    print(f"✓ Added {membership_count} non-System memberships to database")

    # Verify specific membership relationships exist
    # Test a few memberships from our supplemental attributes
    verified_count = 0
    for membership in memberships[:5]:  # Check first 5 memberships
        parent_class = PLEXOS_TYPE_MAP_INVERTED.get(type(membership.parent_object))
        if not parent_class:
            continue

        # PLEXOSMembership stores the child object implicitly via the supplemental attribute attachment
        # We need to find which component this membership is attached to
        # For now, we'll verify the parent exists in the database
        try:
            parent_id = exporter.db.get_object_id(parent_class, membership.parent_object.name)
            assert parent_id is not None, (
                f"Parent object {membership.parent_object.name} not found in database"
            )

            # Check that at least one membership exists for this parent
            query = "SELECT COUNT(*) FROM t_membership WHERE parent_object_id = ?"
            result = exporter.db.query(query, (parent_id,))
            count = result[0][0] if result else 0

            if count > 0:
                verified_count += 1
                print(
                    f"✓ Verified memberships for parent: {membership.parent_object.name} (collection={membership.collection})"
                )
        except Exception as e:
            print(f"  Warning: Could not verify membership for {membership.parent_object.name}: {e}")
            continue

    assert verified_count > 0, "Could not verify any membership relationships"
    print(f"✓ Successfully verified {verified_count} membership relationships")
