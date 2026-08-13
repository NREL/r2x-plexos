from typing import cast
from unittest.mock import patch

import pytest
from plexosdb import ClassEnum, CollectionEnum

from r2x_core import PluginContext, System
from r2x_plexos import PLEXOSConfig, PLEXOSPropertyValue
from r2x_plexos.exporter import FLOW_CLIP_MEMO_TEXT, PLEXOSExporter
from r2x_plexos.models import PLEXOSDatafile, PLEXOSGenerator, PLEXOSLine, PLEXOSMembership, PLEXOSNode

pytestmark = pytest.mark.export


def test_add_component_memberships_no_memberships(mocker):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()
    sys.get_supplemental_attributes.return_value = []

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    exporter._add_component_memberships()


def test_add_component_memberships_skips_invalid(mocker):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()
    membership = mocker.Mock()
    membership.parent_object = None
    membership.child_object = None
    sys.get_supplemental_attributes.return_value = [membership]

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    exporter._add_component_memberships()


def test_create_datafile_objects_no_dir(tmp_path, mocker):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.output_path = str(tmp_path)

    exporter._create_datafile_objects()


def test_add_component_datafile_objects_no_datafiles(mocker):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()
    sys.get_components.return_value = []

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    exporter._add_component_datafile_objects()


def test_add_component_datafile_objects_filename_none(mocker):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()
    datafile = mocker.Mock()
    datafile.name = "test"
    datafile.filename = None
    sys.get_components.return_value = [datafile]

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    mocker.patch.object(exporter, "_create_datafile_objects")
    exporter._add_component_datafile_objects()


def test_add_component_properties_db_none_logs_error(caplog):
    """Test _add_component_properties handles db None - lines 363-364."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = None

    exporter._add_component_properties()

    assert "Database not initialized" in caplog.text


def test_add_component_properties_adds_datafile_filename(template_db):
    """Test _add_component_properties adds Filename property for DataFile - lines 370-371."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    datafile = PLEXOSDatafile(
        name="TestFile", filename=PLEXOSPropertyValue.from_dict({"datafile_name": "test.csv"})
    )
    sys.add_component(datafile)

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    template_db.add_object(ClassEnum.DataFile, "TestFile", category="CSV")
    exporter._add_component_properties()

    props = template_db.get_object_properties(ClassEnum.DataFile, "TestFile")
    prop_names = [p.get("property") for p in props]
    assert "Filename" in prop_names


def test_add_component_properties_filters_metadata_fields(template_db):
    """Test _add_component_properties filters out metadata fields - lines 392-393."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    gen = PLEXOSGenerator(
        name="TestGen",
        category="coaloldscr",
        units=1,
        rating=50.0,
    )
    sys.add_component(gen)

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    template_db.add_object(ClassEnum.Generator, "TestGen", category="thermal")

    exporter._add_component_properties()

    props = template_db.get_object_properties(ClassEnum.Generator, "TestGen")
    prop_names = [p.get("property") for p in props]

    assert "name" not in [pn.lower() for pn in prop_names if pn]
    assert "category" not in [pn.lower() for pn in prop_names if pn]
    assert "Units" in prop_names
    assert "Rating" in prop_names
    assert "Forced Outage Rate" in prop_names
    assert "Min Stable Level" in prop_names


def test_add_component_properties_handles_dict_with_text(template_db):
    """Test _add_component_properties handles dict properties with 'text' - lines 406-408."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    # Create a generator with a property that's a dict with 'text'
    gen = PLEXOSGenerator(name="TestGen", category="thermal", units=1, rating=50.0)
    # Manually set a property as dict with 'text'
    gen.max_capacity = PLEXOSPropertyValue.from_dict({"datafile_name": "test.csv"})  # ty: ignore[invalid-assignment]
    sys.add_component(gen)

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    template_db.add_object(ClassEnum.Generator, "TestGen", category="thermal")

    exporter._add_component_properties()

    # Properties should be added
    props = template_db.get_object_properties(ClassEnum.Generator, "TestGen")
    assert len(props) > 0


def test_add_component_properties_skips_none_values(template_db):
    """Test _add_component_properties skips None values and time series properties."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    gen = PLEXOSGenerator(
        name="TestGen",
        category="biopower",
        units=1,
        rating=50.0,
    )
    sys.add_component(gen)

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    template_db.add_object(ClassEnum.Generator, "TestGen", category="thermal")

    exporter._add_component_properties()

    props = template_db.get_object_properties(ClassEnum.Generator, "TestGen")
    prop_names = [p.get("property") for p in props]

    assert "Units" in prop_names
    assert "Rating" in prop_names
    assert "Forced Outage Rate" in prop_names
    assert "Min Stable Level" in prop_names
    assert "Maintenance Rate" in prop_names
    assert "Mean Time to Repair" in prop_names


def test_add_component_properties_does_not_export_explicit_default_values(template_db):
    """Regression: explicit default-valued fields should not be exported with exclude_defaults=True."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    # Expansion Economy Units defaults to 0 and is not required for thermal generators.
    # Setting it explicitly should not force export when exclude_defaults=True.
    gen = PLEXOSGenerator(
        name="TestGen",
        category="thermal",
        units=1,
        rating=50.0,
        expansion_economy_units=0,
    )
    sys.add_component(gen)

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db
    exporter.exclude_defaults = True

    template_db.add_object(ClassEnum.Generator, "TestGen", category="thermal")

    exporter._add_component_properties()

    props = template_db.get_object_properties(ClassEnum.Generator, "TestGen")
    prop_names = [p.get("property") for p in props]

    assert "Expansion Economy Units" not in prop_names


def test_add_component_properties_adds_line_flow_clip_memo(template_db):
    """Memo text is added only for line Min/Max Flow rows clipped to -99999/99999."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    clipped = PLEXOSLine(name="ClippedLine", max_flow=99999, min_flow=-99999)
    normal = PLEXOSLine(name="NormalLine", max_flow=8000, min_flow=-8000)
    sys.add_component(clipped)
    sys.add_component(normal)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    template_db.add_object(ClassEnum.Line, "ClippedLine", category="-")
    template_db.add_object(ClassEnum.Line, "NormalLine", category="-")

    exporter._add_component_properties()

    rows = template_db.query(
        """
        SELECT o.name, p.name, md.value
        FROM t_memo_data md
        INNER JOIN t_data d ON d.data_id = md.data_id
        INNER JOIN t_property p ON p.property_id = d.property_id
        INNER JOIN t_membership m ON m.membership_id = d.membership_id
        INNER JOIN t_object o ON o.object_id = m.child_object_id
        INNER JOIN t_class c ON c.class_id = o.class_id
        WHERE c.name = 'Line' AND p.name IN ('Min Flow', 'Max Flow')
        """
    )

    assert len(rows) == 2
    assert {r[0] for r in rows} == {"ClippedLine"}
    assert {r[1] for r in rows} == {"Min Flow", "Max Flow"}
    assert all(r[2] == FLOW_CLIP_MEMO_TEXT for r in rows)


def test_add_component_properties_removes_heat_rate_when_curve_defined(template_db):
    """Use heat-rate curve terms over single-point heat rate when both are present."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    gen = PLEXOSGenerator(
        name="CurveHeatRateGen",
        category="thermal",
        units=1,
        rating=50.0,
        heat_rate=10.0,
        heat_rate_base=8.0,
        heat_rate_incr=2.0,
    )
    sys.add_component(gen)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    template_db.add_object(ClassEnum.Generator, "CurveHeatRateGen", category="thermal")

    exporter._add_component_properties()

    props = template_db.get_object_properties(ClassEnum.Generator, "CurveHeatRateGen")
    prop_names = {p.get("property") for p in props}

    assert "Heat Rate Base" in prop_names
    assert "Heat Rate Incr" in prop_names
    assert "Heat Rate" not in prop_names


def test_add_component_memberships_db_none_logs_error(caplog):
    """Test _add_component_memberships handles db None - lines 429-440."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = None

    exporter._add_component_memberships()

    assert "Database not initialized" in caplog.text


def test_add_component_memberships_no_memberships_warns(template_db, caplog):
    """Test _add_component_memberships warns when no memberships found - line 444."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    exporter._add_component_memberships()

    assert "No memberships found" in caplog.text


def test_add_component_memberships_skips_missing_parent_or_child(template_db, caplog):
    """Test _add_component_memberships skips memberships with missing objects."""
    from unittest.mock import Mock, patch

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    gen = PLEXOSGenerator(name="Gen1", category="thermal", units=1, rating=50.0)
    node = PLEXOSNode(name="Node1")
    sys.add_component(gen)
    sys.add_component(node)

    _ = PLEXOSMembership(parent_object=node, child_object=gen, collection=CollectionEnum.Generators)

    mock_membership = Mock()
    mock_membership.parent_object = None
    mock_membership.child_object = None
    mock_membership.collection = CollectionEnum.Generators

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    with patch.object(sys, "get_supplemental_attributes", return_value=[mock_membership]):
        exporter._add_component_memberships()

    assert "No valid membership records to add." in caplog.text


def test_add_component_memberships_no_valid_records_warns(template_db, caplog):
    """Test _add_component_memberships warns when no valid records - line 495."""
    from unittest.mock import Mock, patch

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    gen = PLEXOSGenerator(name="Gen1", category="thermal", units=1, rating=50.0)
    node = PLEXOSNode(name="Node1")

    mock_membership = Mock()
    mock_membership.parent_object = node
    mock_membership.child_object = gen
    mock_membership.collection = None

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    with patch.object(sys, "get_supplemental_attributes", return_value=[mock_membership]):
        exporter._add_component_memberships()

    assert "No valid membership records to add." in caplog.text


def test_add_component_memberships_enables_used_collections(template_db):
    """After bulk insert, _add_component_memberships sets is_enabled=1 for all used collections.

    add_memberships_from_records (bulk path) does not flip the is_enabled flag on
    t_collection. The exporter must do so explicitly; otherwise PLEXOS ignores the
    memberships because the collection appears disabled.
    """
    from unittest.mock import Mock, patch

    from r2x_plexos.models import PLEXOSRegion

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_obj = System(name="test")

    region = PLEXOSRegion(name="RegA")
    node = PLEXOSNode(name="NodeA")

    template_db.add_object(ClassEnum.Region, "RegA", category="default")
    template_db.add_object(ClassEnum.Node, "NodeA", category="default")

    coll_id = template_db.get_collection_id(
        CollectionEnum.ReferenceNode,
        parent_class_enum=ClassEnum.Region,
        child_class_enum=ClassEnum.Node,
    )
    row_before = template_db._db.fetchone(
        "SELECT is_enabled FROM t_collection WHERE collection_id=?", (coll_id,)
    )
    assert row_before is not None
    assert row_before[0] == 0, "Precondition: collection must start with is_enabled=0"

    mock_membership = Mock()
    mock_membership.parent_object = region
    mock_membership.child_object = node
    mock_membership.collection = CollectionEnum.ReferenceNode

    ctx = PluginContext(config=config, system=sys_obj)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    with patch.object(sys_obj, "get_supplemental_attributes", return_value=[mock_membership]):
        exporter._add_component_memberships()

    row_after = template_db._db.fetchone(
        "SELECT is_enabled FROM t_collection WHERE collection_id=?", (coll_id,)
    )
    assert row_after is not None
    assert row_after[0] == 1, "is_enabled must be 1 after _add_component_memberships"


def test_add_component_memberships_reference_node_row_inserted(template_db):
    """Region→Node 'Reference Node' membership row must exist in t_membership after export.

    Exercises the full exporter path for the ReferenceNode collection (collection_id=211):
    objects are resolved, the membership record is built and bulk-inserted, and the
    collection is enabled so PLEXOS can read it.
    """
    from unittest.mock import Mock, patch

    from r2x_plexos.models import PLEXOSRegion

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_obj = System(name="test")

    region = PLEXOSRegion(name="RegB")
    node = PLEXOSNode(name="NodeB")

    template_db.add_object(ClassEnum.Region, "RegB", category="default")
    template_db.add_object(ClassEnum.Node, "NodeB", category="default")

    mock_membership = Mock()
    mock_membership.parent_object = region
    mock_membership.child_object = node
    mock_membership.collection = CollectionEnum.ReferenceNode

    ctx = PluginContext(config=config, system=sys_obj)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    with patch.object(sys_obj, "get_supplemental_attributes", return_value=[mock_membership]):
        exporter._add_component_memberships()

    region_id = template_db.get_object_id(ClassEnum.Region, "RegB")
    node_id = template_db.get_object_id(ClassEnum.Node, "NodeB")
    coll_id = template_db.get_collection_id(
        CollectionEnum.ReferenceNode,
        parent_class_enum=ClassEnum.Region,
        child_class_enum=ClassEnum.Node,
    )

    count = template_db._db.fetchone(
        "SELECT COUNT(*) FROM t_membership WHERE parent_object_id=? AND child_object_id=? AND collection_id=?",
        (region_id, node_id, coll_id),
    )
    assert count is not None
    assert count[0] == 1, "Exactly one Region→Node Reference Node membership row expected"


def test_add_component_datafile_objects_db_none(caplog):
    """Test _add_component_datafile_objects handles db None - lines 527, 529."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = None

    exporter._add_component_datafile_objects()

    assert "Database not initialized" in caplog.text


def test_add_component_datafile_objects_updates_object_ids(template_db):
    """Test _add_component_datafile_objects updates object_id and datafile_id - line 557."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    datafile = PLEXOSDatafile(
        name="TestFile", filename=PLEXOSPropertyValue.from_dict({"datafile_name": "test.csv"})
    )
    sys.add_component(datafile)

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    exporter._add_component_datafile_objects()

    assert datafile.object_id is not None


def test_add_component_datafile_objects_handles_no_filename(template_db, caplog):
    """Test _add_component_datafile_objects handles datafile without filename."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    datafile = PLEXOSDatafile(name="TestFile", filename=None)
    sys.add_component(datafile)

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    exporter._add_component_datafile_objects()

    datafiles_in_db = template_db.list_objects_by_class(ClassEnum.DataFile)
    assert "TestFile" in datafiles_in_db


def test_create_datafile_objects_no_directory(tmp_path, caplog):
    """Test _create_datafile_objects handles missing directory."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    # Use a non-existent path that won't be created
    non_existent_path = tmp_path / "does_not_exist" / "nested"
    exporter.output_path = str(non_existent_path)

    # Mock get_output_directory to return a path that doesn't exist
    with patch("r2x_plexos.exporter.get_output_directory", return_value=non_existent_path / "Data"):
        exporter._create_datafile_objects()

    assert "No time series directory found" in caplog.text


def test_create_datafile_objects_creates_from_csv_files(tmp_path):
    """Test _create_datafile_objects creates DataFile objects from CSV files."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    data_dir = tmp_path / "Data"
    data_dir.mkdir()
    (data_dir / "test1.csv").write_text("col1,col2\n1,2\n")
    (data_dir / "test2.csv").write_text("col1,col2\n3,4\n")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.output_path = str(tmp_path)

    exporter._create_datafile_objects()

    datafiles = list(sys.get_components(PLEXOSDatafile))
    assert len(datafiles) == 2
    assert any(df.name == "test1" for df in datafiles)
    assert any(df.name == "test2" for df in datafiles)


def test_create_datafile_objects_skips_existing_component(tmp_path):
    """Test _create_datafile_objects creates DataFile retrievable by name."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    data_dir = tmp_path / "Data"
    data_dir.mkdir()
    (data_dir / "ts1.csv").write_text("col\n1\n")

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.output_path = str(tmp_path)

    exporter._create_datafile_objects()

    datafiles = list(sys.get_components(PLEXOSDatafile))
    assert any(df.name == "ts1" for df in datafiles)


def test_link_datafiles_to_components_db_none(caplog):
    """Test _link_datafiles_to_components logs error when db is None."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = None

    exporter._link_datafiles_to_components()
    assert "Database not initialized" in caplog.text


def test_link_datafiles_to_components_missing_output_dir(template_db, tmp_path, caplog):
    """Test _link_datafiles_to_components handles FileNotFoundError for output dir."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    non_existent = tmp_path / "no_such_dir" / "Data"
    with patch("r2x_plexos.exporter.get_output_directory", return_value=non_existent):
        exporter._link_datafiles_to_components()
    assert "not found" in caplog.text


def test_add_component_properties_list_raw(template_db):
    """Test _add_component_properties handles list-of-dict property values."""
    from r2x_plexos.models import PLEXOSReserve

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    reserve = PLEXOSReserve(name="Res1", type=1, duration=15)
    sys.add_component(reserve)

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    template_db.add_object(ClassEnum.Reserve, "Res1", category="variable-reserve")

    exporter._add_component_properties()

    props = template_db.get_object_properties(ClassEnum.Reserve, "Res1")
    assert len(props) > 0


def test_add_component_properties_skips_ts_property(template_db, mocker):
    """Test _add_component_properties skips static value for ts-linked properties."""
    from r2x_plexos.models import PLEXOSRegion

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    region = PLEXOSRegion(name="RegA")
    sys.add_component(region)

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    template_db.add_object(ClassEnum.Region, "RegA", category="default")

    # Simulate that this region has time series
    mocker.patch.object(sys, "has_time_series", return_value=True)

    ts_key = mocker.Mock()
    ts_key.name = "load"
    mocker.patch.object(sys, "list_time_series_keys", return_value=[ts_key])

    exporter._add_component_properties()

    props = template_db.get_object_properties(ClassEnum.Region, "RegA")
    prop_names = [p.get("property") for p in props]
    # "Load" should NOT appear as a plain static value
    assert "Load" not in prop_names


def test_add_component_properties_skips_purchaser_fixed_load_ts_property(template_db, mocker):
    """Test purchaser Fixed Load is omitted when it is supplied by a time series."""
    from r2x_plexos.models import PLEXOSPurchaser

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    purchaser = PLEXOSPurchaser(name="PurchaserA")
    sys.add_component(purchaser)

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    template_db.add_object(ClassEnum.Purchaser, "PurchaserA", category="default")

    mocker.patch.object(sys, "has_time_series", return_value=True)

    ts_key = mocker.Mock()
    ts_key.name = "max_active_power"
    mocker.patch.object(sys, "list_time_series_keys", return_value=[ts_key])

    exporter._add_component_properties()

    props = template_db.get_object_properties(ClassEnum.Purchaser, "PurchaserA")
    prop_names = [p.get("property") for p in props]
    assert "Fixed Load" not in prop_names


def test_link_datafiles_to_components_links_generator_via_fallback_filename(mocker, tmp_path):
    """Class-prefix fallback matches when exact solve-year filename is absent."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_obj = mocker.Mock()

    generator = PLEXOSGenerator(name="GenA", category="thermal", units=1, rating=50.0)
    # File uses class prefix but a different suffix, so exact match won't fire.
    fallback_filename = "PLEXOSGenerator_max_active_power_OtherSuffix.csv"
    datafile = PLEXOSDatafile(
        name="PLEXOSGenerator_max_active_power_OtherSuffix",
        filename=PLEXOSPropertyValue.from_dict({"datafile_name": f"Data/{fallback_filename}"}),
    )
    datafile.object_id = 101

    ts_key = mocker.Mock()
    ts_key.name = "max active power"
    ts_key.features = {}

    class DummyType:
        pass

    sys_obj.get_component_types.return_value = [DummyType]
    sys_obj.get_components.return_value = [generator]
    sys_obj.has_time_series.return_value = True
    sys_obj.list_time_series_keys.return_value = [ts_key]

    def _get_component(component_type, name):
        if component_type is PLEXOSDatafile and name == "PLEXOSGenerator_max_active_power_OtherSuffix":
            return datafile
        return None

    sys_obj.get_component.side_effect = _get_component
    sys_obj.get_supplemental_attributes.return_value = []

    ctx = PluginContext(config=config, system=sys_obj)
    exporter = PLEXOSExporter.from_context(ctx)

    db = mocker.Mock()
    db._db = mocker.Mock()
    exporter.db = db

    output_dir = tmp_path / "Data"
    output_dir.mkdir()
    mocker.patch("r2x_plexos.exporter.get_output_directory", return_value=output_dir)
    mocker.patch("r2x_plexos.exporter.os.listdir", return_value=[fallback_filename])
    mocker.patch.object(exporter, "_resolve_matching_time_series", return_value=None)

    exporter._link_datafiles_to_components()

    property_names = {call.kwargs.get("name") for call in db.add_property.call_args_list}
    assert "Rating" in property_names
    assert "Load Subtracter" in property_names


def test_link_datafiles_max_active_power_hydro_er_sienna_type_maps_to_max_energy_hour(mocker, tmp_path):
    """max_active_power on a generator with sienna_type=HydroEnergyReservoir links to Max Energy Hour."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_obj = mocker.Mock()

    generator = PLEXOSGenerator(
        name="HydroER",
        category="some-other-cat",  # does NOT match _hydro_er_cats codes
        units=1,
        rating=100.0,
        ext={"sienna_type": "HydroEnergyReservoir"},
    )
    datafile = PLEXOSDatafile(
        name="PLEXOSGenerator_max_active_power_Base_2024",
        filename=PLEXOSPropertyValue.from_dict(
            {"datafile_name": "Data/PLEXOSGenerator_max_active_power_Base_2024.csv"}
        ),
    )
    datafile.object_id = 42

    ts_key = mocker.Mock()
    ts_key.name = "max_active_power"
    ts_key.features = {}

    class DummyType:
        pass

    sys_obj.get_component_types.return_value = [DummyType]
    sys_obj.get_components.return_value = [generator]
    sys_obj.has_time_series.return_value = True
    sys_obj.list_time_series_keys.return_value = [ts_key]

    def _get_component(component_type, name):
        if component_type is PLEXOSDatafile and name == "PLEXOSGenerator_max_active_power_Base_2024":
            return datafile
        return None

    sys_obj.get_component.side_effect = _get_component
    sys_obj.get_supplemental_attributes.return_value = []

    ctx = PluginContext(config=config, system=sys_obj)
    exporter = PLEXOSExporter.from_context(ctx)

    db = mocker.Mock()
    db._db = mocker.Mock()
    exporter.db = db

    output_dir = tmp_path / "Data"
    output_dir.mkdir()
    (output_dir / "PLEXOSGenerator_max_active_power_Base_2024.csv").write_text("DateTime,HydroER\n")
    mocker.patch("r2x_plexos.exporter.get_output_directory", return_value=output_dir)
    mocker.patch(
        "r2x_plexos.exporter.os.listdir", return_value=["PLEXOSGenerator_max_active_power_Base_2024.csv"]
    )
    mocker.patch.object(exporter, "_resolve_matching_time_series", return_value=None)

    exporter._link_datafiles_to_components()

    property_names = {call.kwargs.get("name") for call in db.add_property.call_args_list}
    assert "Max Energy Hour" in property_names
    assert "Rating" not in property_names
    assert "Load Subtracter" not in property_names
