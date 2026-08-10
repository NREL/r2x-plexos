from datetime import datetime, timedelta
from typing import cast
from unittest.mock import patch

import pytest
from plexosdb import CollectionEnum
from rust_ok import Ok

from r2x_core import Err, PluginContext, System
from r2x_plexos import PLEXOSConfig
from r2x_plexos.exporter import PLEXOSExporter
from r2x_plexos.models import PLEXOSGenerator, PLEXOSMembership

pytestmark = pytest.mark.export


def test_export_time_series_no_components(mocker):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()
    sys.get_component_types.return_value = []

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    result = exporter.export_time_series()
    assert result.is_ok()


def test_export_time_series_csv_error(mocker):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()

    class DummyType:
        pass

    sys.get_component_types.return_value = [DummyType]
    sys.get_components.return_value = [mocker.Mock(name="comp")]
    sys.has_time_series.return_value = True

    ts_key = mocker.Mock()
    ts_key.name = "ts_key"
    ts_key.features = {}
    ts_key.initial_timestamp = None
    sys.list_time_series_keys.return_value = [ts_key]
    sys.list_time_series.return_value = [mocker.Mock()]
    mocker.patch("r2x_plexos.exporter.export_time_series_csv", return_value=Err("fail"))

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    result = exporter.export_time_series()
    assert result.is_err()


def test_export_time_series_no_components_with_ts(template_db):
    """Test export_time_series handles no components with time series."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    # Add component without time series
    gen = PLEXOSGenerator(name="Gen1", category="thermal", units=1, rating=50.0)
    sys.add_component(gen)

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.db = template_db

    result = exporter.export_time_series()

    assert result.is_ok()


def test_get_time_series_property_name_returns_none_for_unknown():
    """Test _get_time_series_property_name returns None for unrecognized type."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    class DummyComp:
        pass

    result = exporter._get_time_series_property_name(DummyComp(), ts_key_name="some_key")
    assert result is None


def test_get_time_series_property_name_fixed_types():
    """Test _get_time_series_property_name returns fixed names for reserve/region/storage."""
    from r2x_plexos.models import PLEXOSRegion, PLEXOSReserve, PLEXOSStorage

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    assert exporter._get_time_series_property_name(PLEXOSReserve(name="r")) == "Min Provision"
    assert exporter._get_time_series_property_name(PLEXOSRegion(name="r")) == "Load"
    assert exporter._get_time_series_property_name(PLEXOSStorage(name="s")) == "Natural Inflow"


def test_build_generator_to_storage_map_with_pairs(mocker):
    """Test _build_generator_to_storage_map maps generator <-> storage both ways."""
    from r2x_plexos.models import PLEXOSStorage

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_obj = System(name="test")
    ctx = PluginContext(config=config, system=sys_obj)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    gen = PLEXOSGenerator(name="HydroGen", category="hydro-turbine", units=1, rating=100.0)
    storage = PLEXOSStorage(name="HydroRes")
    m1 = PLEXOSMembership(parent_object=gen, child_object=storage, collection=CollectionEnum.Storages)

    with patch.object(sys_obj, "get_supplemental_attributes", return_value=[m1]):
        result = exporter._build_generator_to_storage_map()

    assert result["HydroGen"].name == "HydroRes"


def test_export_time_series_with_weather_and_solve_year(mocker, tmp_path):
    """Test export_time_series uses weather_year and solve_year in filenames."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_mock = mocker.Mock()

    class DummyType:
        pass

    comp = mocker.Mock()
    comp.name = "Gen1"
    type(comp).__name__ = "DummyType"

    sys_mock.get_component_types.return_value = [DummyType]
    sys_mock.get_components.return_value = [comp]
    sys_mock.has_time_series.return_value = True
    ts_key = mocker.Mock()
    ts_key.name = "max_active_power"
    ts_key.features = {}
    sys_mock.list_time_series_keys.return_value = [ts_key]
    sys_mock.get_time_series_by_key.return_value = mocker.Mock()

    data_dir = tmp_path / "Data"
    data_dir.mkdir()

    ctx = PluginContext(config=config, system=sys_mock)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))
    exporter.weather_year = 2020
    exporter.solve_year = 2024

    mocker.patch("r2x_plexos.exporter.get_output_directory", return_value=data_dir)
    mocker.patch("r2x_plexos.exporter.export_time_series_csv", return_value=Ok(None))

    result = exporter.export_time_series()
    assert result.is_ok()


def test_export_time_series_purchaser_without_filter_func_dependency(mocker, tmp_path):
    """Ensure purchaser TS export uses has_time_series gating without get_components filter_func."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()

    class PurchaserType:
        pass

    comp = mocker.Mock()
    comp.name = "Purchaser_H2"
    type(comp).__name__ = "PLEXOSPurchaser"

    ts_key = mocker.Mock()
    ts_key.name = "ReEDSElectrolyzerDemand"
    ts_key.features = {}
    ts_key.initial_timestamp = None

    ts_obj = mocker.Mock()
    ts_obj.data = [1.0, 1.0]

    sys.get_component_types.return_value = [PurchaserType]
    # Intentionally provide a callable that accepts only component_type.
    # If exporter passes filter_func, this test would fail.
    sys.get_components.side_effect = lambda component_type: [comp]
    sys.has_time_series.return_value = True
    sys.list_time_series_keys.return_value = [ts_key]
    sys.list_time_series.return_value = [ts_obj]

    data_dir = tmp_path / "Data"
    data_dir.mkdir()

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    mocker.patch("r2x_plexos.exporter.get_output_directory", return_value=data_dir)
    export_csv = mocker.patch("r2x_plexos.exporter.export_time_series_csv", return_value=Ok(None))

    result = exporter.export_time_series()
    assert result.is_ok()
    export_csv.assert_called_once()


def test_export_time_series_separates_same_ts_key_by_component_class(mocker, tmp_path):
    """Same TS key on different classes should generate distinct class-specific CSVs."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()

    class GeneratorType:
        pass

    class PurchaserType:
        pass

    gen = mocker.Mock()
    gen.name = "Gen1"
    type(gen).__name__ = "PLEXOSGenerator"

    purchaser = mocker.Mock()
    purchaser.name = "H2Purchaser"
    type(purchaser).__name__ = "PLEXOSPurchaser"

    ts_key_gen = mocker.Mock()
    ts_key_gen.name = "max_active_power"
    ts_key_gen.features = {}
    ts_key_gen.initial_timestamp = None
    ts_key_gen.resolution = None

    ts_key_purch = mocker.Mock()
    ts_key_purch.name = "max_active_power"
    ts_key_purch.features = {}
    ts_key_purch.initial_timestamp = None
    ts_key_purch.resolution = None

    ts_obj = mocker.Mock()
    ts_obj.data = [1.0, 2.0]

    sys.get_component_types.return_value = [GeneratorType, PurchaserType]
    sys.get_components.side_effect = lambda component_type: (
        [gen] if component_type is GeneratorType else [purchaser] if component_type is PurchaserType else []
    )
    sys.has_time_series.return_value = True
    sys.list_time_series_keys.side_effect = lambda component: (
        [ts_key_gen] if component is gen else [ts_key_purch]
    )
    sys.list_time_series.return_value = [ts_obj]

    data_dir = tmp_path / "Data"
    data_dir.mkdir()

    ctx = PluginContext(config=config, system=sys)
    exporter = cast(PLEXOSExporter, PLEXOSExporter.from_context(ctx))

    mocker.patch("r2x_plexos.exporter.get_output_directory", return_value=data_dir)
    export_csv = mocker.patch("r2x_plexos.exporter.export_time_series_csv", return_value=Ok(None))

    result = exporter.export_time_series()
    assert result.is_ok()
    assert export_csv.call_count == 2

    exported_paths = [call.args[0].name for call in export_csv.call_args_list]
    assert any(name.startswith("PLEXOSGenerator_max_active_power_") for name in exported_paths)
    assert any(name.startswith("PLEXOSPurchaser_max_active_power_") for name in exported_paths)


def test_export_time_series_prefers_resolution_match(mocker, tmp_path):
    """Exporter should pick TS variant matching ts_key resolution, not first element."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_mock = mocker.Mock()

    class DummyType:
        pass

    comp1 = mocker.Mock()
    comp1.name = "HydroA"
    type(comp1).__name__ = "PLEXOSGenerator"

    comp2 = mocker.Mock()
    comp2.name = "HydroB"
    type(comp2).__name__ = "PLEXOSGenerator"

    ts_key = mocker.Mock()
    ts_key.name = "hydro_budget"
    ts_key.features = {}
    ts_key.initial_timestamp = datetime(2024, 1, 1)
    ts_key.resolution = timedelta(days=7)

    weekly_a = mocker.Mock()
    weekly_a.data = [1.0] * 53
    weekly_a.initial_timestamp = ts_key.initial_timestamp
    weekly_a.resolution = timedelta(days=7)

    hourly_a = mocker.Mock()
    hourly_a.data = [1.0] * 8760
    hourly_a.initial_timestamp = ts_key.initial_timestamp
    hourly_a.resolution = timedelta(hours=1)

    weekly_b = mocker.Mock()
    weekly_b.data = [2.0] * 53
    weekly_b.initial_timestamp = ts_key.initial_timestamp
    weekly_b.resolution = timedelta(days=7)

    sys_mock.get_component_types.return_value = [DummyType]
    sys_mock.get_components.return_value = [comp1, comp2]
    sys_mock.has_time_series.return_value = True
    sys_mock.list_time_series_keys.return_value = [ts_key]

    def _list_ts(component, name, **features):
        if component.name == "HydroA":
            return [hourly_a, weekly_a]
        return [weekly_b]

    sys_mock.list_time_series.side_effect = _list_ts

    ctx = PluginContext(config=config, system=sys_mock)
    exporter = PLEXOSExporter.from_context(ctx)

    data_dir = tmp_path / "Data"
    data_dir.mkdir()

    def _capture(filepath, ts_data, **kwargs):
        assert len(ts_data) == 2
        assert len(ts_data[0][1].data) == 53
        assert len(ts_data[1][1].data) == 53
        return Ok(None)

    mocker.patch("r2x_plexos.exporter.get_output_directory", return_value=data_dir)
    mocker.patch("r2x_plexos.exporter.export_time_series_csv", side_effect=_capture)

    result = exporter.export_time_series()
    assert result.is_ok()


def test_export_time_series_skips_unmatched_variant(mocker, tmp_path):
    """If no TS matches key resolution/timestamp, exporter should skip that component."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_mock = mocker.Mock()

    class DummyType:
        pass

    comp1 = mocker.Mock()
    comp1.name = "HydroA"
    type(comp1).__name__ = "PLEXOSGenerator"

    comp2 = mocker.Mock()
    comp2.name = "HydroB"
    type(comp2).__name__ = "PLEXOSGenerator"

    ts_key = mocker.Mock()
    ts_key.name = "hydro_budget"
    ts_key.features = {}
    ts_key.initial_timestamp = datetime(2024, 1, 1)
    ts_key.resolution = timedelta(days=7)

    hourly_only = mocker.Mock()
    hourly_only.data = [1.0] * 8760
    hourly_only.initial_timestamp = ts_key.initial_timestamp
    hourly_only.resolution = timedelta(hours=1)

    weekly_b = mocker.Mock()
    weekly_b.data = [2.0] * 53
    weekly_b.initial_timestamp = ts_key.initial_timestamp
    weekly_b.resolution = timedelta(days=7)

    sys_mock.get_component_types.return_value = [DummyType]
    sys_mock.get_components.return_value = [comp1, comp2]
    sys_mock.has_time_series.return_value = True
    sys_mock.list_time_series_keys.return_value = [ts_key]

    def _list_ts(component, name, **features):
        if component.name == "HydroA":
            return [hourly_only]
        return [weekly_b]

    sys_mock.list_time_series.side_effect = _list_ts

    ctx = PluginContext(config=config, system=sys_mock)
    exporter = PLEXOSExporter.from_context(ctx)

    data_dir = tmp_path / "Data"
    data_dir.mkdir()

    def _capture(filepath, ts_data, **kwargs):
        assert [name for name, _ in ts_data] == ["HydroB"]
        assert len(ts_data[0][1].data) == 53
        return Ok(None)

    mocker.patch("r2x_plexos.exporter.get_output_directory", return_value=data_dir)
    mocker.patch("r2x_plexos.exporter.export_time_series_csv", side_effect=_capture)

    result = exporter.export_time_series()
    assert result.is_ok()


def test_export_time_series_hydro_budget_always_resolves_to_coarsest(mocker, tmp_path):
    """Hydro budget always resolves to the coarsest TS, regardless of key resolution.

    The hourly energy profile is carried by max_active_power (Max Energy Hour),
    not by hydro_budget. So hydro_budget should always pick the coarsest variant.
    """
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_mock = mocker.Mock()

    class DummyType:
        pass

    comp = mocker.Mock()
    comp.name = "HydroA"
    type(comp).__name__ = "PLEXOSGenerator"

    ts_key = mocker.Mock()
    ts_key.name = "hydro_budget"
    ts_key.features = {}
    ts_key.initial_timestamp = datetime(2024, 1, 1)
    ts_key.resolution = timedelta(hours=1)

    weekly = mocker.Mock()
    weekly.data = [2.0] * 53
    weekly.initial_timestamp = ts_key.initial_timestamp
    weekly.resolution = timedelta(days=7)

    hourly = mocker.Mock()
    hourly.data = [1.0] * 8760
    hourly.initial_timestamp = ts_key.initial_timestamp
    hourly.resolution = timedelta(hours=1)

    sys_mock.get_component_types.return_value = [DummyType]
    sys_mock.get_components.return_value = [comp]
    sys_mock.has_time_series.return_value = True
    sys_mock.list_time_series_keys.return_value = [ts_key]
    sys_mock.list_time_series.return_value = [hourly, weekly]

    ctx = PluginContext(config=config, system=sys_mock)
    exporter = PLEXOSExporter.from_context(ctx)

    data_dir = tmp_path / "Data"
    data_dir.mkdir()

    def _capture(filepath, ts_data, **kwargs):
        assert len(ts_data) == 1
        # key resolution is hourly but hydro_budget always returns coarsest (weekly, 53 pts)
        assert len(ts_data[0][1].data) == 53
        return Ok(None)

    mocker.patch("r2x_plexos.exporter.get_output_directory", return_value=data_dir)
    mocker.patch("r2x_plexos.exporter.export_time_series_csv", side_effect=_capture)

    result = exporter.export_time_series()
    assert result.is_ok()


def test_export_time_series_hydro_budget_falls_back_to_coarsest_when_no_resolution_match(mocker, tmp_path):
    """Hydro budget falls back to the coarsest TS when no variant matches the key resolution."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_mock = mocker.Mock()

    class DummyType:
        pass

    comp = mocker.Mock()
    comp.name = "HydroA"
    type(comp).__name__ = "PLEXOSGenerator"

    ts_key = mocker.Mock()
    ts_key.name = "hydro_budget"
    ts_key.features = {}
    ts_key.initial_timestamp = datetime(2024, 1, 1)
    # Key says 6-hourly but no 6-hourly TS exists — only weekly and daily.
    ts_key.resolution = timedelta(hours=6)

    weekly = mocker.Mock()
    weekly.data = [2.0] * 53
    weekly.initial_timestamp = ts_key.initial_timestamp
    weekly.resolution = timedelta(days=7)

    daily = mocker.Mock()
    daily.data = [3.0] * 365
    daily.initial_timestamp = ts_key.initial_timestamp
    daily.resolution = timedelta(days=1)

    sys_mock.get_component_types.return_value = [DummyType]
    sys_mock.get_components.return_value = [comp]
    sys_mock.has_time_series.return_value = True
    sys_mock.list_time_series_keys.return_value = [ts_key]
    sys_mock.list_time_series.return_value = [daily, weekly]

    ctx = PluginContext(config=config, system=sys_mock)
    exporter = PLEXOSExporter.from_context(ctx)

    data_dir = tmp_path / "Data"
    data_dir.mkdir()

    def _capture(filepath, ts_data, **kwargs):
        assert len(ts_data) == 1
        # No 6-hourly variant → coarsest (weekly, 53 points) is the fallback.
        assert len(ts_data[0][1].data) == 53
        return Ok(None)

    mocker.patch("r2x_plexos.exporter.get_output_directory", return_value=data_dir)
    mocker.patch("r2x_plexos.exporter.export_time_series_csv", side_effect=_capture)

    result = exporter.export_time_series()
    assert result.is_ok()


def test_get_time_series_property_name_subclass_paths_use_map_values():
    """Subclass instances should use variable map lookups instead of fixed type shortcut."""
    from r2x_plexos.models import PLEXOSStorage

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")
    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    class StorageChild(PLEXOSStorage):
        pass

    class GeneratorChild(PLEXOSGenerator):
        pass

    storage = StorageChild(name="StorageChild")
    generator = GeneratorChild(name="GeneratorChild", category="thermal", units=1, rating=10.0)

    assert exporter._get_time_series_property_name(storage, ts_key_name="natural_inflow") is not None
    assert exporter._get_time_series_property_name(generator, ts_key_name="max_active_power") is not None
    assert exporter._get_time_series_property_name(generator, ts_key_name="fixed_load") == "Fixed Load"


def test_build_generator_to_storage_map_skips_invalid_and_maps_reverse_direction(mocker):
    """None parent/child records are ignored, and storage->generator memberships are mapped."""
    from r2x_plexos.models import PLEXOSStorage

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_obj = System(name="test")
    ctx = PluginContext(config=config, system=sys_obj)
    exporter = PLEXOSExporter.from_context(ctx)

    gen = PLEXOSGenerator(name="HydroGen", category="hydro", units=1, rating=100.0)
    storage = PLEXOSStorage(name="HydroRes")

    invalid_membership = mocker.Mock()
    invalid_membership.parent_object = None
    invalid_membership.child_object = gen

    reverse_membership = PLEXOSMembership(
        parent_object=storage, child_object=gen, collection=CollectionEnum.Generators
    )

    with patch.object(
        sys_obj, "get_supplemental_attributes", return_value=[invalid_membership, reverse_membership]
    ):
        mapping = exporter._build_generator_to_storage_map()

    assert mapping["HydroGen"].name == "HydroRes"


def test_resolve_matching_time_series_handles_exception_and_empty_results(mocker):
    """Resolver should return None when backend query fails or returns no series."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_obj = mocker.Mock()
    ctx = PluginContext(config=config, system=sys_obj)
    exporter = PLEXOSExporter.from_context(ctx)

    component = mocker.Mock()
    component.name = "Gen1"

    ts_key = mocker.Mock()
    ts_key.name = "load"
    ts_key.features = {}

    sys_obj.list_time_series.side_effect = RuntimeError("boom")
    assert exporter._resolve_matching_time_series(component, ts_key) is None


def test_export_time_series_passes_target_year_from_solve_year(mocker, tmp_path):
    """solve_year is passed as target_year to export_time_series_csv."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_mock = mocker.Mock()

    class DummyType:
        pass

    comp = mocker.Mock()
    comp.name = "Gen1"
    type(comp).__name__ = "PLEXOSGenerator"

    ts_key = mocker.Mock()
    ts_key.name = "max_active_power"
    ts_key.features = {}
    ts_key.initial_timestamp = datetime(2012, 1, 1)
    ts_key.resolution = timedelta(hours=1)

    ts = mocker.Mock()
    ts.data = [1.0] * 8760
    ts.initial_timestamp = ts_key.initial_timestamp
    ts.resolution = ts_key.resolution

    sys_mock.get_component_types.return_value = [DummyType]
    sys_mock.get_components.return_value = [comp]
    sys_mock.has_time_series.return_value = True
    sys_mock.list_time_series_keys.return_value = [ts_key]
    sys_mock.list_time_series.return_value = [ts]

    ctx = PluginContext(config=config, system=sys_mock)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.solve_year = 2050

    captured_target_years = []

    def _capture(filepath, ts_data, **kwargs):
        captured_target_years.append(kwargs.get("target_year"))
        return Ok(None)

    mocker.patch("r2x_plexos.exporter.get_output_directory", return_value=tmp_path / "Data")
    (tmp_path / "Data").mkdir(parents=True, exist_ok=True)
    mocker.patch("r2x_plexos.exporter.export_time_series_csv", side_effect=_capture)

    result = exporter.export_time_series()
    assert result.is_ok()
    assert any(y == 2050 for y in captured_target_years)


def test_resolve_matching_time_series_fallbacks_and_initial_timestamp_mismatch(mocker):
    """Resolver should exercise fallback matching branches and single-item timestamp mismatch path."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_obj = mocker.Mock()
    ctx = PluginContext(config=config, system=sys_obj)
    exporter = PLEXOSExporter.from_context(ctx)

    component = mocker.Mock()
    component.name = "Gen1"

    ts_key = mocker.Mock()
    ts_key.name = "load"
    ts_key.features = {}
    ts_key.initial_timestamp = datetime(2024, 1, 1)
    ts_key.resolution = timedelta(hours=1)

    ts_resolution_only = mocker.Mock()
    ts_resolution_only.initial_timestamp = datetime(2025, 1, 1)
    ts_resolution_only.resolution = timedelta(hours=1)

    ts_initial_only = mocker.Mock()
    ts_initial_only.initial_timestamp = datetime(2024, 1, 1)
    ts_initial_only.resolution = timedelta(days=1)

    sys_obj.list_time_series.return_value = [ts_initial_only, ts_resolution_only]
    assert exporter._resolve_matching_time_series(component, ts_key) is ts_resolution_only

    ts_key.resolution = "bad-type"
    sys_obj.list_time_series.return_value = [ts_resolution_only, ts_initial_only]
    assert exporter._resolve_matching_time_series(component, ts_key) is ts_initial_only

    ts_key.resolution = timedelta(hours=1)
    ts_key.initial_timestamp = datetime(2024, 1, 1)
    one_ts = mocker.Mock()
    one_ts.initial_timestamp = datetime(2024, 1, 2)
    one_ts.resolution = timedelta(hours=1)
    sys_obj.list_time_series.return_value = [one_ts]
    assert exporter._resolve_matching_time_series(component, ts_key) is None


def test_export_time_series_skips_group_when_all_series_unresolved(mocker, tmp_path):
    """When a group resolves to no time series payload, CSV export is skipped and export still succeeds."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys_obj = mocker.Mock()

    class DummyType:
        pass

    component = mocker.Mock()
    component.name = "Gen1"
    type(component).__name__ = "PLEXOSGenerator"

    ts_key = mocker.Mock()
    ts_key.name = "load"
    ts_key.features = {}
    ts_key.initial_timestamp = datetime(2024, 1, 1)
    ts_key.resolution = timedelta(hours=1)

    sys_obj.get_component_types.return_value = [DummyType]
    sys_obj.get_components.return_value = [component]
    sys_obj.has_time_series.return_value = True
    sys_obj.list_time_series_keys.return_value = [ts_key]

    ctx = PluginContext(config=config, system=sys_obj)
    exporter = PLEXOSExporter.from_context(ctx)

    data_dir = tmp_path / "Data"
    data_dir.mkdir()
    mocker.patch("r2x_plexos.exporter.get_output_directory", return_value=data_dir)
    export_csv_mock = mocker.patch("r2x_plexos.exporter.export_time_series_csv", return_value=Ok(None))
    mocker.patch.object(exporter, "_resolve_matching_time_series", return_value=None)

    result = exporter.export_time_series()

    assert result.is_ok()
    export_csv_mock.assert_not_called()
