from datetime import date

import pytest
from plexosdb import ClassEnum, CollectionEnum, PlexosDB

from .profiles import NORMALIZED_SOLAR_PROFILE, NORMALIZED_WIND_PROFILE


@pytest.fixture
def db_base():
    from datetime import datetime

    from r2x_plexos.config import PLEXOSConfig
    from r2x_plexos.exporter import DEFAULT_XML_TEMPLATE
    from r2x_plexos.utils_simulation import datetime_to_ole_date

    template_xml = PLEXOSConfig.get_config_path().joinpath(DEFAULT_XML_TEMPLATE)
    db: PlexosDB = PlexosDB.from_xml(template_xml)

    db.add_object(ClassEnum.Model, "Base")
    db.add_object(ClassEnum.Horizon, "Year")
    db.add_membership(ClassEnum.Model, ClassEnum.Horizon, "Base", "Year", CollectionEnum.Horizon)

    db.add_attribute(
        ClassEnum.Horizon,
        "Year",
        attribute_name="Date From",
        attribute_value=datetime_to_ole_date(datetime(2024, 1, 1)),
    )

    yield db


@pytest.fixture
def db_with_topology(db_base):
    db = db_base

    _ = db.add_object(ClassEnum.Generator, "thermal-01", category="thermal")
    _ = db.add_object(ClassEnum.Generator, "solar-01", category="solar")
    _ = db.add_object(ClassEnum.Generator, "wind-01", category="wind")
    _ = db.add_object(ClassEnum.Node, "node-01", category="region-01")
    _ = db.add_object(ClassEnum.Region, "region-01", category="iso")

    db.add_membership(ClassEnum.Generator, ClassEnum.Node, "thermal-01", "node-01", CollectionEnum.Nodes)
    db.add_membership(ClassEnum.Generator, ClassEnum.Node, "solar-01", "node-01", CollectionEnum.Nodes)
    db.add_membership(ClassEnum.Generator, ClassEnum.Node, "wind-01", "node-01", CollectionEnum.Nodes)
    db.add_membership(ClassEnum.Node, ClassEnum.Region, "node-01", "region-01", CollectionEnum.Region)

    yield db


@pytest.fixture
def db_thermal_gen(db_with_topology):
    db = db_with_topology

    db.add_property(ClassEnum.Generator, "thermal-01", "Max Capacity", 100.0, band=1)
    db.add_property(ClassEnum.Generator, "thermal-01", "Fuel Price", 5.0, band=1)
    db.add_property(ClassEnum.Generator, "thermal-01", "Heat Rate", 10.5, band=1)

    yield db


@pytest.fixture
def db_thermal_gen_multiband(db_with_topology):
    db = db_with_topology

    db.add_property(ClassEnum.Generator, "thermal-01", "Max Capacity", 100.0, band=1)
    db.add_property(ClassEnum.Generator, "thermal-01", "Fuel Price", 5.0, band=1)
    db.add_property(ClassEnum.Generator, "thermal-01", "Heat Rate", 10.5, band=1)
    db.add_property(ClassEnum.Generator, "thermal-01", "Heat Rate", 11.5, band=2)
    db.add_property(ClassEnum.Generator, "thermal-01", "Heat Rate", 12.5, band=3)
    db.add_property(ClassEnum.Generator, "thermal-01", "Start Cost", 1000, band=1)

    yield db


@pytest.fixture
def db_solar_gen_with_profile(db_with_topology, datetime_single_component_data):
    db = db_with_topology

    start_date = date(2024, 1, 1)
    profile_path = datetime_single_component_data(
        "solar-01", start_date=start_date, days=365, profile=NORMALIZED_SOLAR_PROFILE
    )

    db.add_property(ClassEnum.Generator, "solar-01", "Max Capacity", 50.0, band=1)
    db.add_property(
        ClassEnum.Generator, "solar-01", "Rating", 0.0, band=1, text={ClassEnum.DataFile: str(profile_path)}
    )

    yield db


@pytest.fixture
def db_wind_gen_with_profile(db_with_topology, datetime_single_component_data):
    db = db_with_topology

    start_date = date(2024, 1, 1)
    profile_path = datetime_single_component_data(
        "wind-01", start_date=start_date, days=365, profile=NORMALIZED_WIND_PROFILE
    )

    db.add_property(ClassEnum.Generator, "wind-01", "Max Capacity", 75.0, band=1)
    db.add_property(
        ClassEnum.Generator, "wind-01", "Rating", 0.0, band=1, text={ClassEnum.DataFile: str(profile_path)}
    )

    yield db


@pytest.fixture
def db_all_gen_types(db_with_topology, datetime_single_component_data):
    db = db_with_topology
    start_date = date(2024, 1, 1)

    db.add_property(ClassEnum.Generator, "thermal-01", "Max Capacity", 100.0, band=1)
    db.add_property(ClassEnum.Generator, "thermal-01", "Fuel Price", 5.0, band=1)
    db.add_property(ClassEnum.Generator, "thermal-01", "Heat Rate", 10.5, band=1)
    db.add_property(ClassEnum.Generator, "thermal-01", "Heat Rate", 11.5, band=2)
    db.add_property(ClassEnum.Generator, "thermal-01", "Heat Rate", 12.5, band=3)

    solar_profile_path = datetime_single_component_data(
        "solar-01", start_date=start_date, days=365, profile=NORMALIZED_SOLAR_PROFILE
    )
    db.add_property(ClassEnum.Generator, "solar-01", "Max Capacity", 50.0, band=1)
    db.add_property(
        ClassEnum.Generator,
        "solar-01",
        "Rating",
        0.0,
        band=1,
        text={ClassEnum.DataFile: str(solar_profile_path)},
    )

    wind_profile_path = datetime_single_component_data(
        "wind-01", start_date=start_date, days=365, profile=NORMALIZED_WIND_PROFILE
    )
    db.add_property(ClassEnum.Generator, "wind-01", "Max Capacity", 75.0, band=1)
    db.add_property(
        ClassEnum.Generator,
        "wind-01",
        "Rating",
        0.0,
        band=1,
        text={ClassEnum.DataFile: str(wind_profile_path)},
    )

    yield db


@pytest.fixture
def db_with_scenarios(db_thermal_gen):
    db = db_thermal_gen

    db.add_object(ClassEnum.Scenario, "Base")
    scenario_2 = db.add_object(ClassEnum.Scenario, "High")

    db.add_property(ClassEnum.Generator, "thermal-01", "Max Capacity", 150.0, band=1, scenario_id=scenario_2)
    db.add_property(ClassEnum.Generator, "thermal-01", "Fuel Price", 7.5, band=1, scenario_id=scenario_2)

    yield db


@pytest.fixture
def db_with_variable_monthly(db_base, monthly_component_data):
    db = db_base
    from .profiles import MONTHLY_CAPACITY_FACTORS

    monthly_data = {"TestGen": MONTHLY_CAPACITY_FACTORS}
    datafile_path = monthly_component_data(monthly_data)

    datafile_name = "Ratings"
    datafile_id = db.add_object(ClassEnum.DataFile, datafile_name)
    db.add_property(
        ClassEnum.DataFile, datafile_name, "Filename", value=0, text={ClassEnum.DataFile: str(datafile_path)}
    )

    variable_name = "RatingMultiplier"
    variable_id = db.add_object(ClassEnum.Variable, variable_name)
    variable_prop_id = db.add_property(ClassEnum.Variable, variable_name, "Profile", value=1)
    db._db.execute(
        "INSERT INTO t_tag(object_id,data_id,action_id) VALUES (?,?,?)", (datafile_id, variable_prop_id, 2)
    )
    db._db.execute("INSERT INTO t_band(band_id,data_id) VALUES (?,?)", (1, variable_prop_id))

    db.add_object(ClassEnum.Generator, "TestGen", collection_enum=CollectionEnum.Generators)
    gen_rating_id = db.add_property(
        ClassEnum.Generator,
        "TestGen",
        "Rating",
        value=0.0,
        text={ClassEnum.DataFile: datafile_name},
        collection_enum=CollectionEnum.Generators,
    )
    db._db.execute("INSERT INTO t_band(band_id,data_id) VALUES (?,?)", (1, gen_rating_id))
    db._db.execute(
        "INSERT INTO t_tag(object_id,data_id, action_id) VALUES (?,?,?)", (datafile_id, gen_rating_id, 1)
    )
    db._db.execute(
        "INSERT INTO t_tag(object_id,data_id, action_id) VALUES (?,?,?)", (variable_id, gen_rating_id, 1)
    )

    yield db


@pytest.fixture
def db_with_reserve_collection_property(db_with_topology, datetime_single_component_data):
    db = db_with_topology

    start_date = date(2024, 1, 1)
    lolp_profile_path = datetime_single_component_data(
        "region-01", start_date=start_date, days=366, profile=[1.5, 2.0, 2.5, 3.0, 3.5, 4.0] * 4
    )

    db.add_object(ClassEnum.Reserve, "TestReserve")
    db.add_membership(
        parent_class_enum=ClassEnum.Reserve,
        child_class_enum=ClassEnum.Region,
        parent_object_name="TestReserve",
        child_object_name="region-01",
        collection_enum=CollectionEnum.Regions,
    )

    db.add_property(
        ClassEnum.Region,
        "region-01",
        "Load Risk",
        6.0,
        collection_enum=CollectionEnum.Regions,
        parent_class_enum=ClassEnum.Reserve,
        parent_object_name="TestReserve",
    )

    db.add_property(
        ClassEnum.Region,
        "region-01",
        "LOLP Target",
        0.0,
        text={ClassEnum.DataFile: str(lolp_profile_path)},
        collection_enum=CollectionEnum.Regions,
        parent_class_enum=ClassEnum.Reserve,
        parent_object_name="TestReserve",
    )

    yield db


@pytest.fixture
def db_with_multiband_variable(db_base, multi_year_data_file):
    db = db_base

    scenarios = ["scenario_1", "scenario_2", "scenario_3"]

    for idx, scenario in enumerate(scenarios):
        db.add_object(ClassEnum.Scenario, scenario)
        priority = idx + 1
        db.add_attribute(ClassEnum.Scenario, scenario, attribute_name="Read Order", attribute_value=priority)

    datafile_name = "LoadProfiles"
    datafile_id = db.add_object(ClassEnum.DataFile, datafile_name)

    for idx, scenario in enumerate(scenarios):
        band = idx + 1
        db.add_property(
            ClassEnum.DataFile,
            datafile_name,
            "Filename",
            value=0.0,
            text={ClassEnum.DataFile: multi_year_data_file[idx % len(multi_year_data_file)]},
            band=band,
            scenario=scenario,
        )

    db.add_membership(ClassEnum.Model, ClassEnum.Scenario, "Base", "scenario_2", CollectionEnum.Scenarios)

    variable_name = "LoadProfiles"
    variable_id = db.add_object(ClassEnum.Variable, variable_name)
    for idx, scenario in enumerate(scenarios):
        band = idx + 1
        variable_prop_id = db.add_property(
            ClassEnum.Variable,
            variable_name,
            "Profile",
            value=0.0,
            band=band,
            scenario=scenario,
            text={ClassEnum.DataFile: multi_year_data_file[idx % len(multi_year_data_file)]},
        )
        db._db.execute(
            "INSERT INTO t_tag(object_id,data_id,action_id) VALUES (?,?,?)",
            (datafile_id, variable_prop_id, 2),
        )

    regions = ["r1", "r2"]
    db.add_objects(ClassEnum.Region, regions)
    for region in regions:
        region_prop_id = db.add_property(ClassEnum.Region, region, "Load", 0.0, band=1)
        db._db.execute(
            "INSERT INTO t_tag(object_id,data_id, action_id) VALUES (?,?,?)", (variable_id, region_prop_id, 1)
        )

    yield db
