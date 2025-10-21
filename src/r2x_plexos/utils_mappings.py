"""MAPPING FOR CLASS ENUM."""

from plexosdb import ClassEnum

from .models import (
    PLEXOSBattery,
    PLEXOSDatafile,
    PLEXOSGenerator,
    PLEXOSHorizon,
    PLEXOSInterface,
    PLEXOSLine,
    PLEXOSModel,
    PLEXOSNode,
    PLEXOSObject,
    PLEXOSRegion,
    PLEXOSReserve,
    PLEXOSScenario,
    PLEXOSStorage,
    PLEXOSTimeslice,
    PLEXOSVariable,
    PLEXOSZone,
)

PLEXOS_TYPE_MAP: dict[ClassEnum, type[PLEXOSObject]] = {
    ClassEnum.Generator: PLEXOSGenerator,
    ClassEnum.Node: PLEXOSNode,
    ClassEnum.Storage: PLEXOSStorage,
    ClassEnum.Line: PLEXOSLine,
    ClassEnum.DataFile: PLEXOSDatafile,
    ClassEnum.Variable: PLEXOSVariable,
    ClassEnum.Scenario: PLEXOSScenario,
    ClassEnum.Battery: PLEXOSBattery,
    ClassEnum.Reserve: PLEXOSReserve,
    ClassEnum.Region: PLEXOSRegion,
    ClassEnum.Zone: PLEXOSZone,
    ClassEnum.Interface: PLEXOSInterface,
    ClassEnum.Timeslice: PLEXOSTimeslice,
    ClassEnum.Model: PLEXOSModel,
    ClassEnum.Horizon: PLEXOSHorizon,
}
PLEXOS_TYPE_MAP_INVERTED = dict(zip(PLEXOS_TYPE_MAP.values(), PLEXOS_TYPE_MAP.keys(), strict=False))
