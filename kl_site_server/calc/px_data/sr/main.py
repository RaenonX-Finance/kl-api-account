from pandas import DataFrame

from .model import SRLevelsData


def calc_support_resistance_levels(_: DataFrame) -> SRLevelsData:
    return SRLevelsData(
        levels=[],
        min_gap=20
    )
