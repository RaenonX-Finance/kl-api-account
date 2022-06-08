from pandas import DataFrame

from .fx import support_resistance_window
from .model import SRLevelsData


def calc_support_resistance_levels(df: DataFrame) -> SRLevelsData:
    return SRLevelsData(
        levels=support_resistance_window(df, 10),
        min_gap=20
    )
