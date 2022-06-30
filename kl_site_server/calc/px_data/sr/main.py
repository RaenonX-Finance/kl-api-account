from pandas import DataFrame

from .fx import support_resistance_range_of_2_close
from .model import SRLevelsData


def calc_support_resistance_levels(df_1k: DataFrame, symbol: str) -> SRLevelsData:
    groups = support_resistance_range_of_2_close(df_1k, symbol)

    return SRLevelsData(groups=groups)
