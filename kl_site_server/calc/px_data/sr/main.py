from pandas import DataFrame

from .fx import sr_levels_range_of_pair, sr_levels_range_of_pair_merged
from .model import SRLevelsData


def calc_support_resistance_levels(df_1k: DataFrame, symbol: str) -> SRLevelsData:
    return SRLevelsData(
        groups=sr_levels_range_of_pair(df_1k, symbol),
        basic=sr_levels_range_of_pair_merged(df_1k, symbol)
    )
