from pandas import DataFrame

from kl_site_common.utils import print_warning
from .fx import sr_levels_range_of_pair, sr_levels_range_of_pair_merged
from .model import SRLevelsData


def calc_support_resistance_levels(df_1k: DataFrame, symbol: str) -> SRLevelsData:
    if not len(df_1k):
        print_warning(f"Attempt to calculate SR levels on empty dataframe for {symbol}", force=True)
        return SRLevelsData(groups=[], basic=[])

    return SRLevelsData(
        groups=sr_levels_range_of_pair(df_1k, symbol),
        basic=sr_levels_range_of_pair_merged(df_1k, symbol)
    )
