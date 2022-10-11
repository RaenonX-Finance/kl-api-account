import numpy as np
from pandas import DataFrame

from kl_site_common.utils import df_fill_na_with_none


def test_df_fill_na_with_none():
    @df_fill_na_with_none
    def returns_df_with_nan():
        return DataFrame({"A": [np.nan, np.nan], "B": [7, np.nan]})

    assert returns_df_with_nan().to_dict("records") == [{"A": None, "B": 7}, {"A": None, "B": None}]
