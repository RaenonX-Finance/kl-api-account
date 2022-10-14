import numpy as np
from pandas import DataFrame

from kl_site_common.utils import df_fill_na_with_none, df_get_last_non_nan_rev_index


def test_df_fill_na_with_none():
    @df_fill_na_with_none
    def returns_df_with_nan():
        return DataFrame({"A": [np.nan, np.nan], "B": [7, np.nan]})

    assert returns_df_with_nan().to_dict("records") == [{"A": None, "B": 7}, {"A": None, "B": None}]


def test_df_get_last_non_nan_rev_index():
    df = DataFrame(
        {
            "A": [1, 2, 3, 4, np.nan],
            "B": [101, 102, 103, np.nan, np.nan]
        },
        index=["L", "M", "N", "O", "P"]
    )

    assert df_get_last_non_nan_rev_index(df, ["A"]) == -1
    assert df_get_last_non_nan_rev_index(df, ["A", "B"]) == -2
