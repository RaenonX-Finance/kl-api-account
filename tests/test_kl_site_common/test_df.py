import numpy as np
from pandas import DataFrame

from kl_site_common.utils import (
    df_fill_na_with_none, df_get_last_non_nan_rev_index,
    df_get_last_rev_index_of_matching_val,
)


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


def test_df_get_last_non_nan_rev_index_all_nan():
    df = DataFrame({"A": [np.nan, np.nan, np.nan, np.nan]})

    assert df_get_last_non_nan_rev_index(df, ["A"]) is None


def test_df_get_last_rev_index_of_matching_val():
    df_base = DataFrame({"A": [1, 6, 3]})
    df_comp = DataFrame({"A": [1, 6, 4, 5]})

    assert df_get_last_rev_index_of_matching_val(df_base, df_comp, "A") == -2

    df_base = DataFrame({"A": [1, 6, 3]})
    df_comp = DataFrame({"A": [1, 6, 3, 4]})

    assert df_get_last_rev_index_of_matching_val(df_base, df_comp, "A") == -1

    df_base = DataFrame({"A": [1, 6, 3, 4]})
    df_comp = DataFrame({"A": [1, 6, 3]})

    assert df_get_last_rev_index_of_matching_val(df_base, df_comp, "A") == -2

    df_base = DataFrame({"A": [1, 2, 3, 4]}, index=["A", "B", "C", "D"])
    df_comp = DataFrame({"A": [5, 6, 7]}, index=["B", "C", "D"])

    assert df_get_last_rev_index_of_matching_val(df_base, df_comp, "A") is None
