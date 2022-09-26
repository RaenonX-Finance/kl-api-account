import numpy as np
import pytest
from pandas import DataFrame

from kl_site_server.calc.px_data_fields.ema import calc_ema_full, calc_ema_last
from kl_site_server.enums import PxDataCol


def test_calc_ema_full():
    ema_periods = {5, 10}

    df = DataFrame({PxDataCol.CLOSE: [5, 7, 5, 6, 4, 3, 8, 9, 6, 7, 8, 1]})
    df = calc_ema_full(df, ema_periods)

    expected_dict = {
        PxDataCol.get_ema_col_name(5): [
            np.nan, np.nan, np.nan, np.nan, 5.4,
            4.6, 5.733, 6.822, 6.548, 6.699,
            7.133, 5.088,
        ],
        PxDataCol.get_ema_col_name(10): [
            np.nan, np.nan, np.nan, np.nan, np.nan,
            np.nan, np.nan, np.nan, np.nan, 6,
            6.364, 5.388
        ]
    }

    for ema_period in ema_periods:
        col_name = PxDataCol.get_ema_col_name(ema_period)

        assert df[col_name].values == pytest.approx(expected_dict[col_name], rel=1E-4, nan_ok=True), col_name


def test_calc_ema_last():
    ema_periods = {5, 10}

    df = DataFrame({
        PxDataCol.CLOSE: [3, 3, 3, 3, 3],
        PxDataCol.get_ema_col_name(5): [1, 1, 1, 8, 1],
        PxDataCol.get_ema_col_name(10): [1, 1, 1, 8, 1],
    })
    df = calc_ema_last(df, ema_periods)

    expected_dict = {
        PxDataCol.get_ema_col_name(5): [1, 1, 1, 8, 6.333],
        PxDataCol.get_ema_col_name(10): [1, 1, 1, 8, 7.091]
    }

    for ema_period in ema_periods:
        col_name = PxDataCol.get_ema_col_name(ema_period)

        assert df[col_name].values == pytest.approx(expected_dict[col_name], rel=1E-4, nan_ok=True), col_name
