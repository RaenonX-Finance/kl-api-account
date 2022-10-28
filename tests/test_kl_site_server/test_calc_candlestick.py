import pytest
from pandas import DataFrame

from kl_site_server.calc.px_data_fields.candlestick import calc_candlestick_partial
from kl_site_server.enums import PxDataCol
from kl_site_server.utils import CANDLESTICK_DIR_MACD_FAST, CANDLESTICK_DIR_MACD_SLOW


def test_calc_candlestick_partial():
    df = DataFrame({
        PxDataCol.CLOSE: [8, 9, 6, 8, 9, 2],
    })
    df_ema_base = DataFrame({
        PxDataCol.CLOSE: [8, 9, 6, 7, 8, 1],
        PxDataCol.get_ema_col_name(CANDLESTICK_DIR_MACD_FAST): [5.5, 5.833, 5.849, 5.959, 6.153, 5.662],
        PxDataCol.get_ema_col_name(CANDLESTICK_DIR_MACD_SLOW): [7.7, 7.709, 7.698, 7.693, 7.695, 7.651],
        PxDataCol.MACD_SIGNAL: [6.3, 5.278, 4.387, 3.622, 2.976, 2.356],
        PxDataCol.CANDLESTICK_DIR: [-1, -1, -1, -1, -1, -1],
    })

    df_calc = calc_candlestick_partial(df, df_ema_base, -4)

    assert df_calc[PxDataCol.CANDLESTICK_DIR].to_list() == [-1, -1, -1, -1, -1, -1]
    assert df_calc[PxDataCol.MACD_SIGNAL].to_list() == pytest.approx(
        [6.3, 5.278, 4.387, 3.633, 3.007, 2.413], abs=1E-3
    )
