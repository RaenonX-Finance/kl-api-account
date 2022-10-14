from pandas import DataFrame

from kl_site_server.calc.px_data_fields.tie_point import calc_tie_point_partial
from kl_site_server.enums import PxDataCol


def test_calc_tie_point_partial_same_date():
    df_new = DataFrame({
        PxDataCol.CLOSE: [1, 2, 3, 4, 8, 11],
        PxDataCol.DATE_MARKET: [1, 1, 1, 1, 1, 1],
    })
    df_cached = DataFrame({
        PxDataCol.CLOSE: [1, 2, 3, 4, 7],
        PxDataCol.DATE_MARKET: [1, 1, 1, 1, 1],
        PxDataCol.MARKET_DATE_HIGH: [7, 7, 7, 7, 7],
        PxDataCol.MARKET_DATE_LOW: [1, 1, 1, 1, 1],
        PxDataCol.TIE_POINT: [4, 4, 4, 4, 4],
    })

    df_calc = calc_tie_point_partial(df_new, df_cached, -2, 60)

    assert df_calc[PxDataCol.CLOSE].to_list() == [1, 2, 3, 4, 8, 11]
    assert df_calc[PxDataCol.MARKET_DATE_HIGH].to_list() == [7, 7, 7, 7, 8, 11]
    assert df_calc[PxDataCol.MARKET_DATE_LOW].to_list() == [1, 1, 1, 1, 1, 1]
    assert df_calc[PxDataCol.TIE_POINT].to_list() == [4, 4, 4, 4, 4.5, 6]


def test_calc_tie_point_partial_cross_date():
    df_new = DataFrame({
        PxDataCol.CLOSE: [1, 2, 3, 4, 8, 11],
        PxDataCol.DATE_MARKET: [1, 1, 1, 1, 2, 2],
    })
    df_cached = DataFrame({
        PxDataCol.CLOSE: [1, 2, 3, 4, 7],
        PxDataCol.DATE_MARKET: [1, 1, 1, 1, 2],
        PxDataCol.MARKET_DATE_HIGH: [7, 7, 7, 7, 7],
        PxDataCol.MARKET_DATE_LOW: [1, 1, 1, 1, 1],
        PxDataCol.TIE_POINT: [4, 4, 4, 4, 4],
    })

    df_calc = calc_tie_point_partial(df_new, df_cached, -2, 60)

    assert df_calc[PxDataCol.CLOSE].to_list() == [1, 2, 3, 4, 8, 11]
    assert df_calc[PxDataCol.MARKET_DATE_HIGH].to_list() == [7, 7, 7, 7, 8, 11]
    assert df_calc[PxDataCol.MARKET_DATE_LOW].to_list() == [1, 1, 1, 1, 8, 8]
    assert df_calc[PxDataCol.TIE_POINT].to_list() == [4, 4, 4, 4, 8, 9.5]
