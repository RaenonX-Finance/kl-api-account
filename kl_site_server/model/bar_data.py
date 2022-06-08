from kl_site_server.enums import PxDataCol
from tcoreapi_mq.message import PxHistoryDataEntry

BarDataDict = dict[PxDataCol, float | int]


def to_bar_data_dict_tcoreapi(data: PxHistoryDataEntry, period_sec: int) -> BarDataDict:
    return {
        PxDataCol.OPEN: data.open,
        PxDataCol.HIGH: data.high,
        PxDataCol.LOW: data.low,
        PxDataCol.CLOSE: data.close,
        PxDataCol.EPOCH_SEC: data.epoch_sec // period_sec * period_sec,
        PxDataCol.VOLUME: data.volume,
    }


def to_bar_data_dict_merged(bars: list[BarDataDict]) -> BarDataDict:
    return {
        PxDataCol.OPEN: min(bars, key=lambda item: item[PxDataCol.EPOCH_SEC])[PxDataCol.OPEN],
        PxDataCol.HIGH: max(bars, key=lambda item: item[PxDataCol.HIGH])[PxDataCol.HIGH],
        PxDataCol.LOW: min(bars, key=lambda item: item[PxDataCol.LOW])[PxDataCol.LOW],
        PxDataCol.CLOSE: max(bars, key=lambda item: item[PxDataCol.EPOCH_SEC])[PxDataCol.CLOSE],
        PxDataCol.EPOCH_SEC: min(bars, key=lambda item: item[PxDataCol.EPOCH_SEC])[PxDataCol.EPOCH_SEC],
        PxDataCol.VOLUME: sum(bar[PxDataCol.VOLUME] for bar in bars),
    }
