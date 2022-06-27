from kl_site_server.enums import PxDataCol
from tcoreapi_mq.message import PxHistoryDataEntry

BarDataDict = dict[PxDataCol, float | int]


def to_bar_data_dict_tcoreapi(data: PxHistoryDataEntry) -> BarDataDict:
    return {
        PxDataCol.OPEN: data.open,
        PxDataCol.HIGH: data.high,
        PxDataCol.LOW: data.low,
        PxDataCol.CLOSE: data.close,
        PxDataCol.EPOCH_SEC: data.epoch_sec // 60 * 60,
        PxDataCol.EPOCH_SEC_ORIGINAL: data.epoch_sec,
        PxDataCol.VOLUME: data.volume,
    }
