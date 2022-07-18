from datetime import datetime

import pymongo

from kl_site_common.utils import print_log
from tcoreapi_mq.message import HistoryData, HistoryInterval, PxHistoryDataEntry
from .const import px_data_col
from .model import DbHistoryDataResult


def get_history_data_from_db(
    symbol_complete: str,
    interval: HistoryInterval,
    start: datetime,
    end: datetime,
) -> DbHistoryDataResult:
    print_log(
        f"[DB-Px] Requesting history data of [yellow]{symbol_complete}[/yellow] at [yellow]{interval}[/yellow] "
        f"starting from {start} to {end}"
    )

    def get_find_cursor():
        return px_data_col.find({
            "s": symbol_complete,
            "i": interval,
            "ts": {
                "$gte": start,
                "$lte": end,
            }
        })

    try:
        return DbHistoryDataResult(
            earliest=get_find_cursor().sort("ts", pymongo.ASCENDING).limit(1).next()["ts"],
            latest=get_find_cursor().sort("ts", pymongo.DESCENDING).limit(1).next()["ts"],
            data=(PxHistoryDataEntry.from_mongo_doc(data) for data in get_find_cursor()),
        )
    except StopIteration:
        return DbHistoryDataResult(earliest=None, latest=None, data=[])


def store_history_to_db(data: HistoryData):
    print_log(
        f"[DB-Px] Storing history data of [yellow]{data.symbol_complete}[/yellow] "
        f"at [yellow]{data.data_type}[/yellow]"
    )
    px_data_col.delete_many({
        "$or": [
            {"ts": entry["ts"], "s": entry["s"], "i": entry["i"]}
            for entry in data.to_db_entries()
        ]
    })
    # `ordered=False` to ignore duplicated data
    px_data_col.insert_many(data.to_db_entries(), ordered=False)
