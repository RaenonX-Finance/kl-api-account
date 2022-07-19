import json
import zlib
from typing import Any

from pandas import DataFrame

from kl_site_common.utils import JSONEncoder
from kl_site_server.const import fast_api_socket


def df_rows_to_list_of_data(df: DataFrame, columns: dict[str, str]) -> list[dict[str, Any]]:
    return df.rename(columns=columns)[columns.values()].to_dict("records")


def dump_and_compress(data: Any) -> bytes:
    return zlib.compress(json.dumps(data, cls=JSONEncoder).encode("utf-8"), 1)


async def socket_send_to_session(event: str, data: str | bytes, session_id: str):
    await fast_api_socket.emit(event, data, to=session_id)


async def socket_send_to_all(event: str, data: str | bytes):
    await fast_api_socket.emit(event, data)
