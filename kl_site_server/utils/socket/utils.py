import json
import zlib
from typing import Any, TYPE_CHECKING

from kl_site_common.utils import JSONEncoder
if TYPE_CHECKING:
    from kl_site_server.model import BarDataDict


def data_rename_col(data: list["BarDataDict"], columns: dict[str, str]) -> list[dict[str, int | float]]:
    return [
        {columns[k]: v for k, v in elem.items() if k in columns}
        for elem in data
    ]


def dump_and_compress(data: Any) -> bytes:
    return zlib.compress(json.dumps(data, cls=JSONEncoder).encode("utf-8"), 1)
