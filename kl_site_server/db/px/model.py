from dataclasses import dataclass
from datetime import datetime
from typing import Iterator

from tcoreapi_mq.message import PxHistoryDataEntry


@dataclass(kw_only=True)
class DbHistoryDataResult:
    data: Iterator[PxHistoryDataEntry]
    earliest: datetime | None
    latest: datetime | None
