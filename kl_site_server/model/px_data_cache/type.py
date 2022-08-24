from datetime import datetime
from typing import Callable, TypeAlias

from tcoreapi_mq.message import HistoryInterval
from tcoreapi_mq.model import SymbolBaseType

HistoryDataFetcherCallable: TypeAlias = Callable[[SymbolBaseType, HistoryInterval, datetime, datetime], None]
