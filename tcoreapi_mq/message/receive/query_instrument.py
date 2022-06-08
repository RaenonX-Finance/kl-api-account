"""
Sample message for instrument query result:
{
    'Reply': 'QUERYINSTRUMENTINFO',
    'Success': 'OK',
    'Info': {
        'TC.F.CME': (Check the sample message body of :class:`QueryInstrumentExchange`),
        'TC.F.CME.NQ': (Check the sample message body of :class:`QueryInstrumentProduct`)
    }
}
"""
import json
from dataclasses import InitVar, dataclass, field
from typing import TYPE_CHECKING

from .query_instrument_exchange import QueryInstrumentExchange
from .query_instrument_product import QueryInstrumentProduct

if TYPE_CHECKING:
    from tcoreapi_mq.model import SymbolBaseType


@dataclass(kw_only=True)
class QueryInstrumentMessage:
    message: InitVar[str]

    symbol_obj: "SymbolBaseType"

    success: bool = field(init=False)
    info_exchange: QueryInstrumentExchange = field(init=False)
    info_product: QueryInstrumentProduct = field(init=False)

    def __post_init__(self, message: str):
        body = json.loads(message)

        self.success = body["Success"] == "OK"
        self.info_exchange = QueryInstrumentExchange(body=next(
            info_val for info_key, info_val in body["Info"].items() if info_key.count(".") == 2
        ))
        self.info_product = QueryInstrumentProduct(
            symbol_obj=self.symbol_obj,
            body=next(
                info_val for info_key, info_val in body["Info"].items() if info_key.count(".") == 3
            )
        )
