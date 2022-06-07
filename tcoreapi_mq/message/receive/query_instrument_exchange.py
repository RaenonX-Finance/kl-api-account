"""
Sample message of query instrument for exchange:
{
    ...
    'TC.F.CME': {
        'Duration': 'ROD;',
        'EXG.SIM': 'CME',
        'EXG.TC3': 'CME',
        'EXGName.CHS': '芝加哥商业交易所',
        'EXGName.CHT': '芝加哥商業交易所',
        'HideOrderType': '1',
        'OpenCloseTime': '22:00~21:15',
        'OrderType': 'MARKET;LIMIT;STOP;STPLMT;',
        'OrderTypeMX': 'LIMIT:ROD;MARKET:ROD;STOP:ROD;STPLMT:ROD;',
        'OrderTypeMX.TC': 'LIMIT:ROD,IOC,FOK,GTC;MARKET:ROD,IOC,FOK,GTC;STOP:ROD,IOC,FOK,GTC;STPLMT:ROD,IOC,FOK,GTC;',
        'Position': 'A;',
        'ResetTime': '21:57',
        'Symbol.SS2': 'I.F.CME',
        'TimeZone': 'Asia/Shanghai'
    }
}
"""
from dataclasses import InitVar, dataclass, field


@dataclass(kw_only=True)
class QueryInstrumentExchange:
    body: InitVar[dict[str, str]]

    code: str = field(init=False)

    def __post_init__(self, body: dict[str, str]):
        self.code = body["EXG.SIM"]
