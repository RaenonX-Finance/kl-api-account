"""
Sample message of query instrument for product:
{
    ...
    'TC.F.CME.NQ': {
        'CertSymbol.CONCORDS': 'NQ',
        'Currency': 'USD',
        'Denominator': '1',
        'Denominator.ML': '1',
        'Denominator.yuanta': '1',
        'Duration': 'ROD;',
        'EXG': 'CME',
        'EXG.CME': 'XCME',
        'EXG.CTP': 'CME',
        'EXG.DA': 'CME',
        'EXG.Entrust': 'CME',
        'EXG.GTS': 'CME',
        'EXG.IDC': 'CME',
        'EXG.ITS': 'CME',
        'EXG.ITS_KGI': 'CME',
        'EXG.ITS_KS': 'CME',
        'EXG.ITS_T': 'SIM',
        'EXG.ITS_TW': 'CME',
        'EXG.JSUNBO': 'CME',
        'EXG.ML': 'CME',
        'EXG.NEWEDGE': 'CME',
        'EXG.PATS': 'CME',
        'EXG.POEMS': 'CME',
        'EXG.RJO': 'CME',
        'EXG.SF': 'CME',
        'EXG.SIM': 'CME',
        'EXG.TC3': 'CME',
        'EXG.TWPAT': 'CME',
        'EXG.dcn': 'CME',
        'EXG.mdc': 'CME',
        'EXG.mo': 'CME',
        'EXG.yuanta': 'CME',
        'EXGName.CHS': '芝加哥商业交易所',
        'EXGName.CHT': '芝加哥商業交易所',
        'Group.CHS': '指数',
        'Group.CHT': '指數',
        'Group.ENG': 'Equities',
        'HideOrderType': '1',
        'I3_TickSize': '0.25',
        'Multiplier.CTP': '1',
        'Multiplier.DA': '1',
        'Multiplier.GQ2': '1',
        'Multiplier.GTS': '1',
        'Multiplier.ML': '1',
        'Multiplier.yuanta': '1',
        'Name.CHS': '小型纳指',
        'Name.CHT': '小那斯達',
        'Name.ENG': 'E-mini NASDAQ 100',
        'OpenCloseTime': '22:00~21:00',
        'OrderType': 'MARKET;LIMIT;STOP;STPLMT;',
        'OrderTypeMX': 'LIMIT:ROD;MARKET:ROD;STOP:ROD;STPLMT:ROD;',
        'OrderTypeMX.TC': 'LIMIT:ROD,IOC,FOK,GTC;MARKET:ROD,IOC,FOK,GTC;STOP:ROD,IOC,FOK,GTC;STPLMT:ROD,IOC,FOK,GTC;',
        'Position': 'A;',
        'ResetTime': '21:57',
        'ShowDeno': '1',
        'ShowDeno.Entrust': '1',
        'ShowDeno.concords': '1',
        'ShowDeno.dcn': '1',
        'ShowDeno.kgi': '1',
        'ShowDeno.pfcf': '1',
        'ShowDeno.tw': '1',
        'ShowDeno.wlf': '1',
        'ShowMulti': '1',
        'ShowMulti.Entrust': '1',
        'ShowMulti.concords': '1',
        'ShowMulti.dcn': '1',
        'ShowMulti.kgi': '1',
        'ShowMulti.pfcf': '1',
        'ShowMulti.tw': '1',
        'ShowMulti.wlf': '1',
        'Symbol': 'NQ',
        'Symbol.CME': 'NQ',
        'Symbol.CTP': 'NQ',
        'Symbol.DA': 'NQ',
        'Symbol.Entrust': 'NQ',
        'Symbol.GQ2': 'ICE.CME.NQ',
        'Symbol.GTS': 'NQ',
        'Symbol.IDC': 'NQ',
        'Symbol.ITS': 'NQ',
        'Symbol.ITS_KGI': 'NQ',
        'Symbol.ITS_KS': 'NQ',
        'Symbol.ITS_T': 'NQ',
        'Symbol.ITS_TW': 'NQ',
        'Symbol.JSUNBO': 'NQ',
        'Symbol.ML': 'NQ',
        'Symbol.NEWEDGE': 'MINI NSDQ',
        'Symbol.PATS': 'NQ',
        'Symbol.POEMS': 'MINI NSDQ',
        'Symbol.RJO': 'MINI NSDQ',
        'Symbol.SF': 'MINI_NSDQ',
        'Symbol.SIM': 'NQ',
        'Symbol.SS2': 'I.F.CME.NQ',
        'Symbol.TC3': 'ICE.CME.NQ',
        'Symbol.TCDATA': 'ICE.TC_CME.NQ',
        'Symbol.TWPAT': 'MINI NSDQ',
        'Symbol.dcn': 'NQ',
        'Symbol.mdc': 'NQ',
        'Symbol.mo': 'NQ',
        'Symbol.yuanta': 'NQ',
        'TickSize': '0.25',
        'TicksPerPoint': '100',
        'TimeZone': 'Asia/Shanghai',
        'Weight': '20',
        'pinyin': 'xxnz'
    }
}
"""
import math
from dataclasses import InitVar, dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tcoreapi_mq.model import SymbolBaseType


@dataclass(kw_only=True)
class QueryInstrumentProduct:
    body: InitVar[dict[str, str]]

    symbol_obj: "SymbolBaseType"

    symbol: str = field(init=False)
    tick: float = field(init=False)
    decimals: int = field(init=False)

    def __post_init__(self, body: dict[str, str]):
        self.symbol = body["Symbol"]
        self.tick = float(body["TickSize"])
        self.decimals = int(math.log10(int(body["TicksPerPoint"])))
