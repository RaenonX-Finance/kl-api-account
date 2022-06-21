"""
Sample realtime data return:
{
  'DataType': 'REALTIME',
  'Quote': {
    'Symbol': 'TC.F.TWF.FITX.HOT',
    'Exchange': 'TWF',
    'ExchangeName': '台灣期交所',
    'Security': 'FITX',
    'SecurityName': '臺指',
    'SecurityType': '6',
    'TradeQuantity': '1',
    'FilledTime': '1912',
    'TradeDate': '20220608',
    'FlagOfBuySell': '2',
    'OpenTime': '84500',
    'CloseTime': '50000',
    'BidVolume': '1',
    'BidVolume1': '1',
    'BidVolume2': '10',
    'BidVolume3': '7',
    'BidVolume4': '13',
    'BidVolume5': '0',
    'BidVolume6': '0',
    'BidVolume7': '0',
    'BidVolume8': '0',
    'BidVolume9': '0',
    'AskVolume': '1',
    'AskVolume1': '2',
    'AskVolume2': '3',
    'AskVolume3': '1',
    'AskVolume4': '7',
    'AskVolume5': '0',
    'AskVolume6': '0',
    'AskVolume7': '0',
    'AskVolume8': '0',
    'AskVolume9': '0',
    'TotalBidCount': '32975',
    'TotalBidVolume': '58894',
    'TotalAskCount': '32451',
    'TotalAskVolume': '57707',
    'BidSize': '33428',
    'AskSize': '34772',
    'FirstDerivedBidVolume': '0',
    'FirstDerivedAskVolume': '0',
    'BuyCount': '32975',
    'SellCount': '32451',
    'EndDate': '20220615',
    'BestBidVolume': '1',
    'BestAskVolume': '1',
    'BeginDate': '20210617',
    'YTradeDate': '0',
    'ExpiryDate': '8',
    'TradingPrice': '16557',
    'Change': '96',
    'TradeVolume': '54363',
    'OpeningPrice': '16465',
    'HighPrice': '16586',
    'LowPrice': '16433',
    'ClosingPrice': '16557',
    'ReferencePrice': '16461',
    'UpperLimitPrice': '18107',
    'LowerLimitPrice': '14815',
    'YClosedPrice': '16570',
    'YTradeVolume': '54302',
    'Bid': '16554',
    'Bid1': '16553',
    'Bid2': '16552',
    'Bid3': '16551',
    'Bid4': '16550',
    'Bid5': '',
    'Bid6': '',
    'Bid7': '',
    'Bid8': '',
    'Bid9': '',
    'Ask': '16558',
    'Ask1': '16559',
    'Ask2': '16560',
    'Ask3': '16561',
    'Ask4': '16562',
    'Ask5': '',
    'Ask6': '',
    'Ask7': '',
    'Ask8': '',
    'Ask9': '',
    'PreciseTime': '1912730000',
    'FirstDerivedBid': '',
    'FirstDerivedAsk': '',
    'SettlementPrice': '16461',
    'BestBid': '16554',
    'BestAsk': '16558',
    'Deposit': '',
    'TickSize': '',
    'TradeStatus': '',
    'OpenInterest': '86546'
  }
}
"""
from ._base import SubscriptionDataBase
from .common import CommonData


class RealtimeData(SubscriptionDataBase):
    def __init__(self, data: CommonData):
        super().__init__(data)

        self.quote: dict[str, str] = self.data.body["Quote"]
        # Trade Qty could be 0, no real trade occur in this case (possibly B/A updated?)
        self.is_valid: bool = bool(int(self.quote["TradeQuantity"]))

    @property
    def symbol_complete(self) -> str:
        return self.quote["Symbol"]

    @property
    def security(self) -> str:
        return self.quote["Security"]

    @property
    def security_name(self) -> str:
        return self.quote["SecurityName"]

    @property
    def exchange(self) -> str:
        return f"{self.quote['Exchange']} ({self.quote['ExchangeName']})"

    @property
    def last_px(self) -> float:
        return float(self.quote["TradingPrice"])

    @property
    def open(self) -> float:
        # `OpeningPrice` could be `0` for some reason - might because of holiday
        return float(self.quote["OpeningPrice"]) or float(self.quote["ReferencePrice"])

    @property
    def high(self) -> float:
        return float(self.quote["HighPrice"])

    @property
    def low(self) -> float:
        return float(self.quote["LowPrice"])

    @property
    def close(self) -> float:
        return float(self.quote["ClosingPrice"] or self.last_px)

    @property
    def change_val(self) -> float:
        return float(self.quote["Change"])

    @property
    def change_pct(self) -> float:
        return self.change_val / self.open * 100
