class PxDataCol:
    OPEN = "open"
    HIGH = "high"
    LOW = "low"
    CLOSE = "close"
    EPOCH_SEC = "epoch_sec"
    VOLUME = "volume"

    VWAP = "vwap"

    DIFF = "diff"

    DATE = "date"
    DATE_MARKET = "market_date"

    PRICE_TIMES_VOLUME = "price_times_volume"

    @staticmethod
    def get_sma_col_name(period: int) -> str:
        return f"sma_{period}"
