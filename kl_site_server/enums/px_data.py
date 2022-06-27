class PxDataCol:
    OPEN = "open"
    HIGH = "high"
    LOW = "low"
    CLOSE = "close"
    EPOCH_SEC = "epoch_sec"
    VOLUME = "volume"

    DIFF = "diff"

    DATE = "date"
    DATE_MARKET = "market_date"

    EPOCH_SEC_ORIGINAL = "epoch_sec_original"

    @staticmethod
    def get_sma_col_name(period: int) -> str:
        return f"sma_{period}"
