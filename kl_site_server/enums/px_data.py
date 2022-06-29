class PxDataCol:
    # Original values

    OPEN = "open"
    HIGH = "high"
    LOW = "low"
    CLOSE = "close"
    EPOCH_SEC = "epoch_sec"
    VOLUME = "volume"

    # Derived values

    DIFF = "diff"

    DATE = "date"
    DATE_MARKET = "market_date"

    # Common indicators

    @staticmethod
    def get_sma_col_name(period: int) -> str:
        return f"sma_{period}"

    @staticmethod
    def get_current_avg_col_name(period: int) -> str:
        return f"avg_cur_{period}"

    # KL indicators

    STRENGTH = "strength"
    CANDLESTICK_DIR = "candlestick_dir"
    TIE_POINT = "tie_point"
