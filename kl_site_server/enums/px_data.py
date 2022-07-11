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

    EPOCH_SEC_TIME = "epoch_sec_time"

    # Common indicators

    @staticmethod
    def get_sma_col_name(period: int) -> str:
        return f"sma_{period}"

    @staticmethod
    def get_ema_col_name(period: int) -> str:
        return f"ema_{period}"

    @staticmethod
    def get_current_avg_col_name(period: int) -> str:
        return f"avg_cur_{period}"

    # KL indicators

    STRENGTH = "strength"
    CANDLESTICK_DIR = "candlestick_dir"
    TIE_POINT = "tie_point"

    AUTO_SR_GROUP_BASIS = "auto_sr_group_basis"
