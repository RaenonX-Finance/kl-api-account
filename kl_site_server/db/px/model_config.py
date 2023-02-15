from pydantic import BaseModel, Field

from kl_site_common.db import PyObjectId


class EmaPairConfigModel(BaseModel):
    fast: int = Field(..., description="Faster period in an EMA pair.")
    slow: int = Field(..., description="Slower period in an EMA pair.")

    @property
    def ema_periods(self) -> set[int]:
        return {self.fast, self.slow}


class CandleDirectionConfigModel(EmaPairConfigModel):
    signal: int = Field(..., description="Signal period for calculating MACD used in candlestick direction.")

    @property
    def ema_periods(self) -> set[int]:
        return super().ema_periods | {self.signal}


class SrLevelConfigModel(BaseModel):
    min_diff: int = Field(..., description="Minimum level difference to show SR level.")


class PeriodEntryConfig(BaseModel):
    period_min: int = Field(..., description="Period to calculate in minutes.", alias="period")
    name: str = Field(..., description="Period name to display at the frontend.")


class PxConfigModel(BaseModel):
    id: PyObjectId | None = Field(default_factory=PyObjectId, alias="_id")
    ema_net: EmaPairConfigModel = Field(..., description="Periods to use for calculating EMA net.")
    candle_dir: CandleDirectionConfigModel = Field(
        ...,
        description="Periods to use for calculating candlestick direction."
    )
    ema_strong_sr: list[EmaPairConfigModel] = Field(..., description="Periods to use for calculating EMA SR levels.")
    sr_level: SrLevelConfigModel = Field(..., description="Fixed SR levels calculation config.")
    periods: list[PeriodEntryConfig] = Field(..., description="Data periods to calculate.", alias="period")

    period_mins: list[PeriodEntryConfig] = Field(
        None,  # Just give `None` because the value will be initiated in `__init__`
        description="Period config from `periods` that uses minute data."
    )
    period_days: list[PeriodEntryConfig] = Field(
        None,  # Just give `None` because the value will be initiated in `__init__`
        description="Period config from `periods` that uses daily data."
    )
    ema_periods: set[int] = Field(
        None,
        description="All EMA periods in-use."
    )

    def __init__(self, **data):
        super().__init__(**data)

        self.period_mins = [period for period in self.periods if period.period_min < 1440]
        self.period_days = [period for period in self.periods if period.period_min >= 1440]
        self.ema_periods = self.ema_net.ema_periods | {
            ema_period for ema_strong_sr_config in self.ema_strong_sr
            for ema_period in ema_strong_sr_config.ema_periods
        }
