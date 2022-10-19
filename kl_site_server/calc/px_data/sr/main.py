from tcoreapi_mq.model import FUTURES_SECURITY_TO_SYM_OBJ
from .const import SR_LEVEL_KEY_TIMES
from .fx import get_sr_level_pairs, get_sr_level_pairs_merged
from .model import SRLevelsData


def calc_support_resistance_levels(security: str, current_px: float) -> SRLevelsData:
    symbol_complete = FUTURES_SECURITY_TO_SYM_OBJ[security].symbol_complete
    sr_level_key_times = SR_LEVEL_KEY_TIMES[security]

    return SRLevelsData(
        groups=get_sr_level_pairs(symbol_complete, current_px, sr_level_key_times.group),
        basic=get_sr_level_pairs_merged(symbol_complete, current_px, sr_level_key_times.basic)
    )
