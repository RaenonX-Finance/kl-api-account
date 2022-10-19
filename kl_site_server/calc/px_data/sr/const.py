from kl_site_common.const import DATA_SOURCES
from .model import SrLevelKeyTimes

SR_LEVEL_KEY_TIMES: dict[str, SrLevelKeyTimes] = {
    data_source["symbol"]: SrLevelKeyTimes.from_config_obj(data_source["sr-level"])
    for data_source in DATA_SOURCES
}
