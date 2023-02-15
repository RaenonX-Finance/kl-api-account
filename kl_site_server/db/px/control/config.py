from ..const import px_config_col
from ..model_config import PxConfigModel


def _get_config_model() -> PxConfigModel:
    return PxConfigModel(**px_config_col.find_one())


PX_CONFIG = _get_config_model()
