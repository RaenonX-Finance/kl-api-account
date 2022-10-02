from typing import Any, Callable

import numpy as np
from pandas import DataFrame


def df_fill_na_with_none(func: Callable[[Any], DataFrame]):
    def wrapper(*args, **kwargs):
        df = func(*args, **kwargs)
        df = df.fillna(np.nan).replace(np.nan, None)

        return df

    return wrapper
