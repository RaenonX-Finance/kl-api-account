from typing import cast

import numpy as np


def avg(val: list[float]) -> float:
    if not val:
        return 0

    return cast(float, np.mean(val))
