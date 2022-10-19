from typing import Generator


def find_missing_intervals(
    start: int,
    stop: int,
    interval: int,
    arr: list[int]
) -> list[(int, int | None)]:
    if stop - start <= 0:
        raise ValueError("`stop` must be greater than `0`")

    arr = sorted(arr)
    ret = []

    missing_start = None
    idx_arr = 0

    for item in range(start, stop + interval, interval):
        if idx_arr >= len(arr):
            ret.append((item, None))
            break

        cur_arr = arr[idx_arr]

        if cur_arr != item:
            if missing_start is None:
                missing_start = item
            continue

        idx_arr += 1

        if missing_start is None:
            continue

        ret.append((missing_start, item))
        missing_start = None

    return ret


def split_chunks(*arr: list, chunk_size: int) -> Generator[list, None, None]:
    lens = min(len(sub_arr) for sub_arr in arr)

    for i in range(0, lens, chunk_size):
        if len(arr) > 1:
            yield [sub_arr[i:i + chunk_size] for sub_arr in arr]
        else:
            yield arr[0][i:i + chunk_size]
