from .array import find_missing_intervals


def test_find_missing_intervals():
    assert find_missing_intervals(1, 10, 1, []) == [(1, None)]
    assert find_missing_intervals(1, 10, 1, [1, 2, 3]) == [(4, None)]
    assert find_missing_intervals(1, 10, 1, [3, 4, 5]) == [(1, 3), (6, None)]
    assert find_missing_intervals(1, 10, 1, [6, 8, 9]) == [(1, 6), (7, 8), (10, None)]
    assert find_missing_intervals(1, 10, 1, list(range(1, 11))) == []
