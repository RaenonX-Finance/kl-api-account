from kl_site_common.utils.array import find_missing_intervals, split_chunks


def test_find_missing_intervals():
    assert find_missing_intervals(1, 10, 1, []) == [(1, None)]
    assert find_missing_intervals(1, 10, 1, [1, 2, 3]) == [(4, None)]
    assert find_missing_intervals(1, 10, 1, [3, 4, 5]) == [(1, 3), (6, None)]
    assert find_missing_intervals(1, 10, 1, [6, 8, 9]) == [(1, 6), (7, 8), (10, None)]
    assert find_missing_intervals(1, 10, 1, list(range(1, 11))) == []


def test_split_chunks():
    assert list(split_chunks([1, 2, 3], 1)) == [[1], [2], [3]]
    assert list(split_chunks([1, 2, 3], 2)) == [[1, 2], [3]]
    assert list(split_chunks([1, 2, 3], 3)) == [[1, 2, 3]]
    assert list(split_chunks([1, 2, 3], 4)) == [[1, 2, 3]]
