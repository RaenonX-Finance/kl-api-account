from kl_site_common.utils.array import find_missing_intervals, split_chunks


def test_find_missing_intervals():
    assert find_missing_intervals(1, 10, 1, []) == [(1, None)]
    assert find_missing_intervals(1, 10, 1, [1, 2, 3]) == [(4, None)]
    assert find_missing_intervals(1, 10, 1, [3, 4, 5]) == [(1, 3), (6, None)]
    assert find_missing_intervals(1, 10, 1, [6, 8, 9]) == [(1, 6), (7, 8), (10, None)]
    assert find_missing_intervals(1, 10, 1, list(range(1, 11))) == []


def test_split_chunks():
    assert list(split_chunks([1, 2, 3], chunk_size=1)) == [[1], [2], [3]]
    assert list(split_chunks([1, 2, 3], chunk_size=2)) == [[1, 2], [3]]
    assert list(split_chunks([1, 2, 3], chunk_size=3)) == [[1, 2, 3]]
    assert list(split_chunks([1, 2, 3], chunk_size=4)) == [[1, 2, 3]]


def test_split_chunks_multiple_arrays():
    assert list(split_chunks([1, 2, 3], [4, 5, 6], chunk_size=1)) == [[[1], [4]], [[2], [5]], [[3], [6]]]
    assert list(split_chunks([1, 2, 3], [4, 5, 6], chunk_size=2)) == [[[1, 2], [4, 5]], [[3], [6]]]
    assert list(split_chunks([1, 2, 3], [4, 5, 6], chunk_size=3)) == [[[1, 2, 3], [4, 5, 6]]]
    assert list(split_chunks([1, 2, 3], [4, 5, 6], chunk_size=4)) == [[[1, 2, 3], [4, 5, 6]]]
