import polars as pl
import minimal_plugin as mp
from polars.testing import assert_frame_equal


def test_noop():
    df = pl.DataFrame(
        {"a": [1, 1, None], "b": [4.1, 5.2, 6.3], "c": ["hello", "everybody!", "!"]}
    )
    result = df.with_columns(mp.noop(pl.all()).name.suffix("_noop"))
    expected = pl.DataFrame(
        {
            "a": [1, 1, None],
            "b": [4.1, 5.2, 6.3],
            "c": ["hello", "everybody!", "!"],
            "a_noop": [1, 1, None],
            "b_noop": [4.1, 5.2, 6.3],
            "c_noop": ["hello", "everybody!", "!"],
        }
    )
    assert_frame_equal(result, expected)


def test_abs_i64():
    df = pl.DataFrame(
        {"a": [1, -1, None], "b": [4.1, 5.2, -6.3], "c": ["hello", "everybody!", "!"]}
    )
    result = df.with_columns(mp.abs_i64("a").name.suffix("_abs"))
    expected = pl.DataFrame(
        {
            "a": [1, -1, None],
            "b": [4.1, 5.2, -6.3],
            "c": ["hello", "everybody!", "!"],
            "a_abs": [1, 1, None],
        }
    )
    assert_frame_equal(result, expected)

def test_abs_numeric():
    df = pl.DataFrame(
        {"a": [1, -1, None], "b": [4.1, 5.2, -6.3], "c": ["hello", "everybody!", "!"]}
    )
    result = df.with_columns(mp.abs_numeric(pl.col('a', 'b')).name.suffix('_abs'))
    expected = pl.DataFrame({'a': [1, -1, None], 'b': [4.1, 5.2, -6.3], 'c': ['hello', 'everybody!', '!'], 'a_abs': [1, 1, None], 'b_abs': [4.1, 5.2, 6.3]})
    assert_frame_equal(result, expected)

def test_sum_i64():
    df = pl.DataFrame({'a': [1, 5, 2], 'b': [3, None, -1]})
    result = df.with_columns(a_plus_b=mp.sum_i64('a', 'b'))
    expected = pl.DataFrame({'a': [1, 5, 2], 'b': [3, None, -1], 'a_plus_b': [4, None, 1]})
    assert_frame_equal(result, expected)

def test_cum_sum():
    df = pl.DataFrame({
        'a': [1, 2, 3, 4, None, 5],
        'b': [1, 1, 1, 2, 2, 2],
    })
    result = df.with_columns(a_cum_sum=mp.cum_sum('a'))
    expected = pl.DataFrame({'a': [1, 2, 3, 4, None, 5], 'b': [1, 1, 1, 2, 2, 2], 'a_cum_sum': [1, 3, 6, 10, None, 15]})
    assert_frame_equal(result, expected)
    result = df.with_columns(a_cum_sum=mp.cum_sum('a').over('b'))
    expected = pl.DataFrame({'a': [1, 2, 3, 4, None, 5], 'b': [1, 1, 1, 2, 2, 2], 'a_cum_sum': [1, 3, 6, 4, None, 9]})
    assert_frame_equal(result, expected)