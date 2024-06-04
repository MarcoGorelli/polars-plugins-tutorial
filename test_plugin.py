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
    result = df.with_columns(mp.abs_numeric(pl.col("a", "b")).name.suffix("_abs"))
    expected = pl.DataFrame(
        {
            "a": [1, -1, None],
            "b": [4.1, 5.2, -6.3],
            "c": ["hello", "everybody!", "!"],
            "a_abs": [1, 1, None],
            "b_abs": [4.1, 5.2, 6.3],
        }
    )
    assert_frame_equal(result, expected)


def test_sum_i64():
    df = pl.DataFrame({"a": [1, 5, 2], "b": [3, None, -1]})
    result = df.with_columns(a_plus_b=mp.sum_i64("a", "b"))
    expected = pl.DataFrame(
        {"a": [1, 5, 2], "b": [3, None, -1], "a_plus_b": [4, None, 1]}
    )
    assert_frame_equal(result, expected)


def test_cum_sum():
    df = pl.DataFrame(
        {
            "a": [1, 2, 3, 4, None, 5],
            "b": [1, 1, 1, 2, 2, 2],
        }
    )
    result = df.with_columns(a_cum_sum=mp.cum_sum("a"))
    expected = pl.DataFrame(
        {
            "a": [1, 2, 3, 4, None, 5],
            "b": [1, 1, 1, 2, 2, 2],
            "a_cum_sum": [1, 3, 6, 10, None, 15],
        }
    )
    assert_frame_equal(result, expected)
    result = df.with_columns(a_cum_sum=mp.cum_sum("a").over("b"))
    expected = pl.DataFrame(
        {
            "a": [1, 2, 3, 4, None, 5],
            "b": [1, 1, 1, 2, 2, 2],
            "a_cum_sum": [1, 3, 6, 4, None, 9],
        }
    )
    assert_frame_equal(result, expected)


def test_pig_latinnify():
    df = pl.DataFrame({"a": ["I", "love", "pig", "latin"]})
    result = df.with_columns(a_pig_latin=mp.pig_latinnify("a"))
    expected = pl.DataFrame(
        {
            "a": ["I", "love", "pig", "latin"],
            "a_pig_latin": ["Iay", "ovelay", "igpay", "atinlay"],
        }
    )
    assert_frame_equal(result, expected)


def test_add_suffix():
    df = pl.DataFrame({"a": ["bob", "billy"]})
    result = df.with_columns(mp.add_suffix("a", suffix="-billy"))
    expected = pl.DataFrame({"a": ["bob-billy", "billy-billy"]})
    assert_frame_equal(result, expected)


def test_weighted_mean():
    df = pl.DataFrame(
        {"values": [[1, 3, 2], [5, 7], None, [5, 7], []], "weights": [[0.5, 0.3, 0.2], [0.1, 0.9], [.1, .9], None, []]}
    )
    result = df.with_columns(weighted_mean=mp.weighted_mean("values", "weights"))
    expected = pl.DataFrame(
        {
            "values": [[1, 3, 2], [5, 7], None, [5, 7], []],
            "weights": [[0.5, 0.3, 0.2], [0.1, 0.9], [.1, .9], None, []],
            "weighted_mean": [1.7999999999999998, 6.8, None, None, None],
        }
    )
    assert_frame_equal(result, expected)


def test_non_zero_indices():
    df = pl.DataFrame({"dense": [[0, 9], [8, 6, 0, 9], None, [3, 3]]})
    result = df.with_columns(indices=mp.non_zero_indices("dense"))
    expected = pl.DataFrame(
        {
            "dense": [[0, 9], [8, 6, 0, 9], None, [3, 3]],
            "indices": [[1], [0, 1, 3], None, [0, 1]],
        },
        schema_overrides={"indices": pl.List(pl.UInt32)},
    )
    assert_frame_equal(result, expected)


def test_shift_struct():
    df = pl.DataFrame(
        {
            "a": [1, 3, 8],
            "b": [2.0, 3.1, 2.5],
            "c": ["3", "7", "3"],
        }
    ).select(abc=pl.struct("a", "b", "c"))
    result = df.with_columns(abc_shifted=mp.shift_struct("abc"))
    expected = pl.DataFrame(
        {
            "abc": [
                {"a": 1, "b": 2.0, "c": "3"},
                {"a": 3, "b": 3.1, "c": "7"},
                {"a": 8, "b": 2.5, "c": "3"},
            ],
            "abc_shifted": [
                {"a": 2.0, "b": "3", "c": 1},
                {"a": 3.1, "b": "7", "c": 3},
                {"a": 2.5, "b": "3", "c": 8},
            ],
        }
    )
    assert_frame_equal(result, expected)


def test_reverse_geocode():
    df = pl.DataFrame({"lat": [37.7749, 51.01, 52.5], "lon": [-122.4194, -3.9, -0.91]})
    result = df.with_columns(city=mp.reverse_geocode("lat", "lon"))
    expected = pl.DataFrame(
        {
            "lat": [37.7749, 51.01, 52.5],
            "lon": [-122.4194, -3.9, -0.91],
            "city": ["San Francisco", "South Molton", "Market Harborough"],
        }
    )
    assert_frame_equal(result, expected)
