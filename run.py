
import polars as pl
import minimal_plugin as mp


df = pl.DataFrame(
    {"values": [[1, 3, 2], [5, 7], []], "weights": [[0.5, 0.3, 0.2], [0.1, 0.9], []]}
)
print(df.with_columns(weighted_mean=mp.weighted_mean("values", "weights")))

df = pl.DataFrame(
    {
        "english": ["foo", "bar", ""],
    }
)
print(df.with_columns(pig_latin=mp.pig_latinnify("english")))

df = pl.DataFrame(
    {
        "values": [1.0, 3, 2, 5, 7],
        "weights": [0.5, 0.3, 0.2, 0.1, 0.9],
        "group": ["a", "a", "a", "b", "b"],
    }
)
print(
    df.group_by("group").agg(
        weighted_mean=mp.vertical_weighted_mean("values", "weights")
    )
)

df = pl.DataFrame(
    {
        "a": [None, None, 3, None, None, 9, 11, None],
    }
)
result = df.with_columns(interpolate=mp.interpolate("a"))
print(result)


df = pl.DataFrame({
    'filename': [
        "requirements.txt", "Makefile", "pkg.tar.gz", "tmp.d"
    ],
})
print(df.with_columns(without_ext=mp.remove_extension('filename')))

points = pl.Series(
    "points",
    [
        [6.63, 8.35],
        [7.19, 4.85],
        [2.1, 4.21],
        [3.4, 6.13],
        [2.48, 9.26],
        [9.41, 7.26],
        [7.45, 8.85],
        [6.58, 5.22],
        [6.05, 5.77],
        [8.57, 4.16],
        [3.22, 4.98],
        [6.62, 6.62],
        [9.36, 7.44],
        [8.34, 3.43],
        [4.47, 7.61],
        [4.34, 5.05],
        [5.0, 5.05],
        [5.0, 5.0],
        [2.07, 7.8],
        [9.45, 9.6],
        [3.1, 3.26],
        [4.37, 5.72],
    ],
    dtype=pl.Array(pl.Float64, 2),
)
df = pl.DataFrame(points)
result = df.with_columns(midpoints=mp.midpoint_2d("points", ref_point=(5.0, 5.0)))
print(result)
