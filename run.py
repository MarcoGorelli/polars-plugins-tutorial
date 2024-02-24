import polars as pl
import minimal_plugin as mp

df = pl.DataFrame(
    {
        "a": [1, 3, 8],
        "b": [2.0, 3.1, 2.5],
        "c": ["3", "7", "3"],
    }
)

print(df.with_columns(mp.cum_sum('a')))

df = pl.DataFrame({
    'lat': [37.7749, 51.01, 52.5],
    'lon': [-122.4194, -3.9, -.91]
})
print(df.with_columns(city=mp.reverse_geocode('lat', 'lon')))

pl.Config().set_fmt_table_cell_list_len(10)

df = pl.DataFrame({'dense': [[0, 9], [8, 6, 0, 9], None, [3, 3]]})
print(df)
print(df.with_columns(indices=mp.non_zero_indices('dense')))
