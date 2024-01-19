import polars as pl
import minimal_plugin  # noqa: F401

df = pl.DataFrame({
    'values': [[1, 3, 2], [5, 7]],
    'weights': [[.5, .3, .2], [.1, .9]]
})
print(df.with_columns(weighted_mean = pl.col('values').mp.weighted_mean(pl.col('weights'))))