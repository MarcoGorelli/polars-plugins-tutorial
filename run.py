import polars as pl
import minimal_plugin as mp


import polars as pl
import minimal_plugin as mp

df = pl.DataFrame({
    'values': [[1, 3, 2], [5, 7], []],
    'weights': [[.5, .3, .2], [.1, .9], []]
})
print(df.with_columns(weighted_mean = mp.weighted_mean('values', 'weights')))
