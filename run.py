import polars as pl
import minimal_plugin  # noqa: F401
import numpy as np

n = 100
df = pl.DataFrame({
    'a': [1, 2, 3, 4, None, 5],
    'b': [1, 1, 1, 2, 2, 2],
})
print(df.with_columns(a_cum_sum=pl.col('a').mp.cum_sum().over('b')))
