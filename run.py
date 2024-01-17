import polars as pl
import minimal_plugin  # noqa: F401

df = pl.DataFrame({'a': [1,2,3], 'b': [4,5,6]})
print(df.with_columns(pl.col('a').mp.rename()))
