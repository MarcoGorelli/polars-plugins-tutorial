import polars as pl
import minimal_plugin  # noqa: F401

df = pl.DataFrame({'a': ['bob', 'billy']})
print(df.with_columns(pl.col('a').mp.add_suffix(suffix='-billy')))
