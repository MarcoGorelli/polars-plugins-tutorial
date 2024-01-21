import polars as pl
import minimal_plugin  # noqa: F401

df = pl.DataFrame({
    'a': [{'a':1, 'b': 2, 'c': 3}]
})
print(df.with_columns(b=pl.col('a').mp.shift_struct()))
print(df.with_columns(b=pl.col('a').mp.shift_struct())['a'].item())
print(df.with_columns(b=pl.col('a').mp.shift_struct())['b'].item())
print(df.lazy().with_columns(b=pl.col('a').mp.shift_struct()).schema)
print(df.lazy().with_columns(b=pl.col('a').mp.shift_struct()).collect().schema)