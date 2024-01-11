import polars as pl
import minimal_plugin  # noqa: F401

df = pl.DataFrame({
    'a': [1, -1, None],
    'b': [4.1, 5.2, -6.3],
    'c': ['hello', 'everybody!', '!']
})
print(df.with_columns(pl.col(pl.Int64).mp.abs_i64().name.suffix('_abs')))
