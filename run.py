import polars as pl
import minimal_plugin
from datetime import date, datetime, timedelta

df = pl.DataFrame({
    'a': [1, 1, 2, None, 8, None],
    'b': [4, 5, 6, 7, None, None],
})

print(df.with_columns(
    a_noop = pl.col('a').minimal_plugin.noop(),
    a_add_b_expression = pl.col('a').minimal_plugin.add(pl.col('b')),
    a_add_b_literal = pl.col('a').minimal_plugin.add(pl.lit(1, dtype=pl.Int64)),
))

