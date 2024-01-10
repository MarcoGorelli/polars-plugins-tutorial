import polars as pl
import minimal_plugin
from datetime import date, datetime, timedelta

df = pl.DataFrame({
    'a': [1, 1, None],
    'b': [4., 5, 6],
    'c': [True, True, False],
    'd': ['foo', 'bar', 'ham'],
})
pl.Config().set_tbl_cols(30)
print(df.with_columns(
    pl.col('*').minimal_plugin.noop().name.suffix('_')
))

df = pl.DataFrame({
    'a': [1, 1, 1, 1, 1, 1],
    'b': [4,4,4,5,5,5]
})
print(df.with_columns(
    a_cum = pl.col('a').minimal_plugin.cum_sum(),
    a_cum_over = pl.col('a').minimal_plugin.cum_sum().over('b')
))

print(df.lazy().with_columns(pl.col('a').minimal_plugin.cum_sum().over('b')).schema)