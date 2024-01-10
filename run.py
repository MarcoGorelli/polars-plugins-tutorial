import polars as pl
import minimal_plugin
from datetime import date, datetime, timedelta

df = pl.DataFrame({
    'a': [1, 1, None],
    'b': [4, 5, 6],
    'c': [True, True, False],
    'd': ['foo', 'bar', 'ham'],
})
print(df.with_columns(summed=pl.col('a').minimal_plugin.add('b')))
