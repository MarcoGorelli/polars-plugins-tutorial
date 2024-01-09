import polars as pl
import minimal_plugin
from datetime import date, datetime, timedelta

df = pl.DataFrame({
    'a': [1, 1, None],
    'b': [4., 5, 6],
    'c': [True, True, False],
    'd': ['foo', 'bar', 'ham'],
    'e': [date(2020, 1, 1), date(2020, 1, 2), date(2020,1,3)],
    'f': [datetime(2020, 1, 1), datetime(2020, 1, 2), datetime(2020,1,3)],
    'g': [timedelta(1), timedelta(2), timedelta(3)],
    'h': [[1, 2], [3, 4], [5, 6]],
    'i': [{'a': 1}, {'a': 2}, {'a': 3}],
})
pl.Config().set_tbl_cols(30)
print(df.with_columns(
    pl.col('*').minimal_plugin.noop().name.suffix('_')
))
