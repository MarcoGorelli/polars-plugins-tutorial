import polars as pl
import minimal_plugin as mp

df = pl.DataFrame({
    'a': [1,3,8],
    'b': [2.,3.1,2.5],
    'c': ['3', '7', '3'],
}).select(pl.struct('a', 'b', 'c'))
print(df.with_columns(swapped= mp.shift_struct('a')))
