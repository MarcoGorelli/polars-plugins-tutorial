import polars as pl
import minimal_plugin as mp

df = pl.DataFrame({'value':[[9, 1, 0], [1, 2], [3, 0, 1, 2]]})

print(df.with_columns(mp.non_zero_indices('value'))) 
