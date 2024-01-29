import polars as pl
import minimal_plugin as mp

df = pl.DataFrame(
    {
        "a": [1, 3, 8],
        "b": [2.0, 3.1, 2.5],
        "c": ["3", "7", "3"],
    }
).select(abc=pl.struct("a", "b", "c"))
print(df.with_columns(abc_shifted=mp.shift_struct("abc")))
import pprint
pprint.pprint(df.with_columns(abc_shifted=mp.shift_struct("abc")).schema)
# print(df.lazy().with_columns(swapped= mp.shift_struct('a')).schema)
# print(df.lazy().with_columns(swapped= mp.shift_struct('a')).collect().schema)
