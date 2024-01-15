import polars as pl
import minimal_plugin  # noqa: F401


df = pl.DataFrame({'a': ['fdsafasd', 'fdasfsdfae', 'aaafsdfa']})
print(df.with_columns(b=pl.col('a').mp.pig_latinnify_1()))
print(df.with_columns(b=pl.col('a').mp.pig_latinnify_2()))

def pig_latinnify_python(s: str) -> str:
    if s:
        return s[1:] + 'ay'
    return s

print(df.with_columns(b=pl.col('a').map_elements(pig_latinnify_python)))