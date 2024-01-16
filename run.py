import polars as pl
import minimal_plugin  # noqa: F401

df = pl.DataFrame({'a': ["I", "love", "pig", "latin"]})
print(df.with_columns(a_pig_latin=pl.col('a').mp.pig_latinnify()))