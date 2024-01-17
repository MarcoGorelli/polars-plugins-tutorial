import timeit
import warnings
import numpy as np

setup = """
import pandas as pd
import polars as pl
import minimal_plugin  # noqa: F401
import numpy as np
rng = np.random.default_rng(12345)
N = 10_000_000

df = pl.DataFrame({'a': rng.integers(low=-100, high=100, size=N)})
df = df.with_row_index().with_columns(
    pl.when(pl.col('index')%2==1).then(pl.lit(None)).otherwise(pl.col('a')).alias('a')
)
"""

results = np.array(timeit.Timer(
    stmt="df.select(pl.col('a').mp.abs_i64_fast())",
    setup=setup,
    )
    .repeat(7, 3)
)/3
print(f'min: {min(results)}')
print(f'max: {max(results)}')
print(f'{np.mean(results)} +/- {np.std(results)/np.sqrt(len(results))}')

results = np.array(timeit.Timer(
    stmt="df.select(pl.col('a').mp.abs_i64())",
    setup=setup,
    )
    .repeat(7, 3)
)/3
print(f'min: {min(results)}')
print(f'max: {max(results)}')
print(f'{np.mean(results)} +/- {np.std(results)/np.sqrt(len(results))}')

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    results = np.array(timeit.Timer(
        stmt="df.select(pl.col('a').map_elements(lambda x: abs(x)))",
        setup=setup,
        )
        .repeat(7, 3)
    )/3
print(f'min: {min(results)}')
print(f'max: {max(results)}')
print(f'{np.mean(results)} +/- {np.std(results)/np.sqrt(len(results))}')
