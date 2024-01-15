import timeit
import numpy as np

setup = """
import pandas as pd
import polars as pl
import minimal_plugin  # noqa: F401
import numpy as np
N = 1_000_000
s_arr = pl.from_pandas(pd.Series(pd._testing.rands_array(10, N)).astype('string[pyarrow]'))
def pig_latinnify_python(s: str) -> str:
    if s:
        return s[1:] + 'ay'
    return s


df = pl.DataFrame({
    'a': s_arr
})
"""

results = np.array(timeit.Timer(
    stmt="df.select(pl.col('a').mp.pig_latinnify_1())",
    setup=setup,
    )
    .repeat(7, 3)
)/3
print(f'min: {min(results)}')
print(f'max: {max(results)}')
print(f'{np.mean(results)} +/- {np.std(results)/np.sqrt(len(results))}')

results = np.array(timeit.Timer(
    stmt="df.select(pl.col('a').mp.pig_latinnify_2())",
    setup=setup,
    )
    .repeat(7, 3)
)/3
print(f'min: {min(results)}')
print(f'max: {max(results)}')
print(f'{np.mean(results)} +/- {np.std(results)/np.sqrt(len(results))}')

results = np.array(timeit.Timer(
    stmt="df.select(pl.col('a').map_elements(pig_latinnify_python))",
    setup=setup,
    )
    .repeat(7, 3)
)/3
print(f'min: {min(results)}')
print(f'max: {max(results)}')
print(f'{np.mean(results)} +/- {np.std(results)/np.sqrt(len(results))}')
