import timeit
import warnings
import numpy as np

setup = """
import polars as pl
import minimal_plugin as mp
import numpy as np
rng = np.random.default_rng(12345)
N = 100_000

df = pl.DataFrame({'a': [rng.integers(low=-100, high=100, size=5) for _ in range(N)]})
"""

results = (
    np.array(
        timeit.Timer(
            stmt="df.select(mp.non_zero_indices('a'))",
            setup=setup,
        ).repeat(7, 3)
    )
    / 3
)
print(f"min: {min(results)}")
print(f"max: {max(results)}")
print(f"{np.mean(results)} +/- {np.std(results)/np.sqrt(len(results))}")

results = (
    np.array(
        timeit.Timer(
            stmt="df.select(pl.col('a').list.eval(pl.arg_where(pl.element() != 0)))",
            setup=setup,
        ).repeat(7, 3)
    )
    / 3
)
print(f"min: {min(results)}")
print(f"max: {max(results)}")
print(f"{np.mean(results)} +/- {np.std(results)/np.sqrt(len(results))}")
