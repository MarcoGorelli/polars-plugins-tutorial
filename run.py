import polars as pl
import minimal_plugin  # noqa: F401
import numpy as np
rng = np.random.default_rng()

pl.Config.set_fmt_str_lengths(60)

latitudes = [53.2225504]
longitudes = [-4.2242607]

df = pl.DataFrame({'lat': latitudes, 'lon': longitudes}).with_columns(coords=pl.struct('lat', 'lon'))
print(df.select(
    city1=pl.col('coords').mp.reverse_geocode_1(),
    city2=pl.col('coords').mp.reverse_geocode_2(),
))
