import polars as pl
import minimal_plugin  # noqa: F401

latitudes = [10., 20, 15]
longitudes = [-45., 60, 71]
df = pl.DataFrame({"lat": latitudes, "lon": longitudes}).with_columns(
    coords=pl.struct("lat", "lon")
)
print(df.select("coords", city1=pl.col("coords").mp.reverse_geocode()))
