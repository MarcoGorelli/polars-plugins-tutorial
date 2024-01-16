import polars as pl
import minimal_plugin  # noqa: F401

latitudes = [53.2225504]
longitudes = [-4.2242607]
df = pl.DataFrame({"lat": latitudes, "lon": longitudes}).head(3).with_columns(
    coords=pl.struct("lat", "lon")
)
print(df.select("coords", city1=pl.col("coords").mp.reverse_geocode()))
