import polars as pl
import minimal_plugin  # noqa: F401

latitudes = [53.2225504]
longitudes = [-4.2242607]
df = pl.DataFrame({"lat": latitudes, "lon": longitudes}).head(3).with_columns(
    coords=pl.struct("lat", "lon")
)
print(df.select("coords", city1=pl.col("coords").mp.reverse_geocode_1()))

# 1: [82, 101, 109, 105, 114, 101, 45, 77, 111, 110, 116, 106, 111, 108, 121]
# 2: [82, 101, 109, 105, 114, 101, 45, 77, 111, 110, 116, 106, 111, 108, 121, 83, 117, 114]
# 3: [82, 101, 109, 105, 114, 101, 45, 77, 111, 110, 116, 106, 111, 108, 121, 83, 117, 114, 77, 97, 108, 118, 97, 110]
