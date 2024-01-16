# 6. How to CRATE something else entirely

Take a look at [crates.io](https://crates.io/) - there's _so_ much good stuff there!
There's probably a package for practically any use case.

For example, this looks like a fun one: [reverse_geocoder](https://crates.io/crates/reverse_geocoder).
It lets us input a latitude-longitude pair of coordinates, and it'll tell us what the nearest
city is. Can we make a plugin out of it?

## Cargo this, cargo that

If we're going to use `reverse_geocoder`, we're going to need to take it on as a dependency.
The easiest way to do this is probably to run `cargo add reverse_geocoder` - run this, and
watch how `Cargo.toml` changes!
You should see the line
```toml
reverse_geocoder = "4.0.0"
```
somewhere in there.

## Writing an offline reverse-geocoder

Let's write a function which:

- takes a `Struct` columns as input;
- produces a `Utf8` column as output.

We'd like to be able to call it as follows:

```python
df.with_columns(
    city=pl.col('coordinates').mp.reverse_geocode()
)
```

On the Python side, let's add the following function to `minimal_plugin/__init__.py`:

```python
def reverse_geocode(self) -> pl.Expr:
    return self._expr.register_plugin(
        lib=lib,
        symbol="reverse_geocode",
        is_elementwise=True,
    )
```

In order to be able to work with structs on the Rust side, we'll need to enable
the `dtype-struct` feature in `Cargo.toml`:
```toml
polars = { version = "0.36.2", features = ["dtype-struct"], default-features = false }
```

Then, we can define the function like this:

```Rust
#[polars_expr(output_type=String)]
fn reverse_geocode(inputs: &[Series]) -> PolarsResult<Series> {
    let binding = inputs[0].struct_()?.field_by_name("lat")?;
    let latitude = binding.f64()?;
    let binding = inputs[0].struct_()?.field_by_name("lon")?;
    let longitude = binding.f64()?;
    let geocoder = ReverseGeocoder::new();
    let out: StringChunked =
        binary_elementwise(latitude, longitude, |left, right| match (left, right) {
            (Some(left), Some(right)) => {
                let search_result = geocoder.search((left, right));
                Some(search_result.record.name.clone())
            }
            _ => None,
        });
    Ok(out.into_series())
```

Note: this isn't as efficient as it could be. `geocoder.search` is allocating strings,
which we're then cloning. If you wanted to use this in a performance-critical setting,
you might want to clone `reverse-geocoder` and then use the `write!` trick from
`pig_latinnify_2`.

Let's try it out!
```python
import polars as pl
import minimal_plugin  # noqa: F401

latitudes = [10., 20, 15]
longitudes = [-45., 60, 71]
df = pl.DataFrame({"lat": latitudes, "lon": longitudes}).with_columns(
    coords=pl.struct("lat", "lon")
)
print(df.select("coords", city=pl.col("coords").mp.reverse_geocode()))
```
```
shape: (3, 2)
┌──────────────┬─────────────────┐
│ coords       ┆ city            │
│ ---          ┆ ---             │
│ struct[2]    ┆ str             │
╞══════════════╪═════════════════╡
│ {10.0,-45.0} ┆ Remire-Montjoly │
│ {20.0,60.0}  ┆ Sur             │
│ {15.0,71.0}  ┆ Malvan          │
└──────────────┴─────────────────┘
```

## Stretch goal

Can you write a function `reverse_geocode` which accepts float expressions as input?

As in, can you get the following to run?
```python
import polars as pl
import minimal_plugin  # noqa: F401

latitudes = [10., 20, 15]
longitudes = [-45., 60, 71]
df = pl.DataFrame({"lat": latitudes, "lon": longitudes})
print(df.select("coords", city=minimal_plugin.reverse_geocode('lat', 'lon')))
```