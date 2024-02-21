# 11. Lost in space

Suppose, hypothetically speaking, that you're lost somewhere and only have access
to your latitude, your longitude, and a laptop on which you can write a Polars Plugin.
How can you find out what the closest city to you is?

## Reverse geocoding

The practice of starting with a (latitude, longitude) pair and finding out which
city it corresponds to is known as reverse geocoding.
We're not going to implement a reverse geocoder from scratch - instead, we'll
use the `reverse-geocoder` crate and make a plugin out of it!

## Cargo here, cargo there, cargo everywhere

Let's add that crate to our project by running `cargo add reverse-geocoder`.
You'll need to activate the nightly Rust channel, which you can do by making
a file ` rust-toolchain.toml` in your root directory
```toml
[toolchain]
channel = "nightly"
```
You'll also need to add
```toml
polars-arrow = { version = "0.37.0", default-features = false }
polars-core = { version = "0.37.0", default-features = false }
```
to `Cargo.toml`. Yes, this example is getting a bit heavier...

The way the `reverse-geocoder` crate works is:

- you instantiate a `ReverseGeocoder` instance
- you pass a (latitude, longitude) pair to `search`
- you get the city name out

So our plugin will work by taking two `Float64` columns (one of latitude, one
for longitude) and producing a String output column.

## Binary elementwise apply to buffer

In [How to STRING something together], we learned how to use `StringChunked.apply_to_buffer`
to run an elementwise function on a String column. Does Polars have a binary version of that one
which allows us to start from any data type?

  [Prerequisites]: ../prerequisites/
  [How to STRING something together]: ../stringify/

Unfortunately, not. But if you followed the Cookiecutter instrustions in [Prerequisites], then
you'll find a such function in `src/utils.rs`! Please go to that file and uncomment the
function `binary_apply_to_buffer_generic`.

We can then define out reverse geocoder plugin function. On the Python side,
in `minimal_plugin/__init__.py`:
```python
def reverse_geocode(lat: IntoExpr, long: IntoExpr) -> pl.Expr:
    lat = parse_into_expr(lat)
    return lat.register_plugin(
        lib=lib, symbol="reverse_geocode", is_elementwise=True, args=[long]
    )
```
and on the Rust side, in `src/expressions.rs`:
```Rust
use reverse_geocoder::ReverseGeocoder;

#[polars_expr(output_type=String)]
fn reverse_geocode(inputs: &[Series]) -> PolarsResult<Series> {
    let lhs = inputs[0].f64()?;
    let rhs = inputs[1].f64()?;
    let geocoder = ReverseGeocoder::new();

    let out = binary_apply_to_buffer_generic(lhs, rhs, |lhs_val, rhs_val| {
        let search_result = geocoder.search((lhs_val, rhs_val));
        search_result.record.name.to_string()
    });
    Ok(out.into_series())
}
```

To run it, put the following in `run.py`:
```python
import polars as pl
import minimal_plugin as mp

df = pl.DataFrame({
    'lat': [37.7749, 51.01, 52.5],
    'lon': [-122.4194, -3.9, -.91]
})
print(df.with_columns(city=mp.reverse_geocode('lat', 'lon')))
```
then compile with `maturin develop` (or `maturin develop --release` if you're benchmarking)
and you should see
```
shape: (3, 3)
┌─────────┬───────────┬───────────────────┐
│ lat     ┆ lon       ┆ city              │
│ ---     ┆ ---       ┆ ---               │
│ f64     ┆ f64       ┆ str               │
╞═════════╪═══════════╪═══════════════════╡
│ 37.7749 ┆ -122.4194 ┆ San Francisco     │
│ 51.01   ┆ -3.9      ┆ South Molton      │
│ 52.5    ┆ -0.91     ┆ Market Harborough │
└─────────┴───────────┴───────────────────┘
```
in the output!

Great, now in our hypothetical scenario, you're probably still lost, but
at least you know which city you're closest to.
