# 12. Lost in space

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
You'll also need to add `polars-arrow` and `polars-core` to `Cargo.toml`
and pin them to the same version that you pin `polars` to.
Yes, this example is getting a bit heavier...

The way the `reverse-geocoder` crate works is:

- you instantiate a `ReverseGeocoder` instance
- you pass a (latitude, longitude) pair to `search`
- you get the city name out

So our plugin will work by taking two `Float64` columns (one of latitude, one
for longitude) and producing a String output column.

## Binary elementwise apply to buffer

In [How to STRING something together], we learned how to use `StringChunked.apply_into_string_amortized`
to run an elementwise function on a String column. Does Polars have a binary version of that one
which allows us to start from any data type?

  [Prerequisites]: ../prerequisites/
  [How to STRING something together]: ../stringify/

Unfortunately, not. But, this is a good chance to learn about a few new concepts!

We'll start easy by dealing with the Python side. Add the following to `minimal_plugin/__init__.py`:

```python
def reverse_geocode(lat: IntoExpr, long: IntoExpr) -> pl.Expr:
    return register_plugin(
        args=[lat, long], lib=lib, symbol="reverse_geocode", is_elementwise=True
    )
```

On the Rust side, in `src/expressions.rs`, get ready for it, we're going to add:

```Rust
use polars_arrow::array::MutablePlString;
use polars_core::utils::align_chunks_binary;
use reverse_geocoder::ReverseGeocoder;

#[polars_expr(output_type=String)]
fn reverse_geocode(inputs: &[Series]) -> PolarsResult<Series> {
    let latitude = inputs[0].f64()?;
    let longitude = inputs[1].f64()?;
    let geocoder = ReverseGeocoder::new();
    let out = binary_elementwise_into_string_amortized(latitude, longitude, |lhs, rhs, out| {
        let search_result = geocoder.search((lhs, rhs));
        write!(out, "{}", search_result.record.name).unwrap();
    });
    Ok(out.into_series())
}
```

We use the utility function `binary_elementwise_into_string_amortized`,
which is a binary version of `apply_into_string_amortized` which we learned
about in the [Stringify] chapter.

  [Stringify]: ../stringify/

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
