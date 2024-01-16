# 6. How to CRATE something else entirely

Take a look at [crates.io](https://crates.io/) - there's _so_ much good stuff there!
There's probably a package for practically any use case.

For example, this looks like a fun one: [reverse_geocoder](https://crates.io/crates/reverse_geocoder).
It lets us input a latitude-longitude pair of coordinates, and it'll tell us what the nearest
city is. Can we make a plugin out of it?

!!!note

    Before we begin: you'll need Rust Nightly for the
    examples in this page to compile. You can install
    it with

    ```
    rustup toolchain install nightly
    ```
    and then set it as the default with
    ```
    rustup default nightly
    ```

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

Then, we can define the function like this in `src/expressions.rs`:

```Rust
#[polars_expr(output_type=String)]
fn reverse_geocode(inputs: &[Series]) -> PolarsResult<Series> {
    let binding = inputs[0].struct_()?.field_by_name("lat")?;
    let latitude = binding.f64()?;
    let binding = inputs[0].struct_()?.field_by_name("lon")?;
    let longitude = binding.f64()?;
    let geocoder = ReverseGeocoder::new();
    let (lhs, rhs) = align_chunks_binary(latitude, longitude);
    let iter = lhs.downcast_iter().zip(rhs.downcast_iter()).map(
        |(lhs_arr, rhs_arr)| -> LargeStringArray {
            let mut buf = String::new();
            let mut mutarr: MutableUtf8Array<i64> =
                MutableUtf8Array::with_capacities(lhs_arr.len(), lhs_arr.len() * 20);

            for (lhs_opt_val, rhs_opt_val) in lhs_arr.iter().zip(rhs_arr.iter()) {
                match (lhs_opt_val, rhs_opt_val) {
                    (Some(lhs_val), Some(rhs_val)) => {
                        buf.clear();
                        let search_result = geocoder.search((*lhs_val, *rhs_val));
                        write!(buf, "{}", search_result.record.name).unwrap();
                        mutarr.push(Some(&buf))
                    }
                    _ => mutarr.push_null(),
                }
            }
            let arr: Utf8Array<i64> = mutarr.into();
            arr
        },
    );
    let out = StringChunked::from_chunk_iter(lhs.name(), iter);
    Ok(out.into_series())
}
```
You'll also need to add
```Rust
use crate::expressions::polars_core::utils::align_chunks_binary;
use reverse_geocoder::ReverseGeocoder;
use polars_arrow::array::MutableArray;
use polars_arrow::array::{MutableUtf8Array, Utf8Array};
```
to the top of the `src/expressions.rs` file, and add
```toml
polars-arrow = { version = "0.36.2", default-features = false }
```
to `Cargo.toml`.

Let's try it out! Put the following in `run.py`:
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

If you then compile with `maturin develop` (or `maturin develop --release`
if you're benchmarking), and run it with `python run.py`, you'll see:
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

In this example, we took on a few extra dependencies, which increase
the size of the package. By using plugins, we have a way of accessing
extra functionality without having to bloat up the size of the main
Polars install too much!

## Stretch goal

Can you write a function `reverse_geocode` which accepts float expressions as input?

Can you get the following to run?
```python
import polars as pl
import minimal_plugin  # noqa: F401

latitudes = [10., 20, 15]
longitudes = [-45., 60, 71]
df = pl.DataFrame({"lat": latitudes, "lon": longitudes})
print(df.select("coords", city=minimal_plugin.reverse_geocode('lat', 'lon')))
```
