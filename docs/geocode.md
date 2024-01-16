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

But wait - didn't we just clone a string there? Can't we use the `write!` trick we used
in the `pig-latinnify_2` function?

The answer is - yes, but it won't make much difference here. The reason is that we're using
a 3rd party crate (`reverse_geocoder`), and that crate is already allocating a string each
time we call `geocoder.search`.
