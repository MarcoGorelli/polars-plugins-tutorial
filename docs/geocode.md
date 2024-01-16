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

- takes two `Float64` columns as input;
- produces a `Utf8` column as output.

We'd like to be able to call it as follows:

```python
df.with_columns(
    city=minimal_plugin.reverse_geocode('latitude', 'longitude')
)
```

On the Python side, let's add the following function to `minimal_plugin/__init__.py`:

```python

```