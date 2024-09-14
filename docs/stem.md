# 6. How to CRATE something else entirely

Take a look at [crates.io](https://crates.io/) - there's _so_ much good stuff there!
There's probably a package for practically any use case.

For example, this looks like a fun one: [rust_stemmers](https://crates.io/crates/rust-stemmers).
It lets us input a word, and stem it (i.e. reduce it to a simpler version, e.g. 'fearlessly' -> 
'fearless').
Can we make a plugin out of it?

## Cargo this, cargo that

If we're going to use `rust_stemmers`, we're going to need to take it on as a dependency.
The easiest way to do this is probably to run `cargo add rust_stemmers` - run this, and
watch how `Cargo.toml` changes!
You should see the line
```toml
rust-stemmers = "1.2.0"
```
somewhere in there.

## Writing a Snowball Stemmer

Let's write a function which:

- takes a `Utf8` columns as input;
- produces a `Utf8` column as output.

We'd like to be able to call it as follows:

```python
df.with_columns(stemmed_word=mp.snowball_stem('word'))
```

On the Python side, let's add the following function to `minimal_plugin/__init__.py`:

```python
def snowball_stem(expr: IntoExpr) -> pl.Expr:
    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="snowball_stem",
        is_elementwise=True,
    )
```

Then, we can define the function like this in `src/expressions.rs`:

```Rust
use rust_stemmers::{Algorithm, Stemmer};

#[polars_expr(output_type=String)]
fn snowball_stem(inputs: &[Series]) -> PolarsResult<Series> {
    let ca: &StringChunked = inputs[0].str()?;
    let en_stemmer = Stemmer::create(Algorithm::English);
    let out: StringChunked = ca.apply_into_string_amortized(|value: &str, output: &mut String| {
        write!(output, "{}", en_stemmer.stem(value)).unwrap()
    });
    Ok(out.into_series())
}
```

Let's try it out! Put the following in `run.py`:
```python
import polars as pl
import minimal_plugin as mp

df = pl.DataFrame({'word': ["fearlessly", "littleness", "lovingly", "devoted"]})
print(df.with_columns(b=mp.snowball_stem('word')))
```

If you then compile with `maturin develop` (or `maturin develop --release`
if you're benchmarking), and run it with `python run.py`, you'll see:
```
shape: (4, 2)
┌────────────┬──────────┐
│ a          ┆ b        │
│ ---        ┆ ---      │
│ str        ┆ str      │
╞════════════╪══════════╡
│ fearlessly ┆ fearless │
│ littleness ┆ littl    │
│ lovingly   ┆ love     │
│ devoted    ┆ devot    │
└────────────┴──────────┘
```

In this example, we took on an extra dependency, which increased
the size of the package. By using plugins, we have a way of accessing
extra functionality without having to bloat up the size of the main
Polars install too much!

## Stretch goal

Browse through `crates.io` - is there any other crate you could use
to make your own plugin out of?
