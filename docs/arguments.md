# 8. I'd like to have an argument, please

Say you want to rewrite
```python
def add_suffix(s, *, suffix):
    return s + suffix

s.map_elements(lambda x: add_suffix(x, suffix='-billy'))
```
as a plugin. How can you do that?

We've covered passing in extra columns, but...how about passing extra
keyword arguments?

We'll do this with `kwargs`. In `minimal_plugin/__init__.py`, add the
following:

```python
def add_suffix(expr: IntoExprColumn, *, suffix: str) -> pl.Expr:
    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="add_suffix",
        is_elementwise=True,
        kwargs={"suffix": suffix},
    )
```

In `src/expressions.rs`, we'll then first have to define a struct to hold
our keyword-arguments:

```rust
#[derive(Deserialize)]
struct AddSuffixKwargs {
    suffix: String,
}
```
Make sure to also add
```rust
use serde::Deserialize;
```
to the top of the file.

Then, we can just pass an argument of this type to a `add_suffix` function,
which is going to be very similar to the good version of `pig_latinnify`:

```rust
#[polars_expr(output_type=String)]
fn add_suffix(inputs: &[Series], kwargs: AddSuffixKwargs) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.str()?;
    let out = ca.apply_into_string_amortized(|value, output| {
        write!(output, "{}{}", value, kwargs.suffix).unwrap();
    });
    Ok(out.into_series())
}
```

To see it in action, compile with `maturin develop` (or `maturin develop --release` if you're
benchmarking), and then you should be able to put
```python
import polars as pl
import minimal_plugin as mp

df = pl.DataFrame({'a': ['bob', 'billy']})
print(df.with_columns(mp.add_suffix('a', suffix='-billy')))
```
into `run.py`, and run it to get
```
shape: (2, 1)
┌─────────────┐
│ a           │
│ ---         │
│ str         │
╞═════════════╡
│ bob-billy   │
│ billy-billy │
└─────────────┘
```
You can add multiple keyword-arguments in the same function, just make sure to
include them in the struct which you define on the Rust side.
