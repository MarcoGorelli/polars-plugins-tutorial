# 5. How to STRING something together

Tired of examples which only include numeric data? Me neither.
But we need to address the elephant in the room: strings.

We're going to start by re-implementing a pig-latinnifier.
This example is already part of the `pyo3-polars` repo examples,
but we'll tackle it with a different spin here.

## Pig-latinnify - take 1

Let's start by taking our `abs` example, and adapting it to the
string case. We'll follow the same strategy:

- iterate over arrow arrays
- for each element in each array, compute the output value

Put the following in `src/expressions.rs`:

```Rust
#[polars_expr(output_type=String)]
fn pig_latinnify(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.str()?;
    let chunks = ca.downcast_iter().map(|arr| {
        arr.into_iter()
            .map(|opt_v| {
                opt_v.map(|value| {
                    // Note: this isn't the recommended way to do it.
                    // See below for a better way!
                    if let Some(first_char) = value.chars().next() {
                        format!("{}{}ay", &value[1..], first_char)
                    } else {
                        value.to_string()
                    }
                })
            })
            .collect()
    });
    let out = StringChunked::from_chunk_iter(ca.name(), chunks);
    Ok(out.into_series())
}
```

If you combine this with a Python definition (which you should put
in `minimal_plugin/__init__.py`):

```python
    def pig_latinnify(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=lib,
            symbol="pig_latinnify",
            is_elementwise=True,
        )
```
then you'll be able to pig-latinnify a column of strings! To see it
in action, compile with `maturin develop` (or `maturin develop --release`
if you're benchmarking) and put the following in `run.py`:

```python
import polars as pl
import minimal_plugin  # noqa: F401

df = pl.DataFrame({'a': ["I", "love", "pig", "latin"]})
print(df.with_columns(a_pig_latin=pl.col('a').mp.pig_latinnify()))
```
```
shape: (4, 2)
┌───────┬─────────────┐
│ a     ┆ a_pig_latin │
│ ---   ┆ ---         │
│ str   ┆ str         │
╞═══════╪═════════════╡
│ I     ┆ Iay         │
│ love  ┆ ovelay      │
│ pig   ┆ igpay       │
│ latin ┆ atinlay     │
└───────┴─────────────┘
```

This will already be an order of magnitude faster than using `map_elements` and
a Python lambda function. But you may have noticed that, for every row, we're
allocating a string, even if we don't need to.

Can we do better?

## Pig-latinnify - take 2

Yes! In this case, by just writing to already-allocated strings, we can get
a 4x speedup by just changing `pig_latinnify` to:

```Rust
#[polars_expr(output_type=String)]
fn pig_latinnify(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let out: StringChunked = ca.apply_to_buffer(|value, output|
        if let Some(first_char) = value.chars().next() {
            write!(output, "{}{}ay", &value[1..], first_char).unwrap()
        }
    );
    Ok(out.into_series())
}
```
Make sure to also add
```Rust
use std::fmt::Write;
```
to the top of the file.

Thinking about allocations can really make a difference!
