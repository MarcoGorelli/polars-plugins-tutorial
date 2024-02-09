# 5. How to STRING something together

Tired of examples which only include numeric data? Me neither.
But we need to address the elephant in the room: strings.

We're going to start by re-implementing a pig-latinnifier.
This example is already part of the `pyo3-polars` repo examples,
but we'll tackle it with a different spin here by first doing it
the wrong way 😈.

## Pig-latinnify - take 1

Let's start by doing this the wrong way.
We'll use our `abs` example, and adapt it to the
string case. We'll follow the same strategy:

- iterate over arrow arrays;
- for each element in each array, create a new output value.

Put the following in `src/expressions.rs`:

```Rust
use std::borrow::Cow;
use std::fmt::Write;

#[polars_expr(output_type=String)]
fn pig_latinnify(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.str()?;
    let out: StringChunked = ca.apply(|opt_v: Option<&str>| {
        opt_v.map(|value: &str| {
            // Not the recommended way to do it,
            // see below for a better way!
            if let Some(first_char) = value.chars().next() {
                Cow::Owned(format!("{}{}ay", &value[1..], first_char))
            } else {
                Cow::Owned(value.to_string())
            }
        })
    });
    Ok(out.into_series())
}
```
If you're not familiar with [clone-on-write](https://doc.rust-lang.org/std/borrow/enum.Cow.html),
don't worry about it - we're about to see a simpler and better way to do this anyway.
What I'd like you to focus on is that for every row, we're creating a new `String`.

If you combine this with a Python definition (which you should put
in `minimal_plugin/__init__.py`):

```python
def pig_latinnify(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return expr.register_plugin(
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
import minimal_plugin as mp

df = pl.DataFrame({'a': ["I", "love", "pig", "latin"]})
print(df.with_columns(a_pig_latin=mp.pig_latinnify('a')))
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

This will already be an order of magnitude faster than using `map_elements`.
But as mentioned earlier, we're creating a new string for every single row.

Can we do better?

## Pig-latinnify - take 2

Yes! `StringChunked` has a utility `apply_to_buffer` method which amortises
the cost of creating new strings for each row by creating a string upfront,
clearing it, and repeatedly writing to it.
This gives a 4x speedup! All you need to do is change `pig_latinnify` to:

```Rust
#[polars_expr(output_type=String)]
fn pig_latinnify(inputs: &[Series]) -> PolarsResult<Series> {
    let ca: &StringChunked = inputs[0].str()?;
    let out: StringChunked = ca.apply_to_buffer(|value: &str, output: &mut String| {
        if let Some(first_char) = value.chars().next() {
            write!(output, "{}{}ay", &value[1..], first_char).unwrap()
        }
    });
    Ok(out.into_series())
}
```

Simpler, faster, and more memory-efficient.
Thinking about allocations can really make a difference!
