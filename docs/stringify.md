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
                Cow::Borrowed(value)
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
    return register_plugin(
        args=[expr],
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

Yes! `StringChunked` has a utility `apply_into_string_amortized` method which amortises
the cost of creating new strings for each row by creating a string upfront,
clearing it, and repeatedly writing to it.
This gives a 4x speedup! All you need to do is change `pig_latinnify` to:

```Rust
#[polars_expr(output_type=String)]
fn pig_latinnify(inputs: &[Series]) -> PolarsResult<Series> {
    let ca: &StringChunked = inputs[0].str()?;
    let out: StringChunked = ca.apply_into_string_amortized(|value: &str, output: &mut String| {
        if let Some(first_char) = value.chars().next() {
            write!(output, "{}{}ay", &value[1..], first_char).unwrap()
        }
    });
    Ok(out.into_series())
}
```

Simpler, faster, and more memory-efficient.
_Thinking about allocations_ can really make a difference!

## So let's think about allocations!

### Choosing a method to apply a custom function

Consider a user defined function (_that produces a  `String`_) to be applied to the elements of an input Series, e.g., append a suffix, convert to lowercase, etc. To apply that function, it's very likely your choice will be one of:

- `apply_values`
- `apply_to_buffer`
- `apply_to_buffer_generic`

All of them would probably work, as they're all essentially just mapping values. However, by choosing blindly, you might be missing some performance gains. To determine the most efficient method to be used:  
1. Ask yourself: does the applied function allocate memory? If the answer is __yes__, the difference is negligible, use whichever method you prefer.  
2. If it's not allocating, ask yourself again: is the function simply slicing the input? If so, `apply_values` with `Cow::Borrowed` should be the winner:  
```rust
fn remove_last_extension(s: &str) -> &str {
    match s.rfind('.') {
        Some(pos) => &s[..pos],
        None => s,
    }
}

#[polars_expr(output_type=String)]
fn remove_extension(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.str()?;
    let out: StringChunked = ca.apply_values(|val| {
        let res = Cow::Borrowed(remove_last_extension(val));
        res
    });
    Ok(out.into_series())
}
```
3. Otherwise we only need one more piece of information: if the function operates __on a `String`__ to produce a `String`, `apply_to_buffer` should be preferred. An example for that is the `pig_latinnify` from the previous section.
4. Otherwise, if it's any other PolarsDataType ("Any") to `String`, you'll want `apply_to_string_amortized`. Yes, it sounds like the input should be a `String`, but that's referring to the output. Here's an example:
```rust
// example once the PR is merged
// ref:
// - https://github.com/pola-rs/polars/pull/17670
```


Here's a cheatsheet (remember, the output type is always String):

| Allocates | Only slices   | Input type | What to use?                          |
|-----------|---------------|------------|---------------------------------------|
| Yes       | N/A           | N/A        | Whatever you prefer                   |
| No        | Yes           | String     | `apply_values` with `Cow::Borrowed`   |
| No        | No            | String     | `apply_to_buffer`                     |
| No        | No            | Any¹       | `apply_to_buffer_generic`             |

¹ Any `PolarsDataType`
