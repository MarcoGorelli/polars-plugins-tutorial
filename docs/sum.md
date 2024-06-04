# 3. How to do SUMthing

So far, the expressions we wrote only operated on a single expression.

What if we'd like to do something fancier, involving more than one expression?
Let's try to write an expression which lets us do

```python
df.with_columns(mp.sum_i64('a', 'b'))
```

## Take a ride on the Python side

First, we need to be able to pass multiple inputs to our Rust function. We'll do that
by using the `args` argument when we register our expression. Add the following to
`minimal_plugins/__init__.py`:

```python
def sum_i64(expr: IntoExpr, other: IntoExpr) -> pl.Expr:
    return register_plugin(
        args=[expr, other],
        lib=lib,
        symbol="sum_i64",
        is_elementwise=True,
    )
```

## I’ve got 1100011 problems but binary ain't one

Time to write a binary function, in the sense that it takes two
columns as input and produces a third.
Polars gives us a handy `binary_elementwise` function for computing binary elementwise operations
called `binary_elementwise`.

Add the following to `src/expressions.rs`:

```Rust
#[polars_expr(output_type=Int64)]
fn sum_i64(inputs: &[Series]) -> PolarsResult<Series> {
    let left: &Int64Chunked = inputs[0].i64()?;
    let right: &Int64Chunked = inputs[1].i64()?;
    // Note: there's a faster way of summing two columns, see
    // section 7.
    let out: Int64Chunked = binary_elementwise(
        left,
        right,
        |left: Option<i64>, right: Option<i64>| match (left, right) {
            (Some(left), Some(right)) => Some(left + right),
            _ => None,
        },
    );
    Ok(out.into_series())
}
```
Note that you'll also need to add
```Rust
use polars::prelude::arity::binary_elementwise;
```
to the top of the `src/expressions.rs` file.

!!! note

    There's a faster way of implementing this particular operation,
    which we'll cover later in the tutorial in [Branch mispredictions].

The idea is:

- for each row, if both `left` and `right` are valid (i.e. they are both
  `Some`), then we sum them;
- if either of them is missing (`None`), then we return `None`.

To try it out, remember to first compile with `maturin develop`
(or `maturin develop --release` if you're benchmarking). Then
if you make a `run.py` file with
```python
import polars as pl
import minimal_plugin as mp

df = pl.DataFrame({'a': [1, 5, 2], 'b': [3, None, -1]})
print(df.with_columns(a_plus_b=mp.sum_i64('a', 'b')))
```
then `python run.py` should produce
```
shape: (3, 3)
┌─────┬──────┬──────────┐
│ a   ┆ b    ┆ a_plus_b │
│ --- ┆ ---  ┆ ---      │
│ i64 ┆ i64  ┆ i64      │
╞═════╪══════╪══════════╡
│ 1   ┆ 3    ┆ 4        │
│ 5   ┆ null ┆ null     │
│ 2   ┆ -1   ┆ 1        │
└─────┴──────┴──────────┘
```

  [Branch mispredictions]: ../branch_mispredictions/

## Get over your exercises

It's widely acknowledged that the best way to learn is by doing.

Can you make `sum_numeric` (a generic version of `sum_i64`)?
Can you support the case when `left` and `right` are of different
types, e.g. `i8` plus `i16`?
