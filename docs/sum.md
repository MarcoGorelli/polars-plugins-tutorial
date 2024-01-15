# 3. How to do SUMthing

So far, the expressions we wrote only operated on a single expression.

What if we'd like to do something fancier, involving more than one expression?
Let's try to write an expression which lets us do
```python
df.with_columns(
    pl.col('a').mp.sum_i64(pl.col('b'))
)
```

## Take a ride on the Python side

First, we need to be able to pass multiple inputs to our Rust function. We'll do that
by using the `args` argument when we register our expression. Add the following to
`minimal_plugins/__init__.py`:

```python
def sum_i64(self, other: IntoExpr) -> pl.Expr:
    return self._expr.register_plugin(
        lib=lib,
        symbol="sum_i64",
        is_elementwise=True,
        args=[other]
    )
```

## I’ve got 1100011 problems but binary ain't one

Polars gives us a handy `binary_elementwise` function for computing elementwise operations
involving multiple columns. Here's how we can implement `sum_i64`:

```Rust
#[polars_expr(output_type=Int64)]
fn sum_i64(inputs: &[Series]) -> PolarsResult<Series> {
    let left = inputs[0].i64()?;
    let right = inputs[1].i64()?;
    let out: Int64Chunked = binary_elementwise(left, right, |left, right| match (left, right) {
        (Some(left), Some(right)) => Some(left + right),
        _ => None,
    });
    Ok(out.into_series())
}
```
Note that you'll need to add
```Rust
use polars::prelude::arity::binary_elementwise;
```
to the top of the `src/expressions.rs` file.

The idea is:
- for each row, if both `left` and `right` are valid, then we some them;
- if either of them is missing (`None`), then we return `None`.

Can you write a Python script which uses this new `.mp.sum_i64` function
you just wrote?

## Get over your exercises

It's widely acknowledged that the best way to learn is by doing.

Try making some crazy expressions of your own. Try making a `sum_numeric` function.
Try tapping into 3-rd party Rust crates. Go crazy - and if you're ever stuck, rembember:
the "Polars-Plugins" channel of the Polars Discord server is always there for you!
