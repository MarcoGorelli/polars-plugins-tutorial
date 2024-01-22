# 2. How to do ABSolutely nothing

OK, the title's misleading. We won't do "nothing", we'll make an `abs` function
which will work on numeric data.

We'll do this in phases:

- `abs_i64` will take the absolute value of each row of an `Int64` column
- `abs_numeric` will the absolute value of each row in any numeric column

## `abs_i64`

Let's start with the Python side - this is almost the same as what
we did for `noop`, we'll just change the names. Please add this to
`minimal_plugin/__init__.py`, right below the definition of `noop`:
```python
def abs_i64(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="abs_i64",
        is_elementwise=True,
    )
```

Then, please add this to `src/expressions.rs`, right below the Rust
definition of `noop`:

```Rust
#[polars_expr(output_type=Int64)]
fn abs_i64(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca: &Int64Chunked = s.i64()?;
    // NOTE: there's a faster way of implementing `abs_i64`, which we'll
    // cover in section 7.
    let out: Int64Chunked = ca.apply(|opt_v: Option<i64>| opt_v.map(|v: i64| v.abs()));
    Ok(out.into_series())
}
```

The general idea here is:

- Each element `opt_v` can either be `Some(i64)`, or `None`.
  If it's `None`, we return `None`, whereas if it's `Some(i64)`,
  then we return `Some` of the absolute value of the `i64` value.

    !!!note

        There's a faster way of implementing `abs_i64`, which you'll learn
        about in section 7.

- We produce a new ChunkedArray, convert it to Series, and return it.

Let's try this out. Make a Python file `run.py` with the following:
```python
import polars as pl
import minimal_plugin as mp

df = pl.DataFrame({
    'a': [1, -1, None],
    'b': [4.1, 5.2, -6.3],
    'c': ['hello', 'everybody!', '!']
})
print(df.with_columns(mp.abs_i64('a').name.suffix('_abs')))
```
Compile it with `maturin develop` (or `maturin develop --release` if you're benchmarking), and run it with `python run.py`.
If it outputs
```
shape: (3, 4)
┌──────┬──────┬────────────┬───────┐
│ a    ┆ b    ┆ c          ┆ a_abs │
│ ---  ┆ ---  ┆ ---        ┆ ---   │
│ i64  ┆ f64  ┆ str        ┆ i64   │
╞══════╪══════╪════════════╪═══════╡
│ 1    ┆ 4.1  ┆ hello      ┆ 1     │
│ -1   ┆ 5.2  ┆ everybody! ┆ 1     │
│ null ┆ -6.3 ┆ !          ┆ null  │
└──────┴──────┴────────────┴───────┘
```
then you did everything correctly!

## `abs_numeric`

The code above unfortunately only supports `Int64` columns. Let's try to
generalise it a bit, so that it can accept any signed numeric column.

First, add the following definition to `minimal_plugin/__init__.py`:

```python
def abs_numeric(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="abs_numeric",
        is_elementwise=True,
    )
```

Then, we'll go back to `src/expressions.rs`.
Paste in the following:

```Rust
fn impl_abs_numeric(ca: &Int64Chunked) -> Int64Chunked {
    // NOTE: there's a faster way of implementing `abs`, which we'll
    // cover in section 7.
    ca.apply(|opt_v: Option<i64>| opt_v.map(|v: i64| v.abs()))
}

#[polars_expr(output_type=Int64)]
fn abs_numeric(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca: &Int64Chunked = s.i64()?;
    let out = impl_abs_numeric(ca);
    Ok(out.into_series())
}
```

Note how it's exactly like `abs_i64`, but `impl_abs_numeric` was
factored out of the `abs_numeric` function. It's not yet generic,
we need to do a bit more work.
The general idea is:

- each `ChunkedArray` is of some Polars Type `T` (e.g. `Int64`);
- to each Polars Type `T`, there corresponds a Rust native type `T::Native` (e.g. `i64`).

Change `impl_abs_numeric` to:

```Rust
fn impl_abs_numeric<T>(ca: &ChunkedArray<T>) -> ChunkedArray<T>
where
    T: PolarsNumericType,
    T::Native: Signed,
{
    // NOTE: there's a faster way of implementing `abs`, which we'll
    // cover in section 7.
    ca.apply(|opt_v: Option<T::Native>| opt_v.map(|v: T::Native| v.abs()))
}
```
Make sure to add
```Rust
use pyo3_polars::export::polars_core::export::num::Signed;
```
to the top of the `src/expression.rs` file.

We then need to modify `abs_numeric` as follows:
```Rust
#[polars_expr(output_type_func=same_output_type)]
fn abs_numeric(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    match s.dtype() {
        DataType::Int32 => Ok(impl_abs_numeric(s.i32().unwrap()).into_series()),
        DataType::Int64 => Ok(impl_abs_numeric(s.i64().unwrap()).into_series()),
        DataType::Float32 => Ok(impl_abs_numeric(s.f32().unwrap()).into_series()),
        DataType::Float64 => Ok(impl_abs_numeric(s.f64().unwrap()).into_series()),
        dtype => {
            polars_bail!(InvalidOperation:format!("dtype {dtype} not \
            supported for abs_numeric, expected Int32, Int64, Float32, Float64."))
        }
    }
}
```
That's it! Our function is now generic over signed numeric types,
instead of only accepting the `Int64` type.

Finally, modify the `print` line of `run.py` to be
```python
print(df.with_columns(mp.abs_numeric(pl.col('a', 'b')).name.suffix('_abs')))
```

Compile with `maturin develop` (or `maturin develop --release`
if you're benchmarking) and then run with `python run.py`. You should
see:
```
shape: (3, 5)
┌──────┬──────┬────────────┬───────┬───────┐
│ a    ┆ b    ┆ c          ┆ a_abs ┆ b_abs │
│ ---  ┆ ---  ┆ ---        ┆ ---   ┆ ---   │
│ i64  ┆ f64  ┆ str        ┆ i64   ┆ f64   │
╞══════╪══════╪════════════╪═══════╪═══════╡
│ 1    ┆ 4.1  ┆ hello      ┆ 1     ┆ 4.1   │
│ -1   ┆ 5.2  ┆ everybody! ┆ 1     ┆ 5.2   │
│ null ┆ -6.3 ┆ !          ┆ null  ┆ 6.3   │
└──────┴──────┴────────────┴───────┴───────┘
```
Note how we were able to take the absolute value of both `b` (`f64`)
and `a` (`i64`) columns with `abs_numeric`!
