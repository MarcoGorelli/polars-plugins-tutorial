# 2. How to do ABSolutely nothing

OK, the title's misleading. We won't do "nothing", we'll make an `abs` function
which will work on numeric data.

We'll do this in phases:

- `abs_i64` will take the absolute value of each row of an `Int64` column
- `abs_numeric` will the absolute value of each row in any numeric column

## `abs_i64`

Let's start with the Python side - this is almost the same as what
we did for `noop`, we'll just change the names:
```python
def abs_i64(self) -> pl.Expr:
    return self._expr.register_plugin(
        lib=lib,
        symbol="abs_i64",
        is_elementwise=True,
    )
```

Next, let's define `abs_i64` on the Rust side. The general idea will
be:

- A Series is backed by a ChunkedArray. Each chunk is an Arrow Array
  which is continuous in memory. So we're going to start by iterating
  over chunks.
- For each chunk, we iterate over the elements in that array.
- Each element can either be `Some(i64)`, or `None`. If it's `None`,
  we return `None`, whereas if it's `Some(i64)`, then we take its
  absolute value.
- We produce a new ChunkedArray, convert it to Series, and return it.

In code:

```Rust
#[polars_expr(output_type=Int64)]
fn abs_i64(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.i64()?;
    let chunks = ca.downcast_iter().map(|arr| {
        arr.into_iter()
            .map(|opt_v| opt_v.map(|v| v.abs()))
            .collect()
    });
    let out = Int64Chunked::from_chunk_iter(ca.name(), chunks);
    Ok(out.into_series())
}
```

NOTE: there are faster ways of implementing this particular operation. If you
look at the Polars source code, you'll see that it's a bit different there.
The purpose of this exercise is to show you an implementation which is
explicit and generalisable enough that you can customise it according to your
needs, whilst probably being performant enough for most cases.
This is already orders of magnitude faster than `.map_elements`...

## `abs_numeric`

The code above unfortunately only supports `Int64` columns. Let's try to
generalise it a bit, so that it can accept any numeric column.

First, add a `abs_numeric` function to your `minimal_plugin/__init__.py` file.
It should be just like `abs_i64` but with
a different name.

Then, let's go back to `src/expressions.rs` and try to implement it.

Let's start off by just copy-and-pasting
`abs_i64`, but we'll factor part of it out
into a helper function:

```Rust
fn impl_abs_numeric(ca: &Int64Chunked) -> Int64Chunked {
    let chunks = ca.downcast_iter().map(|arr| {
        arr.into_iter()
            .map(|opt_v| opt_v.map(|v| v.abs()))
            .collect()
    });
    Int64Chunked::from_chunk_iter(ca.name(), chunks)
}

#[polars_expr(output_type=Int64)]
fn abs_numeric(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.i64()?;
    let out = impl_abs_numeric(ca);
    Ok(out.into_series())
}
```

Next, we need to make `impl_abs_numeric` generic over
numeric types. We can do that using ? - read
[here](https://doc.rust-lang.org/book/ch10-00-generics.html) for more info.
```Rust
fn impl_abs_numeric<T>(ca: &ChunkedArray<T>) -> ChunkedArray<T>
where
    T: PolarsNumericType,
    T::Native: Signed,
{
    let chunks = ca.downcast_iter().map(|arr| {
        arr.into_iter()
            .map(|opt_v| opt_v.map(|v| v.abs()))
            .collect()
    });
    ChunkedArray::<T>::from_chunk_iter(ca.name(), chunks)
}
```
Make sure to add
```Rust
use pyo3_polars::export::polars_core::export::num::Signed;
```
to the top of the `src/expression.rs` file.

Finally, we can accept more than just `i64` - any (signed)
numeric type will do!

```Rust
fn abs_numeric(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    match s.dtype() {
        DataType::Int32 => Ok(impl_abs_numeric(s.i32().unwrap()).into_series()),
        DataType::Int64 => Ok(impl_abs_numeric(s.i64().unwrap()).into_series()),
        DataType::Float32 => Ok(impl_abs_numeric(s.f32().unwrap()).into_series()),
        DataType::Float64 => Ok(impl_abs_numeric(s.f64().unwrap()).into_series()),
        dtype => polars_bail!(InvalidOperation:format!("dtype {dtype} not supported for abs_numeric, expected Int32, Int64, Float32, Float64.")),
    }
}
```
