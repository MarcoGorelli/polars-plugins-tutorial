# 9.0 Weighted-mean watchers

According to [one YouTube talk](https://youtu.be/u5mIDz5ldmI?si=4AtnyyAwdVk33bYu),
the `list` namespace is one of Polars' main selling points.
If you're also a fan of it, this section will teach you how to extend it even further.

## Motivation

Say you have
```python
In [10]: df = pl.DataFrame({
    ...:     'values': [[1, 3, 2], [5, 7]],
    ...:     'weights': [[.5, .3, .2], [.1, .9]]
    ...: })

In [11]: df
Out[11]:
shape: (2, 2)
┌───────────┬─────────────────┐
│ values    ┆ weights         │
│ ---       ┆ ---             │
│ list[i64] ┆ list[f64]       │
╞═══════════╪═════════════════╡
│ [1, 3, 2] ┆ [0.5, 0.3, 0.2] │
│ [5, 7]    ┆ [0.1, 0.9]      │
└───────────┴─────────────────┘
```

Can you calculate the mean of the values in `'values'`, weighted by the values in `'weights'`?

So:

- `.5*1 + .3*3 + .2*2 = 1.8`
- `5*.1 + 7*.9 = 6.8`

I don't know of an easy way to do this with Polars expressions. There probably is a way - but
as you'll see here, it's not that hard to write a plugin, and it's probably faster too.

## Weighted mean

On the Python side, this'll be similar to `sum_i64`:

```python
def weighted_mean(expr: IntoExpr, weights: IntoExpr) -> pl.Expr:
    return register_plugin(
        args=[expr, weights],
        lib=lib,
        symbol="weighted_mean",
        is_elementwise=True,
    )
```

On the Rust side, we'll define a helper function which will let us work with
pairs of list chunked arrays:

```rust
fn binary_amortized_elementwise<'a, T, K, F>(
    lhs: &'a ListChunked,
    rhs: &'a ListChunked,
    mut f: F,
) -> ChunkedArray<T>
where
    T: PolarsDataType,
    T::Array: ArrayFromIter<Option<K>>,
    F: FnMut(&AmortSeries, &AmortSeries) -> Option<K> + Copy,
{
    {
        let (lhs, rhs) = align_chunks_binary(lhs, rhs);
        lhs.amortized_iter()
            .zip(rhs.amortized_iter())
            .map(|(lhs, rhs)| match (lhs, rhs) {
                (Some(lhs), Some(rhs)) => f(&lhs, &rhs),
                _ => None,
            })
            .collect_ca(PlSmallStr::EMPTY)
    }
}
```

That's a bit of a mouthful, so let's try to make sense of it.

- As we learned about in [Prerequisites], Polars Series are backed by chunked arrays.
  `align_chunks_binary` just ensures that the chunks have the same lengths. It may need
  to rechunk under the hood for us;
- `amortized_iter` returns an iterator of `AmortSeries`, each of which corresponds
  to a row from our input.

We'll explain more about `AmortSeries` in a future iteration of this tutorial.
For now, let's just look at how to use this utility:

- we pass it `ListChunked` as inputs;
- we also pass a function which takes two `AmortSeries` and produces a scalar
  value.

```rust
#[polars_expr(output_type=Float64)]
fn weighted_mean(inputs: &[Series]) -> PolarsResult<Series> {
    let values = inputs[0].list()?;
    let weights = &inputs[1].list()?;
    polars_ensure!(
        values.dtype() == &DataType::List(Box::new(DataType::Int64)),
        ComputeError: "Expected `values` to be of type `List(Int64)`, got: {}", values.dtype()
    );
    polars_ensure!(
        weights.dtype() == &DataType::List(Box::new(DataType::Float64)),
        ComputeError: "Expected `weights` to be of type `List(Float64)`, got: {}", weights.dtype()
    );

    let out: Float64Chunked = binary_amortized_elementwise(
        values,
        weights,
        |values_inner: &AmortSeries, weights_inner: &AmortSeries| -> Option<f64> {
            let values_inner = values_inner.as_ref().i64().unwrap();
            let weights_inner = weights_inner.as_ref().f64().unwrap();
            if values_inner.len() == 0 {
                // Mirror Polars, and return None for empty mean.
                return None
            }
            let mut numerator: f64 = 0.;
            let mut denominator: f64 = 0.;
            values_inner
                .iter()
                .zip(weights_inner.iter())
                .for_each(|(v, w)| {
                    if let (Some(v), Some(w)) = (v, w) {
                        numerator += v as f64 * w;
                        denominator += w;
                    }
                });
            Some(numerator / denominator)
        },
    );
    Ok(out.into_series())
}
```

If you just need to get a problem solved, this function works! But let's note its
limitations:

- it assumes that each inner element of `values` and `weights` has the same
  length - it would be better to raise an error if this assumption is not met
- it only accepts `Int64` `values` and `Float64` `weights`
  (see section 2 for how you could make it more generic).

To try it out, we compile with `maturin develop` (or `maturin develop --release` if you're 
benchmarking), and then we should be able to run `run.py`:

```python
import polars as pl
import minimal_plugin as mp

df = pl.DataFrame({
    'values': [[1, 3, 2], [5, 7]],
    'weights': [[.5, .3, .2], [.1, .9]]
})
print(df.with_columns(weighted_mean = mp.weighted_mean('values', 'weights')))
```
to see
```
shape: (2, 3)
┌───────────┬─────────────────┬───────────────┐
│ values    ┆ weights         ┆ weighted_mean │
│ ---       ┆ ---             ┆ ---           │
│ list[i64] ┆ list[f64]       ┆ f64           │
╞═══════════╪═════════════════╪═══════════════╡
│ [1, 3, 2] ┆ [0.5, 0.3, 0.2] ┆ 1.8           │
│ [5, 7]    ┆ [0.1, 0.9]      ┆ 6.8           │
└───────────┴─────────────────┴───────────────┘
```

  [Prerequisites]: ../prerequisites/

## Gimme ~~chocolate~~ challenge

Could you implement a weighted standard deviation calculator?
