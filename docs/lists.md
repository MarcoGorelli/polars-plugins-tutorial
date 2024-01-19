# 9. Lists at last

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
    def weighted_mean(self, weights: IntoExpr) -> pl.Expr:
        return self._expr.register_plugin(
            lib=lib,
            symbol="weighted_mean",
            is_elementwise=True,
            args=[weights]
        )
```

On the Rust side, this is where things get a bit scary. I don't know of any existing easy
convenience function to do this, so I'll provide you with one here:

```rust
fn binary_amortized_elementwise_float<'a, F>(
    ca: &'a ListChunked,
    weights: &'a ListChunked,
    mut f: F,
) -> Float64Chunked
where
    F: FnMut(&Series, &Series) -> Option<f64>,
{
    unsafe {
        ca.amortized_iter()
            .zip(weights.amortized_iter())
            .map(|(lhs, rhs)| match (lhs, rhs) {
                (Some(lhs), Some(rhs)) => f(lhs.as_ref(), rhs.as_ref()),
                _ => None,
            })
            .collect_ca(ca.name())
    }
}
```
Some of its details (such as `.as_ref()` to get a `Series` out of an `UnstableSeries`) are arguably
implementation details. Hopefully a more generic version of this utility like this can be added to
Polars itself, so that plugin authors won't need to worry about it. But for now, let's just use it to 
implement a weighted mean.

We're just going to accept two inputs (values and weights), multiply them together, and divide by
the sum of the weights:

```rust
#[polars_expr(output_type=Float64)]
fn weighted_mean(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].list()?;
    let weights = &inputs[1].list()?;

    let out = binary_amortized_elementwise_float(ca, weights, |values, weights| {
        let values = values.i64().unwrap();
        let weights = weights.f64().unwrap();
        let out_inner: Float64Chunked = binary_elementwise(
            values,
            weights,
            |lhs: Option<i64>, rhs: Option<f64>| match (lhs, rhs) {
                (Some(lhs), Some(rhs)) => Some(lhs as f64 * rhs),
                _ => None,
            },
        );
        match (out_inner.sum(), weights.sum()) {
            (Some(sum), Some(weights_sum)) => Some(sum / weights_sum),
            _ => None,
        }
    });
    Ok(out.into_series())
}
```
This version only accepts `Int64` values - see section 2 for how you could make it more
generic.

## Gimme challenge

Could you implement a weighted standard deviation calculator?
