# 15. In (the) aggregate

Enough transorming columns! Let's aggregate them instead.

A Polars expression is a function from a Dataframe to a Series. So,
how can we possibly write an expression which produces a scalar?

Simple:

- write an expression which returns a 1-row Series
- when you register the expression, pass `returns_scalar = True`

As an example, let's compute the weighted mean of a column, where
the weights are given by a second column.

## Hello Python my old friend

Nothing fancy here:

```python
def vertical_weighted_mean(values: IntoExpr, weights: IntoExpr) -> pl.Expr:
    return register_plugin_function(
        args=[values, weights],
        plugin_path=LIB,
        function_name="vertical_weighted_mean",
        is_elementwise=False,
        returns_scalar=True,
    )
```

## Rust

To keep this example's complexity down, let's just limit it to `Float64` columns.

```rust
#[polars_expr(output_type=Float64)]
fn vertical_weighted_mean(inputs: &[Series]) -> PolarsResult<Series> {
    let values = &inputs[0].f64()?;
    let weights = &inputs[1].f64()?;
    let mut numerator = 0.;
    let mut denominator = 0.;
    values.iter().zip(weights.iter()).for_each(|(v, w)| {
        if let (Some(v), Some(w)) = (v, w) {
            numerator += v * w;
            denominator += w;
        }
    });
    let result = numerator / denominator;
    Ok(Series::new("", vec![result]))
}
```

## Run it!

Put the following in `run.py`:

```python
df = pl.DataFrame({
    'values': [1., 3, 2, 5, 7],
    'weights': [.5, .3, .2, .1, .9],
    'group': ['a', 'a', 'a', 'b', 'b'],
})
print(df.group_by('group').agg(weighted_mean = mp.vertical_weighted_mean('values', 'weights')))
```

If you compile with `maturin develop` (or `maturin develop --release` if benchmarking), you'll see:

```
shape: (2, 2)
┌───────┬───────────────┐
│ group ┆ weighted_mean │
│ ---   ┆ ---           │
│ str   ┆ f64           │
╞═══════╪═══════════════╡
│ b     ┆ 6.166667      │
│ a     ┆ 2.333333      │
└───────┴───────────────┘
```

Try omitting `returns_scalar=True` when registering the expression - what changes?
