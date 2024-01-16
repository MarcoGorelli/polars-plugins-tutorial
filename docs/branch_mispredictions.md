# 7. Branch mispredictions

Time to go back to the past. In Section 2, I told you that the
implementation we had of `abs_i64` wasn't the most efficient one
you could possibly write. Time to see how to improve it!

Before, our algorithm was:

- for each row, check whether the value is `None` or not
- only if it's not `None`, then take its absolute value

However, `.abs` is a very fast operation. In this case, we can get
a nearly 3x speedup by doing something seemingly worse:

- for each row, compute the absolute value, regardless of
  whether the value's valid
- put the validities back at the end. If a value was invalid to  
  begin with, it'll be invalid at the end as well.

Here's how you can make `abs_i64` faster:

```rust
#[polars_expr(output_type=Int64)]
fn abs_i64(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.i64()?;
    let chunks = ca
        .downcast_iter()
        .map(|arr| arr.values().as_slice())
        .zip(ca.iter_validities())
        .map(|(slice, validity)| {
            let arr: Int64Array = slice.iter().copied().map(|x| x.abs()).collect_arr();
            arr.with_validity(validity.cloned())
        });
    let out = Int64Chunked::from_chunk_iter(ca.name(), chunks);
    Ok(out.into_series())
}
```

This kind of operation is so common in Polars that there's a convenience method
for it: `apply_values`.
Using that, the code becomes:
```Rust
#[polars_expr(output_type=Int64)]
fn abs_i64(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.i64()?;
    let out = ca.apply_values(|x| x.abs());
    Ok(out.into_series())
}
```

## Practice!

Can you go back and make a faster version of `sum_i64`?
