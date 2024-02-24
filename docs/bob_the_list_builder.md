# 12. Bob the (list) builder

Chapter 9 ([Lists at last]) was fun. Let's do it all over again!

Or rather, let's do another list operation. We're going to start with
a dataframe such as

```python
shape: (4, 1)
┌──────────────┐
│ dense        │
│ ---          │
│ list[i64]    │
╞══════════════╡
│ [0, 9]       │
│ [8, 6, 0, 9] │
│ null         │
│ [3, 3]       │
└──────────────┘
```
and we're going to try to count the indices which are non-zero.

!!! note

    You don't really need a plugin to do this, you can just do

    ```python
    df.with_columns(sparse_indices=pl.col('dense').list.eval(pl.arg_where(pl.element() != 0)))
    ```

    But `eval` won't cover every need you ever have ever, so...it's good
    to learn how to do this as a plugin so you can then customize it according to your needs.

Polars has a helper function built-in for dealing with this: `apply_amortized`. We can use it to apply
a function to each element of a List Series. In this case, we just want to find the indices of non-zero
elements, so we'll do:

```rust
fn list_idx_dtype(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = Field::new(input_fields[0].name(), DataType::List(Box::new(IDX_DTYPE)));
    Ok(field.clone())
}

#[polars_expr(output_type_func=list_idx_dtype)]
fn non_zero_indices(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].list()?;

    let out: ListChunked = ca.apply_amortized(|s| {
        let s: &Series = s.as_ref();
        let ca: &Int64Chunked = s.i64().unwrap();
        let mut out: Vec<Option<u32>> = Vec::with_capacity(ca.len());
        for (idx, element) in ca.into_iter().enumerate() {
            match element {
                Some(0) => (),
                Some(_) => out.push(Some(idx as IdxSize)),
                None => (),
            }
        }
        let out: IdxCa = out.into_iter().collect_ca("");
        out.into_series()
    });
    Ok(out.into_series())
}
```
`apply_amortized` is a bit like the `apply_to_buffer` function we used in `pig_latinnify`,
in that it makes a big allocation upfront to amortize the allocation costs.

  [Lists at last]: ../lists/