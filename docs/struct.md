# Cissy STRUCT

!!! note

    "Cissy Strut" is a 1969 funk instrumental by The Meters.
    Any political connotations you may be imagining are purely
    coincidental.

Let's address the struct in the room - how do we consume structs, and how
do we return them?

To learn about structs, we'll rewrite a plugin which takes a `Struct` as
input, and shifts all values forwards by one key. So, for example, if
the input was `{'a': 1, 'b': 2, 'c': 3}`, then the output will be
`{'a': 2, 'b': 3, 'c': 1}`.

On the Python side, usual business:

```python
    def shift_struct(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=lib,
            symbol="shift_struct",
            is_elementwise=True,
        )
```

On the Rust side, we need to first unpack the fields of the struct,
rename them, and then construct a new struct.

```rust
#[polars_expr(output_type_func=same_output_type)]
fn shift_struct(inputs: &[Series]) -> PolarsResult<Series> {
    let struct_ = inputs[0].struct_()?;
    let fields = struct_.fields();
    if fields.is_empty() {
        return Ok(inputs[0].clone());
    }
    let mut field_0 = fields[0].clone();
    field_0.rename(fields[fields.len() - 1].name());
    let mut fields = fields[1..]
        .iter()
        .zip(fields[..fields.len() - 1].iter())
        .map(|(s, name)| {
            let mut s = s.clone();
            s.rename(name.name());
            s
        })
        .collect::<Vec<_>>();
    fields.push(field_0);
    StructChunked::new(struct_.name(), &fields).map(|ca| ca.into_series())
}
```
