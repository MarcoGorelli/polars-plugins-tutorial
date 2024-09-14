# 10. STRUCTin'

> "Day one, I'm in love with your struct" Thumpasaurus (kinda)

How do we consume structs, and how do we return them?

To learn about structs, we'll rewrite a plugin which takes a `Struct` as
input, and shifts all values forwards by one key. So, for example, if
the input was `{'a': 1, 'b': 2., 'c': '3'}`, then the output will be
`{'a': 2., 'b': '3', 'c': 1}`.

On the Python side, usual business:

```python
def shift_struct(expr: IntoExpr) -> pl.Expr:
    return register_plugin(
        args=[expr],
        lib=lib,
        symbol="shift_struct",
        is_elementwise=True,
    )
```

On the Rust side, we need to start by activating the necessary
feature - in `Cargo.toml`, please make this change:

```diff
-polars = { version = "0.43.1", default-features = false }
+polars = { version = "0.43.1", features=["dtype-struct"], default-features = false }
```

Then, we need to get the schema right.

```Rust
fn shifted_struct(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = &input_fields[0];
    match field.dtype() {
        DataType::Struct(fields) => {
            let mut field_0 = fields[0].clone();
            field_0.set_name(fields[fields.len() - 1].name().clone());
            let mut fields = fields[1..]
                .iter()
                .zip(fields[0..fields.len() - 1].iter())
                .map(|(fld, name)| Field::new(name.name().clone(), fld.dtype().clone()))
                .collect::<Vec<_>>();
            fields.push(field_0);
            Ok(Field::new(PlSmallStr::EMPTY, DataType::Struct(fields)))
        }
        _ => unreachable!(),
    }
}
```

In this case, I put the first field's name as the output struct's name, but it doesn't
really matter what we put, as Polars doesn't allow us to rename expressions within
plugins. You can always rename on the Python side if you really want to, but I'd suggest
to just let Polars follow its usual "left-hand-rule".

The function definition is going to follow a similar logic:

```rust
#[polars_expr(output_type_func=shifted_struct)]
fn shift_struct(inputs: &[Series]) -> PolarsResult<Series> {
    let struct_ = inputs[0].struct_()?;
    let fields = struct_.fields_as_series();
    if fields.is_empty() {
        return Ok(inputs[0].clone());
    }
    let mut field_0 = fields[0].clone();
    field_0.rename(fields[fields.len() - 1].name()).clone();
    let mut fields = fields[1..]
        .iter()
        .zip(fields[..fields.len() - 1].iter())
        .map(|(s, name)| {
            let mut s = s.clone();
            s.rename(name.name().clone());
            s
        })
        .collect::<Vec<_>>();
    fields.push(field_0);
    StructChunked::from_series(PlSmallStr::EMPTY, &fields).map(|ca| ca.into_series())
}
```

Let's try this out. Put the following in `run.py`:

```python
import polars as pl
import minimal_plugin as mp

df = pl.DataFrame(
    {
        "a": [1, 3, 8],
        "b": [2.0, 3.1, 2.5],
        "c": ["3", "7", "3"],
    }
).select(abc=pl.struct("a", "b", "c"))
print(df.with_columns(abc_shifted=mp.shift_struct("abc")))
```

Compile with `maturin develop` (or `maturin develop --release` if you're
benchmarking), and if you run `python run.py` you'll see:

```
shape: (3, 2)
┌─────────────┬─────────────┐
│ abc         ┆ abc_shifted │
│ ---         ┆ ---         │
│ struct[3]   ┆ struct[3]   │
╞═════════════╪═════════════╡
│ {1,2.0,"3"} ┆ {2.0,"3",1} │
│ {3,3.1,"7"} ┆ {3.1,"7",3} │
│ {8,2.5,"3"} ┆ {2.5,"3",8} │
└─────────────┴─────────────┘
```

The values look right - but is the schema?
Let's take a look

```
import pprint
pprint.pprint(df.with_columns(abc_shifted=mp.shift_struct("abc")).schema)
```

```
OrderedDict([('abc', Struct({'a': Int64, 'b': Float64, 'c': String})),
             ('abc_shifted', Struct({'a': Float64, 'b': String, 'c': Int64}))])
```

Looks correct!
