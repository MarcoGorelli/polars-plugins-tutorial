# 1. How to do nothing

That's right - this section is about how to do _nothing_.

We'll write a Polars plugin which takes an expression, and returns it exactly
as it is. Nothing more, nothing less. This will just be an exercise in setting
everything up!

If you followed the instructions in [Prerequisites], then your working directory
should look a bit like the following:
```
.
├── Cargo.toml
├── minimal_plugin
│   ├── __init__.py
│   └── utils.py
├── pyproject.toml
└── src
    ├── expressions.rs
    ├── lib.rs
    └── utils.rs
```

  [Prerequisites]: ../prerequisites/

## The Python side

Let's start by getting the Python side ready. It won't run until we
implement the Rust side too, but it's a necessary step.
Start by adding the following to `minimal_plugin/__init__.py`:

```python
def noop(expr: IntoExpr) -> pl.Expr:
    return register_plugin(
        args=[expr],
        lib=lib,
        symbol="noop",
        is_elementwise=True,
    )
```
Let's go through this line-by-line:

- when we compile Rust, it generates a Shared Object file.
  The `lib` variable holds its filepath;
- We'll cover `is_elementwise` in [Yes we SCAN], for now don't pay attention to it;
- We use the utility function `register_plugin`, provided to us by the cookiecutter.
  Polars actually has a public [register_plugin_function](https://docs.pola.rs/py-polars/html/reference/plugins.html#polars.plugins.register_plugin_function) utility for this, but it was only introduced in
  Polars 0.20.16. The `register_plugin` function we introduce here handles backwards-compatibility
  until Polars 0.20.6, so we use that in this tutorial.

Note that string literals are parsed as expressions, so that if somebody
calls `noop('a')`, it gets interpreted as `noop(pl.col('a'))`.

  [Yes we SCAN]: ../cum_sum/

## Let's get Rusty

Let's leave `src/lib.rs` as it is, and add the following to `src/expressions.rs`:

``` rust
fn same_output_type(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = &input_fields[0];
    Ok(field.clone())
}

#[polars_expr(output_type_func=same_output_type)]
fn noop(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    Ok(s.clone())
} 
```

There's a lot to cover here so we'll break it down below.

### Defining `noop`'s schema

Polars needs to know the schema/dtypes resulting from operations to make good
optimization decisions. The way we tell Polars what to expect from our custom
function is with the `polars_expr` attribute.

Our beautiful `noop` doesn't change the data type (in fact, it doesn't change anything...)
so we'll just write a function which returns the same input type:

```Rust
fn same_output_type(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = &input_fields[0];
    Ok(field.clone())
}
```
and use that to define the function output's schema. Just like
`noop`, this function takes a reference to its only input and
clones it.

### Defining `noop`'s body

The input is an iterable of `Series`. In our case, `noop` just
receives a single Series as input, but as we'll see in later
sections, it's possible to pass multiple Series.

We said we wanted our function to do nothing, so let's implement
that: take a reference to the first (and only) input Series,
and return a (cheap!) clone of it.

## Putting it all together

Right, does this all work? Let's make a Python file `run.py` with which to
test it out. We'll just make a toy dataframe and apply `noop`
to each column!
```python
import polars as pl
import minimal_plugin as mp

df = pl.DataFrame({
    'a': [1, 1, None],
    'b': [4.1, 5.2, 6.3],
    'c': ['hello', 'everybody!', '!']
})
print(df.with_columns(mp.noop(pl.all()).name.suffix('_noop')))
```

Your working directory should now look a bit like this:

```
.
├── Cargo.toml
├── minimal_plugin
│   ├── __init__.py
│   └── utils.py
├── pyproject.toml
├── run.py
└── src
    ├── expressions.rs
    ├── lib.rs
    └── utils.rs
```

Let's compile! Please run `maturin develop` (or `maturin develop --release` if benchmarking).
You'll need to do this every time you change any of your Rust code.
It may take a while the first time, but subsequent executions will
be significantly faster as the build process is incremental.

Finally, you can run your code! If you run `python run.py` and get
the following output:
```
shape: (3, 6)
┌──────┬─────┬────────────┬────────┬────────┬────────────┐
│ a    ┆ b   ┆ c          ┆ a_noop ┆ b_noop ┆ c_noop     │
│ ---  ┆ --- ┆ ---        ┆ ---    ┆ ---    ┆ ---        │
│ i64  ┆ f64 ┆ str        ┆ i64    ┆ f64    ┆ str        │
╞══════╪═════╪════════════╪════════╪════════╪════════════╡
│ 1    ┆ 4.1 ┆ hello      ┆ 1      ┆ 4.1    ┆ hello      │
│ 1    ┆ 5.2 ┆ everybody! ┆ 1      ┆ 5.2    ┆ everybody! │
│ null ┆ 6.3 ┆ !          ┆ null   ┆ 6.3    ┆ !          │
└──────┴─────┴────────────┴────────┴────────┴────────────┘
```
then it means everything worked correctly. Congrats!

You're now ready to learn how to do ABSolutely nothing.
