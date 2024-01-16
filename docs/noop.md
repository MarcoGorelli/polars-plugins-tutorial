# 1. How to do nothing

That's right - this section is about how to do _nothing_.

We'll write a Polars plugin which takes an expression, and returns it exactly
as it is. Nothing more, nothing less. This will just be an exercise in setting
everything up!

Here are the files we'll need to create:

- `Cargo.toml`: file with Rust dependencies.

    ```toml
    [package]
    # Name of the project goes here
    # Note - it should be the same as the folder which you store your code in!
    name = "minimal_plugin"
    version = "0.1.0"
    edition = "2021"

    [lib]
    # Name of the project goes here
    # Note - it should be the same as the folder which you store your code in!
    name = "minimal_plugin"
    crate-type= ["cdylib"]

    [dependencies]
    pyo3 = { version = "0.20.0", features = ["extension-module"] }
    pyo3-polars = { version = "0.10.0", features = ["derive"] }
    serde = { version = "1", features = ["derive"] }
    polars = { version = "0.36.2", default-features = false }

    [target.'cfg(target_os = "linux")'.dependencies]
    jemallocator = { version = "0.5", features = ["disable_initial_exec_tls"] } 
    ```

- `pyproject.toml`: file with Python build info.
    ```toml
    [build-system]
    requires = ["maturin>=1.0,<2.0"]
    build-backend = "maturin"

    [project]
    name = "minimal_plugin"  # Should match the folder with your code!
    requires-python = ">=3.8"
    classifiers = [
      "Programming Language :: Rust",
      "Programming Language :: Python :: Implementation :: CPython",
      "Programming Language :: Python :: Implementation :: PyPy",
    ]
    ```

We'll also need to create the following directories:

- `minimal_plugin`: your Python package
- `src`: directory with your blazingly fast (of course) Rust code.

Your working directory should contain at least the following:
```
.
├── Cargo.toml
├── minimal_plugin
├── pyproject.toml
└── src
```

## The Python side

Let's start by getting the Python side ready. It won't run until we
implement the Rust side too, but it's a necessary step.
Start by making a `minimal_plugin/__init__.py` file with the
following content:

``` py
import polars as pl
from polars.utils.udfs import _get_shared_lib_location

lib = _get_shared_lib_location(__file__)


@pl.api.register_expr_namespace("mp")
class MinimalExamples:
    def __init__(self, expr: pl.Expr):
        self._expr = expr

    def noop(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=lib,
            symbol="noop",
            is_elementwise=True,
        )
```
Let's go through this line-by-line:

- when we compile Rust, it generates a Shared Object file.
  The `lib` variable holds its filepath;
- Polars has several namespaces which group together related
  functionality: `.list`, `.str`, `.dt`, and more. We'll register one
  more, `.mp`, to which we'll add functionality from our minimal
  plugin;
- Within the `.mp` namespace, we'll register our amazing do-nothing
  function `noop`. For now, don't pay attention to `is_elementwise`,
  we'll get back to that later.

## Let's get Rusty

We'll need to make two files:

- `src/lib.rs`: list any Rust modules we want to use. We'll
  put `expressions` here (which we define in the next bullet point).
  
    ```Rust
    mod expressions;

    #[cfg(target_os = "linux")]
    use jemallocator::Jemalloc;

    #[global_allocator]
    #[cfg(target_os = "linux")]
    static ALLOC: Jemalloc = Jemalloc; 
    ```
      
    You can ignore the `jemallocator` part - think of it as some
    boilerplate in order to get high-performance memory allocation.
    Your plugin would work just fine without it.

- `src/expressions.rs`: this is where we'll define `noop`
    ``` rust
    #![allow(clippy::unused_unit)]
    use polars::prelude::*;
    use pyo3_polars::derive::polars_expr;

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

If you only ever intend to use your plugin in eager mode, then you might not care about this. You _could_ just write

```Rust
  #[polars_expr(output_type=Int64)]
```
then forget about `fn same_output_type` and be done with it, you code
will work just fine. However, just because you don't need lazy
execution now doesn't mean you won't need it later as you scale or
productionise, so let's take steps to get it right.

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
import minimal_plugin  # noqa: F401

df = pl.DataFrame({
    'a': [1, 1, None],
    'b': [4.1, 5.2, 6.3],
    'c': ['hello', 'everybody!', '!']
})
print(df.with_columns(pl.all().mp.noop().name.suffix('_noop')))
```

Your file tree should look a bit like this:

```
.
├── Cargo.toml
├── minimal_plugin
│   └── __init__.py
├── pyproject.toml
├── run.py
├── src
│   ├── expressions.rs
│   └── lib.rs
```

Let's compile! Please run `maturin develop` (or `maturin develop --release` if benchmarking).
You'll need to do this everytime you change any of your Rust code.
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
