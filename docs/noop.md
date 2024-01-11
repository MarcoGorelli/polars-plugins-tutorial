# 1. How to do nothing

That's right - this section is about how to do _nothing_.

We'll write a Polars plugin which takes an expression, and returns it exactly
as it is. Nothing more, nothing less. This will just be an exercise in setting
everything up!

Here are the files we'll need to create:

- `Cargo.toml`: file with Rust dependencies
- `pyproject.toml`: file with Python build info
- `requirements.txt`: Python build dependencies

Start by copying the `Cargo.toml` and `pyproject.toml`
files from this repository - they contain the
bare minimum you'll need to get started.

We'll also need to create the following directories:

- `minimal_plugin`: your Python package
- `src`: directory with your blazingly fast (of course) Rust code.

## The Python side

Let's start by getting the Python side ready. It won't run until we
implement the Rust side too, but it's a necessary step.
Start by making a `minimal_plugin/__init__.py` file with the
following content:

``` py
import polars as pl
from polars.utils.udfs import _get_shared_lib_location
from polars.type_aliases import IntoExpr

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

- when we'll compile Rust, we'll generate a Shared Object file.
  The `lib` variable will hold its filepath - you don't need to
  do anything here, just let Polars' `_get_shared_lib_location`
  figure it out.
- Polars has several namespaces which group together related
  functionality: `.list`, `.str`, `.dt`, and more. We'll add one
  more, `.mp`, to which we'll add functionality from our minimal
  plugin. This is what `register_expr_namespace` does.
- Within the `.mp` namespace, we'll define our amazing do-nothing
  function: `noop`, using the `register_plugin` function.
  We'll need to pass the following arguments:
  
  - `lib`: location of Shared Object file.
  - `symbol`: name of the Rust function we wish to call. We
    haven't written any Rust yet, but let's put `noop` here
    for now, and we'll implement the `noop` function in Rust
    later.
  - `is_elementwise`: this affects how our function works in
    a group-by context. If your function operates independently
    for each row, then set this to `True`. Else, if your function
    needs to consider the whole column, set this to `False`.
    Incorrectly setting this to `True` could result in wrong results
    in a group-by operation! In our case, `noop` will look at each
    row and just return its value as-is, without considering the
    values of other rows in the column. Therefore, we set
    `is_elementwise=True`.

That's it!

## Let's get Rusty

We'll need to make two files:

- `lib.rs`: list any Rust modules we want to use. We'll
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

- `expressions.rs`: this is where we'll define `noop`
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

That last file looks a bit complex, so let's make sense of it.

### Defining `noop`

Pay no attention to `polars_expr` and `output_type_func`, we'll
get to them below. Let's start with making sense of `noop`!

The input is an iterable of `Series`. In our case, `noop` just
receives a single Series as input, but as we'll see in later
sections, it's possible to pass multiple Series.

We said we wanted our function to do nothing, so let's implement
that: take a reference to the first (and only) input Series,
and return a (cheap!) clone of it.

### Defining `noop`'s schema

If you only ever intend to use your plugin in eager mode, then you might not care about this. You _could_ just write

```Rust
  #[polars_expr(output_type=Int64)]
```
, forget about `fn same_output_type`, and be done with it, you code
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

Note that you can't run this file just yet - we need to compile
our code first! Please run
```
maturin develop
```
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
