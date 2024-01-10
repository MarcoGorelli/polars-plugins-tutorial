# polars-plugins-minimal-examples

Minimal examples of Polars Plugins

## Pre-requisites

All you need is:
- `Cargo.toml`: file with Rust dependencies.
  As a minimum, you'll need:
  - pyo3
  - pyo3-polars
  - polars
- `pyproject.toml`: file with Python build info
- `requirements.txt`: Python dependencies. At a minimum, you'll need:
  - Polars
  - maturin
- `my_plugin`: this is your Python module.
  You should include an `__init__.py` file in which you register your namespace.
- `src`: directory with your blazingly fast (of course) Rust code.
  It's recommended to structure it as follows:
  - a `lib.rs` file, in which you list which modules (from the same directory) you want to use
  - an `expressions.rs` file, in which you just register your expressions
  - other files in which you define the logic in your functions. For example,
    here, `expressions.rs` defines `fn add` with `#[polars_expr(...)]`, but the logic for
    add is in `add.rs`.

## The most minimal plugin - the `noop`

Check `src/expressions.rs` for the most minimal function you can possibly write: `noop`.
It doesn't do anything, it just returns its input. But it still allows us to go over some things.

In `__init__.py`, we registered it as:
```python
def noop(self) -> pl.Expr:
    return self._expr.register_plugin(
        lib=lib,
        symbol="noop",
        is_elementwise=True,
    )
```
The first two arguments aren't too surprising:
- `lib` is just the shared library location. This is the location of the `.so` file (`.pyd` if you're on
  Windows) which your plugin generates. You shouldn't need to worry about this, the
  `_get_shared_lib_location` function from Polars should take care of it for you.
- `symbol`: this is the name of the Rust function you wish to call. In this case, it's the function
  `noop` from `src/expressions.rs`, so we put `symbol='noop'`.
- `is_elementwise` is a bit trickier to understand, and not actually crucial for this example. If your function
  really operates row-by-row, and each row is independent of the previous one, then this won't make any difference
  to the correctness of your results, though setting `is_elementwise=True` will be more performant.
  See the `cum_sum` section below for an example of where `is_elementwise` actually makes a difference.

Let's look at the function definition:
```Rust
#[polars_expr(output_type_func=same_output_type)]
fn noop(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    Ok(s.clone())
}
```

There's a few components we need to understand.

### output_type_func

If you only ever intend to use your plugin in eager mode, then you might not care about this one.
On the other hand, if you envision wanting to use it in lazy mode, then this will ensure that
the schema is always correct.
It's up to you to ensure that this is correct.

In this case, `noop` doesn't change the data type (in fact, it doesn't change anything...)
so we'll just write a function which returns the same input type:

```Rust
fn same_output_type(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = &input_fields[0];
    Ok(field.clone())
}
```
