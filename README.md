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
