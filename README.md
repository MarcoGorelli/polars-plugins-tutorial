# polars-plugins-minimal-examples

Minimal examples of Polars Plugins

## The simplest Polars plugin

Let's make a plugin which take column, and just returns it.

All you need is:
- `Cargo.toml`: file with Rust dependencies.
  Ideally, put the latest Polars and pyo3-polars versions.
- `pyproject.toml`: file with Python build info
- `requirements.txt`: Python dependencies. At a minimum, you'll need:
  - Polars
  - maturin
- `my_plugin`: directory with Python `__init__.py` file to
  register your custom namespace
- `src`: directory with your blazingly fast (of course) Rust code
