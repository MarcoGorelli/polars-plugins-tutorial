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

Let's go over these in turn.

## Cargo.toml

This'll need to be something like
```Rust
[package]
// Name of the project goes here
// Note - it should be the same as the folder which you store
// your code in!
name = "minimal_plugin"
version = "0.1.0"
edition = "2021"

[lib]
name = "minimal_plugin"
crate-type= ["cdylib"]

[dependencies]
pyo3 = { version = "0.20.0", features = ["extension-module"] }
pyo3-polars = { version = "0.10.0", features = ["derive"] }
serde = { version = "1", features = ["derive"] }
// If you need any fancy features from the Polars Rust crate, list
// them in here, e.g.
// polars = { version = "0.36.2", features = ["dtype-date"], default-features = false }
polars = { version = "0.36.2", default-features = false }
// You may also need other Polars crates, e.g.
// polars-time = { version = "0.36.2", features = ["timezones"], default-features = false }

[target.'cfg(target_os = "linux")'.dependencies]
jemallocator = { version = "0.5", features = ["disable_initial_exec_tls"] }
```