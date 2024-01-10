# 1. `noop`: how to do nothing

That's right - this section is about how to do _nothing_.

We'll write a Polars plugin which takes an expression, and returns it exactly
as it is. Nothing more, nothing less. This will just be an exercise in checking
that we have set everything up correctly!

Here are the files we'll need to create:

- `Cargo.toml`: file with Rust dependencies
- `pyproject.toml`: file with Python build info
- `requirements.txt`: Python dependencies. At a minimum, you'll need:
- `minimal_plugin`: your Python package
- `src`: directory with your blazingly fast (of course) Rust code.

## Cargo.toml

Start by creating a `Cargo.toml` file containing the following:

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