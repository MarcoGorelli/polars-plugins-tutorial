
SHELL=/bin/bash

install:
	unset CONDA_PREFIX && \
	source .venv/bin/activate && maturin develop -m Cargo.toml

install-release:
	unset CONDA_PREFIX && \
	source .venv/bin/activate && maturin develop --release -m Cargo.toml

pre-commit:
	rustup component add rustfmt clippy --toolchain nightly-2025-05-21
	cargo fmt --all --manifest-path Cargo.toml && cargo clippy --all-features --manifest-path Cargo.toml
	.venv/bin/python -m ruff format minimal_plugin test_plugin.py
	.venv/bin/python -m ruff check minimal_plugin test_plugin.py

run: install
	source .venv/bin/activate && python run.py

run-release: install-release
	source .venv/bin/activate && python run.py

test:
	source .venv/bin/activate && pytest test_plugin.py
