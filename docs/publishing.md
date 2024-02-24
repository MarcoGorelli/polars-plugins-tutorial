# 13. Publishing your plugin to PyPI and becoming famous

Here are the steps you should follow:

1. publish plugin to PyPI
2. ???
3. profit

This section deals with step 1, and assumes your project live on GitHub.

## Set up trusted publishing

First set up an account on Pypi.org, can't do much without that.

Then, add a file `.github/workflows/publish_to_pypi.yml` to your repo with the following
content:
```yaml
name: CI

on:
  push:
    branches:
      - main
    tags:
      - '*'
  pull_request:
  workflow_dispatch:

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

permissions:
  contents: read

# Make sure CI fails on all warnings, including Clippy lints
env:
  RUSTFLAGS: "-Dwarnings"

jobs:
  linux_tests:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        target: [x86_64]
        python-version: ["3.8", "3.9", "3.10", "3.11"]
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: ${{ matrix.python-version }}

      - name: Set up Rust
        run: rustup show
      - uses: mozilla-actions/sccache-action@v0.0.3
      - run: python3 -m venv venv
      - run: cargo fmt --all && cargo clippy --all-features
      - run: maturin develop
      - run: pytest

  linux:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        target: [x86_64, x86, aarch64, armv7]
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - name: Build wheels
        uses: PyO3/maturin-action@v1
        with:
          target: ${{ matrix.target }}
          args: --release --out dist --find-interpreter
          sccache: 'true'
          manylinux: auto
      - name: Upload wheels
        uses: actions/upload-artifact@v3
        with:
          name: wheels
          path: dist

  windows:
    runs-on: windows-latest
    strategy:
      matrix:
        target: [x64, x86]
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
          architecture: ${{ matrix.target }}
      - name: Build wheels
        uses: PyO3/maturin-action@v1
        with:
          target: ${{ matrix.target }}
          args: --release --out dist --find-interpreter
          sccache: 'true'
      - name: Upload wheels
        uses: actions/upload-artifact@v3
        with:
          name: wheels
          path: dist

  macos:
    runs-on: macos-latest
    strategy:
      matrix:
        target: [x86_64, aarch64]
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - name: Build wheels
        uses: PyO3/maturin-action@v1
        with:
          target: ${{ matrix.target }}
          args: --release --out dist --find-interpreter
          sccache: 'true'
      - name: Upload wheels
        uses: actions/upload-artifact@v3
        with:
          name: wheels
          path: dist

  sdist:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Build sdist
        uses: PyO3/maturin-action@v1
        with:
          command: sdist
          args: --out dist
      - name: Upload sdist
        uses: actions/upload-artifact@v3
        with:
          name: wheels
          path: dist

  release:
    name: Release
    if: "startsWith(github.ref, 'refs/tags/')"
    needs: [linux, windows, macos, sdist]
    runs-on: ubuntu-latest
    environment: pypi
    permissions:
      id-token: write  # IMPORTANT: mandatory for trusted publishing
    steps:
      - uses: actions/download-artifact@v3
        with:
          name: wheels
      - name: Publish to PyPI
        uses: PyO3/maturin-action@v1
        with:
          command: upload
          args: --non-interactive --skip-existing *
```

See https://www.maturin.rs/distribution.html?highlight=publish#using-pypis-trusted-publishing
for some more details on this.

Then, on PyPI, you'll want to (note: this is taken almost verbatim from [PyPA](https://packaging.python.org/en/latest/guides/publishing-package-distribution-releases-using-github-actions-ci-cd-workflows/#configuring-trusted-publishing)):

1. Go to https://pypi.org/manage/account/publishing/.
2. Fill in the name you wish to publish your new PyPI project under (the name value in your pyproject.toml), the GitHub repository owner’s name (org or user), and repository name, and the name of the release workflow file under the .github/ folder, see Creating a workflow definition. Finally, add the name of the GitHub Environment (pypi) we’re going set up under your repository. Register the trusted publisher.

Finally, if you make a commit and tag it, and then push, then a release should be triggered! It will then be
available for install across different platforms, which would be really hard (impossible?) to do if you were building
the wheel manually and uploading to PyPI yourself.
