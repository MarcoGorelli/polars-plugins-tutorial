import subprocess
import sys
import tomllib
import re
from typing import Any


# Remote Cargo.toml from which we extract the version of 'polars'
remote_cargo_url = r"https://raw.githubusercontent.com/MarcoGorelli/cookiecutter-polars-plugins/main/%7B%7B%20cookiecutter.project_slug%20%7D%7D/Cargo.toml"
# Packages that should have the same version as the 'polars' package from above
pinned_packages = ["polars", "polars-arrow", "polars-core"]


def fetch_url_content(url: str) -> str | None:
    try:
        res = subprocess.run(
            ["curl", "-s", url],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        return res.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error fetching URL: {e.stderr}")
        return None


# Fetch contents of remote (template) Cargo.toml
raw_content = fetch_url_content(remote_cargo_url)
if not raw_content:
    print("Fetched template Cargo.toml is empty, try again")
    sys.exit(1)

# Load it as a dict with tomllib
try:
    template_cargo_toml = tomllib.loads(raw_content)
except tomllib.TOMLDecodeError as e:
    print(f"Error decoding template Cargo.toml: {e}")
    sys.exit(1)

# Store remote (template) polars version
template_polars_version: str = template_cargo_toml["dependencies"]["polars"]["version"]

# Load local Cargo.toml as a dict
local_cargo_toml: dict[str, Any]
with open("Cargo.toml", mode="rb") as local_cargo_toml_file:
    try:
        local_cargo_toml = tomllib.load(local_cargo_toml_file)
    except tomllib.TOMLDecodeError as e:
        print(f"Error decoding local Cargo.toml: {e}")
        sys.exit(1)

# Check each local pkg that should be pinned with the same ver. as the remote
for pkg in pinned_packages:
    version = local_cargo_toml["dependencies"][pkg]["version"]
    if version != template_polars_version:
        print(f"{pkg=} {version=} does not match {template_polars_version=}")
        sys.exit(1)


# Additionally, check other locations that reference polars version
def find_local_polars_reference() -> str | None:
    """
    This will output a string with the format:
    <filename><zero-byte><content><newline>
    <filename><zero-byte><content><newline>
    ...
    """
    try:
        # grep -rEZ '^[+-]?polars = ' .
        res = subprocess.run(
            ["grep", "-rEZ", r"^[+-]?polars = ", "."],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        return res.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error running `grep -rEZ '^[+-]?polars = ' .`")
        return None


grep_result = find_local_polars_reference()
if not grep_result:
    print("Error running grep, try again")

# Iterate each non-empty line of the grep result
for line in [ln for ln in grep_result.split("\n") if ln.strip()]:
    # File name and line contents are separated with a null byte
    filename, line = line.split("\0")

    # Use a group to capture the exact version present in the line
    m = re.search(r'\bversion = "([^"]+)"', line)
    if not m:
        print(f"Error extracting version from package in {filename}: {line}")
        sys.exit(1)
    if (ver := m.group(1)) != template_polars_version:
        print(
            f"Error in {filename}: local {ver=} does not"
            f"match {template_polars_version=}: {line=}"
        )
        sys.exit(1)
