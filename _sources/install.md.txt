(installation)=

# Installation

The only pre-requisite to install r2x-plexos is [Python 3.11](https://www.python.org/downloads/release/python-3110/) or greater.

## Python version support

Python 3.11, 3.12, 3.13.

## Installation options

R2X PLEXOS is available to install on [PyPI](https://pypi.org/project/r2x-plexos/) and can be installed using any python package manager of your preference, but we recommend using [uv](https://docs.astral.sh/uv/getting-started/installation/).

### Installation with uv

```console
# Install as a tool
uv tool install r2x-plexos

# Or add to a project
uv add r2x-plexos
```

### Installation with pip

```console
# Install system-wide
pip install r2x-plexos

# Or in a virtual environment
python -m pip install r2x-plexos
```

## Upgrading options

### Upgrading with uv

```console
uv pip install --upgrade r2x-plexos
```

### Upgrading with pip

```console
python -m pip install --upgrade r2x-plexos
```

## Verify installation

Check that R2X PLEXOS is installed correctly:

```python
import r2x_plexos
print(f"R2X PLEXOS version: {r2x_plexos.__version__}")
```

## Next steps

- [Tutorials](tutorials/index.md) - Step-by-step learning guides
- [How-To Guides](how-tos/index.md) - Task-focused recipes
- [API Reference](references/index.md) - Complete API documentation
