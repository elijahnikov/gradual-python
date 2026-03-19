# Gradual Python SDK

[![CI](https://github.com/elijahnikov/gradual-python/actions/workflows/ci.yml/badge.svg)](https://github.com/elijahnikov/gradual-python/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/gradual-sdk)](https://pypi.org/project/gradual-sdk/)
[![Python versions](https://img.shields.io/pypi/pyversions/gradual-sdk)](https://pypi.org/project/gradual-sdk/)

The official Python SDK for [Gradual](https://gradual.dev) feature flags.

## Installation

```bash
pip install gradual-sdk
```

## Quick Start

```python
from gradual import create_gradual, GradualOptions

client = create_gradual(GradualOptions(
    api_key="gra_xxx",
    environment="production",
))
client.wait_until_ready()

if client.is_enabled("new-feature"):
    # new code path
    pass

theme = client.get("theme", fallback="dark")

client.identify({"user": {"id": "user-123", "plan": "pro"}})
client.close()
```

## API Reference

- **`create_gradual(options: GradualOptions)`** -- Create and return a new Gradual client instance.
- **`client.wait_until_ready()`** -- Block until the client has fetched its initial flag configuration.
- **`client.is_enabled(flag: str) -> bool`** -- Check whether a boolean feature flag is enabled.
- **`client.get(flag: str, fallback=None)`** -- Get the value of a feature flag, returning `fallback` if the flag is not found.
- **`client.identify(context: dict)`** -- Set the evaluation context (e.g. user attributes) for subsequent flag checks.
- **`client.close()`** -- Shut down the client and release resources.

## Development

Install the package in development mode with test dependencies:

```bash
pip install -e ".[dev]"
```

Run the tests:

```bash
PYTHONPATH=src pytest tests/ -v
```

## License

MIT
