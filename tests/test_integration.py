"""Integration tests for the Gradual Python SDK.

Run with: pytest -m integration -v
"""

import pytest

from gradual import GradualOptions, create_gradual


@pytest.mark.integration
def test_get_flag_returns_expected_variation():
    client = create_gradual(
        GradualOptions(
            api_key="grdl_x5Hf17fFpgjkUykTZa7bGerkZsCd-OJX",
            environment="testing",
        )
    )
    try:
        client.wait_until_ready(timeout=10)
        value = client.get("testing", fallback=None)
        assert value == "variation-value-2"
    finally:
        client.close()
