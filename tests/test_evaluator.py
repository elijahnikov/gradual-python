"""Evaluator conformance tests loaded from gradual-sdk-spec fixtures."""

from __future__ import annotations

import glob
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from conftest import EVALUATOR_DIR, load_fixture, parse_flag, parse_segments

from gradual._evaluator import evaluate_flag


def _collect_test_cases() -> list[tuple[str, dict[str, Any]]]:
    """Collect all test cases from all fixture files."""
    cases = []
    for filepath in sorted(EVALUATOR_DIR.glob("*.json")):
        data = json.loads(filepath.read_text())
        for test in data["tests"]:
            test_id = f"{filepath.stem}::{test['name']}"
            cases.append((test_id, test))
    return cases


_all_cases = _collect_test_cases()


@pytest.mark.parametrize(
    "test_id,test_case",
    _all_cases,
    ids=[c[0] for c in _all_cases],
)
def test_conformance(test_id: str, test_case: dict[str, Any]) -> None:
    """Run a single conformance test case."""
    flag = parse_flag(test_case["flag"])
    context = test_case.get("context", {})
    segments = parse_segments(test_case.get("segments", {}))

    kwargs: dict[str, Any] = {}
    options = test_case.get("options", {})
    if "now" in options:
        kwargs["now"] = datetime.fromisoformat(
            options["now"].replace("Z", "+00:00")
        )

    result = evaluate_flag(flag, context, segments, **kwargs)

    expected = test_case["expected"]

    # Check value
    if "value" in expected:
        assert result.value == expected["value"], (
            f"value mismatch: got {result.value!r}, expected {expected['value']!r}"
        )

    # Check variationKey
    if "variationKey" in expected:
        assert result.variation_key == expected["variationKey"], (
            f"variationKey mismatch: got {result.variation_key!r}, "
            f"expected {expected['variationKey']!r}"
        )

    # Check reasons
    if "reasons" in expected:
        actual_reasons = [r.to_dict() for r in result.reasons]
        expected_reasons = expected["reasons"]

        # For exact match tests (disabled flags, defaults, etc.)
        # we check containment for flexibility
        for exp_reason in expected_reasons:
            matched = False
            for act_reason in actual_reasons:
                if _reason_matches(act_reason, exp_reason):
                    matched = True
                    break
            assert matched, (
                f"Expected reason {exp_reason!r} not found in {actual_reasons!r}"
            )

    # Check matchedTargetName
    if "matchedTargetName" in expected:
        assert result.matched_target_name == expected["matchedTargetName"]

    # Check reasonType (shorthand for checking the first reason's type)
    if "reasonType" in expected:
        assert len(result.reasons) > 0
        actual_types = [r.type for r in result.reasons]
        assert expected["reasonType"] in actual_types, (
            f"Expected reason type {expected['reasonType']!r} not in {actual_types!r}"
        )

    # Check stepIndex in gradual rollout reasons
    if "stepIndex" in expected:
        gradual_reasons = [
            r for r in result.reasons if r.type == "gradual_rollout"
        ]
        assert len(gradual_reasons) > 0, "Expected gradual_rollout reason"
        assert gradual_reasons[0].step_index == expected["stepIndex"]

    # Check valueOneOf (for rollout tests where value is non-deterministic
    # from the test's perspective)
    if "valueOneOf" in expected:
        assert result.value in expected["valueOneOf"], (
            f"value {result.value!r} not in {expected['valueOneOf']!r}"
        )


def _reason_matches(actual: dict[str, Any], expected: dict[str, Any]) -> bool:
    """Check if an actual reason matches an expected reason (subset match)."""
    for key, value in expected.items():
        if key not in actual:
            return False
        if actual[key] != value:
            return False
    return True
