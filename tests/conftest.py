"""Shared test fixtures and helpers for conformance testing."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from gradual._types import (
    SnapshotFlag,
    SnapshotIndividualEntry,
    SnapshotRollout,
    SnapshotRolloutVariation,
    SnapshotRuleCondition,
    SnapshotScheduleStep,
    SnapshotSegment,
    SnapshotTarget,
    SnapshotVariation,
)

# Path to the conformance test fixtures
SPEC_DIR = Path(__file__).resolve().parent.parent.parent / "gradual-sdk-spec"
TESTDATA_DIR = SPEC_DIR / "testdata"
EVALUATOR_DIR = TESTDATA_DIR / "evaluator"
HASH_DIR = TESTDATA_DIR / "hash"


def load_fixture(filename: str) -> dict[str, Any]:
    """Load a JSON fixture file from the evaluator testdata directory."""
    path = EVALUATOR_DIR / filename
    with open(path) as f:
        return json.load(f)


def load_hash_vectors() -> list[dict[str, Any]]:
    """Load hash vector test data."""
    path = HASH_DIR / "hash-vectors.json"
    with open(path) as f:
        data = json.load(f)
    return data["vectors"]


def parse_condition(c: dict[str, Any]) -> SnapshotRuleCondition:
    return SnapshotRuleCondition(
        context_kind=c["contextKind"],
        attribute_key=c["attributeKey"],
        operator=c["operator"],
        value=c["value"],
    )


def parse_rollout(r: dict[str, Any]) -> SnapshotRollout:
    schedule = None
    if r.get("schedule"):
        schedule = [
            SnapshotScheduleStep(
                duration_minutes=s["durationMinutes"],
                variations=[
                    SnapshotRolloutVariation(
                        variation_key=v["variationKey"], weight=v["weight"]
                    )
                    for v in s["variations"]
                ],
            )
            for s in r["schedule"]
        ]
    return SnapshotRollout(
        variations=[
            SnapshotRolloutVariation(
                variation_key=v["variationKey"], weight=v["weight"]
            )
            for v in r["variations"]
        ],
        bucket_context_kind=r["bucketContextKind"],
        bucket_attribute_key=r["bucketAttributeKey"],
        seed=r.get("seed"),
        schedule=schedule,
        started_at=r.get("startedAt"),
    )


def parse_flag(data: dict[str, Any]) -> SnapshotFlag:
    """Parse a raw JSON flag dict into a SnapshotFlag."""
    variations: dict[str, SnapshotVariation] = {}
    for vk, vv in data.get("variations", {}).items():
        variations[vk] = SnapshotVariation(key=vv["key"], value=vv["value"])

    targets: list[SnapshotTarget] = []
    for tv in data.get("targets", []):
        rollout = parse_rollout(tv["rollout"]) if tv.get("rollout") else None
        conditions = (
            [parse_condition(c) for c in tv["conditions"]]
            if tv.get("conditions")
            else None
        )
        targets.append(
            SnapshotTarget(
                type=tv["type"],
                sort_order=tv["sortOrder"],
                id=tv.get("id"),
                name=tv.get("name"),
                variation_key=tv.get("variationKey"),
                rollout=rollout,
                conditions=conditions,
                context_kind=tv.get("contextKind"),
                attribute_key=tv.get("attributeKey"),
                attribute_value=tv.get("attributeValue"),
                segment_key=tv.get("segmentKey"),
            )
        )

    default_rollout = (
        parse_rollout(data["defaultRollout"]) if data.get("defaultRollout") else None
    )

    return SnapshotFlag(
        key=data["key"],
        type=data["type"],
        enabled=data["enabled"],
        variations=variations,
        off_variation_key=data["offVariationKey"],
        targets=targets,
        default_variation_key=data.get("defaultVariationKey"),
        default_rollout=default_rollout,
    )


def parse_segments(data: dict[str, Any]) -> dict[str, SnapshotSegment]:
    """Parse raw JSON segments dict."""
    segments: dict[str, SnapshotSegment] = {}
    for sk, sv in data.items():
        segments[sk] = SnapshotSegment(
            key=sv["key"],
            conditions=[parse_condition(c) for c in sv.get("conditions", [])],
            included=[
                SnapshotIndividualEntry(
                    context_kind=e["contextKind"],
                    attribute_key=e["attributeKey"],
                    attribute_value=e["attributeValue"],
                )
                for e in sv.get("included", [])
            ],
            excluded=[
                SnapshotIndividualEntry(
                    context_kind=e["contextKind"],
                    attribute_key=e["attributeKey"],
                    attribute_value=e["attributeValue"],
                )
                for e in sv.get("excluded", [])
            ],
        )
    return segments
