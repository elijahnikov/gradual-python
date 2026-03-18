"""Type definitions for the Gradual Python SDK."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


TargetingOperator = Literal[
    "equals",
    "not_equals",
    "contains",
    "not_contains",
    "starts_with",
    "ends_with",
    "greater_than",
    "less_than",
    "greater_than_or_equal",
    "less_than_or_equal",
    "in",
    "not_in",
    "exists",
    "not_exists",
]

EvaluationContext = dict[str, dict[str, Any]]
"""Context keyed by context kind (e.g. 'user', 'company') with arbitrary attributes."""


@dataclass
class SnapshotRuleCondition:
    context_kind: str
    attribute_key: str
    operator: TargetingOperator
    value: Any


@dataclass
class SnapshotIndividualEntry:
    context_kind: str
    attribute_key: str
    attribute_value: str


@dataclass
class SnapshotSegment:
    key: str
    conditions: list[SnapshotRuleCondition]
    included: list[SnapshotIndividualEntry] = field(default_factory=list)
    excluded: list[SnapshotIndividualEntry] = field(default_factory=list)


@dataclass
class SnapshotRolloutVariation:
    variation_key: str
    weight: int  # 0-100_000 for 0.001% precision


@dataclass
class SnapshotScheduleStep:
    duration_minutes: int
    variations: list[SnapshotRolloutVariation]


@dataclass
class SnapshotRollout:
    variations: list[SnapshotRolloutVariation]
    bucket_context_kind: str
    bucket_attribute_key: str
    seed: str | None = None
    schedule: list[SnapshotScheduleStep] | None = None
    started_at: str | None = None


@dataclass
class SnapshotTarget:
    type: Literal["rule", "individual", "segment"]
    sort_order: int
    id: str | None = None
    name: str | None = None
    variation_key: str | None = None
    rollout: SnapshotRollout | None = None
    conditions: list[SnapshotRuleCondition] | None = None
    context_kind: str | None = None
    attribute_key: str | None = None
    attribute_value: str | None = None
    segment_key: str | None = None


@dataclass
class SnapshotVariation:
    key: str
    value: Any


@dataclass
class SnapshotFlag:
    key: str
    type: Literal["boolean", "string", "number", "json"]
    enabled: bool
    variations: dict[str, SnapshotVariation]
    off_variation_key: str
    targets: list[SnapshotTarget]
    default_variation_key: str | None = None
    default_rollout: SnapshotRollout | None = None


@dataclass
class EnvironmentSnapshotMeta:
    project_id: str
    organization_id: str
    environment_slug: str
    environment_id: str


@dataclass
class EnvironmentSnapshot:
    version: int
    generated_at: str
    meta: EnvironmentSnapshotMeta
    flags: dict[str, SnapshotFlag]
    segments: dict[str, SnapshotSegment]


@dataclass
class Reason:
    type: str
    rule_id: str | None = None
    rule_name: str | None = None
    percentage: float | None = None
    bucket: int | None = None
    step_index: int | None = None
    detail: str | None = None
    error_code: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"type": self.type}
        if self.type == "rule_match":
            d["ruleId"] = self.rule_id or ""
            if self.rule_name is not None:
                d["ruleName"] = self.rule_name
        elif self.type == "percentage_rollout":
            d["percentage"] = self.percentage
            d["bucket"] = self.bucket
        elif self.type == "gradual_rollout":
            d["stepIndex"] = self.step_index
            d["percentage"] = self.percentage
            d["bucket"] = self.bucket
        elif self.type == "error":
            d["detail"] = self.detail
            if self.error_code is not None:
                d["errorCode"] = self.error_code
        return d


@dataclass
class EvalOutput:
    value: Any
    variation_key: str | None
    reasons: list[Reason]
    matched_target_name: str | None = None
    error_detail: str | None = None
    inputs_used: list[str] | None = None


@dataclass
class GradualOptions:
    api_key: str
    environment: str
    base_url: str = "https://worker.gradual.so/api/v1"
    polling_enabled: bool = True
    polling_interval_ms: int = 10_000
    realtime_enabled: bool = True
    events_enabled: bool = True
    events_flush_interval_ms: int = 30_000
    events_max_batch_size: int = 100
