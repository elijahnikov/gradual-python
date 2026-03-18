"""Flag evaluation engine — pure function, no I/O.

Port of packages/sdk/src/evaluator.ts.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ._hash import hash_string
from ._types import (
    EvalOutput,
    EvaluationContext,
    Reason,
    SnapshotFlag,
    SnapshotIndividualEntry,
    SnapshotRollout,
    SnapshotRolloutVariation,
    SnapshotRuleCondition,
    SnapshotSegment,
    SnapshotTarget,
    SnapshotVariation,
)


def _get_bucket_value(
    flag_key: str,
    context: EvaluationContext,
    rollout: SnapshotRollout,
    inputs_used: set[str],
) -> int:
    inputs_used.add(f"{rollout.bucket_context_kind}.{rollout.bucket_attribute_key}")
    ctx = context.get(rollout.bucket_context_kind)
    bucket_key = ctx.get(rollout.bucket_attribute_key) if ctx else None
    seed = rollout.seed or ""
    hash_input = f"{flag_key}:{seed}:{bucket_key if bucket_key is not None else 'anonymous'}"
    return hash_string(hash_input) % 100_000


class _ActiveRollout:
    __slots__ = ("variations", "step_index")

    def __init__(
        self, variations: list[SnapshotRolloutVariation], step_index: int
    ) -> None:
        self.variations = variations
        self.step_index = step_index


def _resolve_active_rollout(
    rollout: SnapshotRollout, now: datetime
) -> _ActiveRollout:
    if not (rollout.schedule and rollout.started_at):
        return _ActiveRollout(rollout.variations, -1)

    started_at = datetime.fromisoformat(rollout.started_at.replace("Z", "+00:00"))
    elapsed_ms = (now - started_at).total_seconds() * 1000
    elapsed_minutes = elapsed_ms / 60_000

    if elapsed_minutes < 0:
        first_vars = (
            rollout.schedule[0].variations if rollout.schedule else rollout.variations
        )
        return _ActiveRollout(first_vars, 0)

    cumulative_minutes = 0
    for i, step in enumerate(rollout.schedule):
        if step.duration_minutes == 0:
            return _ActiveRollout(step.variations, i)
        cumulative_minutes += step.duration_minutes
        if elapsed_minutes < cumulative_minutes:
            return _ActiveRollout(step.variations, i)

    last_step = rollout.schedule[-1] if rollout.schedule else None
    return _ActiveRollout(
        last_step.variations if last_step else rollout.variations,
        len(rollout.schedule) - 1,
    )


class _RolloutResult:
    __slots__ = (
        "variation",
        "variation_key",
        "matched_weight",
        "bucket_value",
        "schedule_step_index",
    )

    def __init__(
        self,
        variation: SnapshotVariation,
        variation_key: str,
        matched_weight: int,
        bucket_value: int,
        schedule_step_index: int,
    ) -> None:
        self.variation = variation
        self.variation_key = variation_key
        self.matched_weight = matched_weight
        self.bucket_value = bucket_value
        self.schedule_step_index = schedule_step_index


def _select_variation_from_rollout(
    active_variations: list[SnapshotRolloutVariation],
    bucket_value: int,
    variations: dict[str, SnapshotVariation],
    schedule_step_index: int,
) -> _RolloutResult | None:
    cumulative = 0
    matched_rv: SnapshotRolloutVariation | None = None

    for rv in active_variations:
        cumulative += rv.weight
        if bucket_value < cumulative:
            matched_rv = rv
            break

    if matched_rv is None and active_variations:
        matched_rv = active_variations[-1]

    if matched_rv is None:
        return None

    variation = variations.get(matched_rv.variation_key)
    if variation is None:
        return None

    return _RolloutResult(
        variation=variation,
        variation_key=matched_rv.variation_key,
        matched_weight=matched_rv.weight,
        bucket_value=bucket_value,
        schedule_step_index=schedule_step_index,
    )


def _evaluate_condition(
    condition: SnapshotRuleCondition,
    context: EvaluationContext,
    inputs_used: set[str],
) -> bool:
    ctx_kind = condition.context_kind
    attr_key = condition.attribute_key
    operator = condition.operator
    value = condition.value

    inputs_used.add(f"{ctx_kind}.{attr_key}")
    ctx = context.get(ctx_kind)
    context_value = ctx.get(attr_key) if ctx else None

    if operator == "equals":
        return context_value == value

    if operator == "not_equals":
        return context_value != value

    if operator == "contains":
        if isinstance(context_value, str) and isinstance(value, str):
            return value in context_value
        if isinstance(context_value, list):
            return value in context_value
        return False

    if operator == "not_contains":
        if isinstance(context_value, str) and isinstance(value, str):
            return value not in context_value
        if isinstance(context_value, list):
            return value not in context_value
        return True

    if operator == "starts_with":
        if isinstance(context_value, str) and isinstance(value, str):
            return context_value.startswith(value)
        return False

    if operator == "ends_with":
        if isinstance(context_value, str) and isinstance(value, str):
            return context_value.endswith(value)
        return False

    if operator == "greater_than":
        if isinstance(context_value, (int, float)) and isinstance(value, (int, float)):
            return context_value > value
        return False

    if operator == "less_than":
        if isinstance(context_value, (int, float)) and isinstance(value, (int, float)):
            return context_value < value
        return False

    if operator == "greater_than_or_equal":
        if isinstance(context_value, (int, float)) and isinstance(value, (int, float)):
            return context_value >= value
        return False

    if operator == "less_than_or_equal":
        if isinstance(context_value, (int, float)) and isinstance(value, (int, float)):
            return context_value <= value
        return False

    if operator == "in":
        if isinstance(value, list):
            return context_value in value
        return False

    if operator == "not_in":
        if isinstance(value, list):
            return context_value not in value
        return True

    if operator == "exists":
        return context_value is not None

    if operator == "not_exists":
        return context_value is None

    return False


def _evaluate_conditions(
    conditions: list[SnapshotRuleCondition],
    context: EvaluationContext,
    inputs_used: set[str],
) -> bool:
    return all(
        _evaluate_condition(cond, context, inputs_used) for cond in conditions
    )


def _matches_individual(
    entries: list[SnapshotIndividualEntry],
    context: EvaluationContext,
    inputs_used: set[str],
) -> bool:
    for entry in entries:
        inputs_used.add(f"{entry.context_kind}.{entry.attribute_key}")
        ctx = context.get(entry.context_kind)
        if ctx and ctx.get(entry.attribute_key) == entry.attribute_value:
            return True
    return False


def _evaluate_segment(
    segment: SnapshotSegment,
    context: EvaluationContext,
    inputs_used: set[str],
) -> bool:
    # Priority 1: Excluded individuals never match
    if segment.excluded and _matches_individual(
        segment.excluded, context, inputs_used
    ):
        return False

    # Priority 2: Included individuals always match
    if segment.included and _matches_individual(
        segment.included, context, inputs_used
    ):
        return True

    # Priority 3: Evaluate conditions (empty conditions = no match)
    if not segment.conditions:
        return False
    return _evaluate_conditions(segment.conditions, context, inputs_used)


def _evaluate_target(
    target: SnapshotTarget,
    context: EvaluationContext,
    segments: dict[str, SnapshotSegment],
    inputs_used: set[str],
) -> bool:
    if target.type == "individual":
        if (
            target.context_kind
            and target.attribute_key
            and target.attribute_value is not None
        ):
            inputs_used.add(f"{target.context_kind}.{target.attribute_key}")
            ctx = context.get(target.context_kind)
            return (
                ctx is not None
                and ctx.get(target.attribute_key) == target.attribute_value
            )
        return False

    if target.type == "rule":
        if target.conditions:
            return _evaluate_conditions(target.conditions, context, inputs_used)
        return False

    if target.type == "segment":
        if target.segment_key:
            segment = segments.get(target.segment_key)
            if segment:
                return _evaluate_segment(segment, context, inputs_used)
        return False

    return False


def _resolve_target_variation(
    target: SnapshotTarget,
    flag_key: str,
    context: EvaluationContext,
    variations: dict[str, SnapshotVariation],
    inputs_used: set[str],
    now: datetime,
) -> _RolloutResult | None:
    if target.rollout:
        active = _resolve_active_rollout(target.rollout, now)
        bucket_value = _get_bucket_value(
            flag_key, context, target.rollout, inputs_used
        )
        return _select_variation_from_rollout(
            active.variations, bucket_value, variations, active.step_index
        )

    if target.variation_key:
        variation = variations.get(target.variation_key)
        if variation:
            return _RolloutResult(
                variation=variation,
                variation_key=target.variation_key,
                matched_weight=100_000,
                bucket_value=0,
                schedule_step_index=-1,
            )

    return None


def _resolve_default_variation(
    flag: SnapshotFlag,
    context: EvaluationContext,
    inputs_used: set[str],
    now: datetime,
) -> _RolloutResult | None:
    if flag.default_rollout:
        active = _resolve_active_rollout(flag.default_rollout, now)
        bucket_value = _get_bucket_value(
            flag.key, context, flag.default_rollout, inputs_used
        )
        return _select_variation_from_rollout(
            active.variations, bucket_value, flag.variations, active.step_index
        )

    if flag.default_variation_key:
        variation = flag.variations.get(flag.default_variation_key)
        if variation:
            return _RolloutResult(
                variation=variation,
                variation_key=flag.default_variation_key,
                matched_weight=100_000,
                bucket_value=0,
                schedule_step_index=-1,
            )

    return None


def _build_rule_match_reason(target: SnapshotTarget) -> Reason:
    return Reason(
        type="rule_match",
        rule_id=target.id or "",
        rule_name=target.name,
    )


def _build_rollout_reason(result: _RolloutResult) -> Reason:
    if result.schedule_step_index >= 0:
        return Reason(
            type="gradual_rollout",
            step_index=result.schedule_step_index,
            percentage=result.matched_weight / 1000,
            bucket=result.bucket_value,
        )
    return Reason(
        type="percentage_rollout",
        percentage=result.matched_weight / 1000,
        bucket=result.bucket_value,
    )


def evaluate_flag(
    flag: SnapshotFlag,
    context: EvaluationContext,
    segments: dict[str, SnapshotSegment],
    *,
    now: datetime | None = None,
) -> EvalOutput:
    """Evaluate a feature flag against the given context.

    This is a pure function with no I/O. It produces the same output
    for the same inputs across all Gradual SDK implementations.
    """
    if not flag.enabled:
        off_variation = flag.variations.get(flag.off_variation_key)
        return EvalOutput(
            value=off_variation.value if off_variation else None,
            variation_key=flag.off_variation_key,
            reasons=[Reason(type="off")],
            inputs_used=[],
        )

    if now is None:
        now = datetime.now(timezone.utc)

    inputs_used: set[str] = set()

    sorted_targets = sorted(flag.targets, key=lambda t: t.sort_order)

    for target in sorted_targets:
        if _evaluate_target(target, context, segments, inputs_used):
            resolved = _resolve_target_variation(
                target, flag.key, context, flag.variations, inputs_used, now
            )
            if resolved:
                reasons: list[Reason] = [_build_rule_match_reason(target)]
                if target.rollout:
                    reasons.append(_build_rollout_reason(resolved))

                return EvalOutput(
                    value=resolved.variation.value,
                    variation_key=resolved.variation_key,
                    reasons=reasons,
                    matched_target_name=target.name,
                    inputs_used=list(inputs_used),
                )

    resolved = _resolve_default_variation(flag, context, inputs_used, now)
    if resolved:
        reasons = []
        if flag.default_rollout:
            reasons.append(_build_rollout_reason(resolved))
        reasons.append(Reason(type="default"))

        return EvalOutput(
            value=resolved.variation.value,
            variation_key=resolved.variation_key,
            reasons=reasons,
            inputs_used=list(inputs_used),
        )

    return EvalOutput(
        value=None,
        variation_key=None,
        reasons=[Reason(type="default")],
        inputs_used=list(inputs_used),
    )
