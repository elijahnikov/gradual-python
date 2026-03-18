"""Gradual SDK client for Python.

Port of packages/sdk/src/client.ts.
Provides both sync and async interfaces via httpx.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import re
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, TypeVar, overload

import httpx

from ._evaluator import evaluate_flag
from ._event_buffer import EventBuffer
from ._types import (
    EnvironmentSnapshot,
    EnvironmentSnapshotMeta,
    EvalOutput,
    EvaluationContext,
    GradualOptions,
    Reason,
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

logger = logging.getLogger("gradual")

T = TypeVar("T")


def _hash_context(context: EvaluationContext) -> str:
    """Deterministic hash of context for MAU counting."""
    parts: list[str] = []
    for kind in sorted(context.keys()):
        attrs = context.get(kind, {})
        id_val = attrs.get("id") or attrs.get("key")
        if id_val is not None:
            parts.append(f"{kind}:{id_val}")
    s = "|".join(parts)
    h1 = 5381
    h2 = 52_711
    for ch in s:
        c = ord(ch)
        h1 = (h1 * 33 + c) & 0xFFFFFFFF
        h2 = (h2 * 31 + c) & 0xFFFFFFFF
    # Convert to signed to match Math.abs behavior
    if h1 >= 0x80000000:
        h1 -= 0x100000000
    if h2 >= 0x80000000:
        h2 -= 0x100000000
    a = abs(h1)
    b = abs(h2)
    # Convert to base36
    def to_base36(n: int) -> str:
        if n == 0:
            return "0"
        digits = ""
        while n:
            digits = "0123456789abcdefghijklmnopqrstuvwxyz"[n % 36] + digits
            n //= 36
        return digits

    return to_base36(a) + to_base36(b)


def _parse_snapshot(data: dict[str, Any]) -> EnvironmentSnapshot:
    """Parse a raw JSON snapshot dict into typed dataclasses."""
    meta_raw = data["meta"]
    meta = EnvironmentSnapshotMeta(
        project_id=meta_raw["projectId"],
        organization_id=meta_raw["organizationId"],
        environment_slug=meta_raw["environmentSlug"],
        environment_id=meta_raw["environmentId"],
    )

    flags: dict[str, SnapshotFlag] = {}
    for fk, fv in data.get("flags", {}).items():
        variations: dict[str, SnapshotVariation] = {}
        for vk, vv in fv.get("variations", {}).items():
            variations[vk] = SnapshotVariation(key=vv["key"], value=vv["value"])

        targets: list[SnapshotTarget] = []
        for tv in fv.get("targets", []):
            rollout = None
            if "rollout" in tv and tv["rollout"]:
                rollout = _parse_rollout(tv["rollout"])
            conditions = None
            if "conditions" in tv and tv["conditions"]:
                conditions = [_parse_condition(c) for c in tv["conditions"]]

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

        default_rollout = None
        if fv.get("defaultRollout"):
            default_rollout = _parse_rollout(fv["defaultRollout"])

        flags[fk] = SnapshotFlag(
            key=fv["key"],
            type=fv["type"],
            enabled=fv["enabled"],
            variations=variations,
            off_variation_key=fv["offVariationKey"],
            targets=targets,
            default_variation_key=fv.get("defaultVariationKey"),
            default_rollout=default_rollout,
        )

    segments: dict[str, SnapshotSegment] = {}
    for sk, sv in data.get("segments", {}).items():
        segments[sk] = SnapshotSegment(
            key=sv["key"],
            conditions=[_parse_condition(c) for c in sv.get("conditions", [])],
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

    return EnvironmentSnapshot(
        version=data["version"],
        generated_at=data["generatedAt"],
        meta=meta,
        flags=flags,
        segments=segments,
    )


def _parse_condition(c: dict[str, Any]) -> SnapshotRuleCondition:
    return SnapshotRuleCondition(
        context_kind=c["contextKind"],
        attribute_key=c["attributeKey"],
        operator=c["operator"],
        value=c["value"],
    )


def _parse_rollout(r: dict[str, Any]) -> SnapshotRollout:
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


class GradualClient:
    """Feature flag client that connects to the Gradual edge service.

    Usage::

        client = GradualClient(GradualOptions(
            api_key="gra_xxx",
            environment="production",
        ))
        client.wait_until_ready()

        enabled = client.is_enabled("my-flag")
        value = client.get("theme", fallback="dark")

        client.identify({"user": {"id": "user-123", "plan": "pro"}})
        client.close()
    """

    def __init__(self, options: GradualOptions) -> None:
        self._api_key = options.api_key
        self._environment = options.environment
        self._base_url = options.base_url
        self._snapshot: EnvironmentSnapshot | None = None
        self._identified_context: EvaluationContext = {}
        self._identity_hash: str | None = None
        self._identity_hash_sent = False
        self._sent_context_hashes: set[str] = set()
        self._mau_limit_reached = False
        self._update_listeners: set[Any] = set()
        self._event_buffer: EventBuffer | None = None
        self._etag: str | None = None
        self._events_enabled = options.events_enabled
        self._events_flush_interval_ms = options.events_flush_interval_ms
        self._events_max_batch_size = options.events_max_batch_size
        self._polling_enabled = options.polling_enabled
        self._polling_interval_ms = options.polling_interval_ms
        self._polling_timer: threading.Timer | None = None
        self._ready_event = threading.Event()
        self._http = httpx.Client(timeout=30)
        self._init_error: Exception | None = None

        self._init_thread = threading.Thread(target=self._init, daemon=True)
        self._init_thread.start()

    def _init(self) -> None:
        try:
            resp = self._http.post(
                f"{self._base_url}/sdk/init",
                json={"apiKey": self._api_key},
            )
            if resp.status_code != 200:
                error = resp.json().get("error", resp.reason_phrase)
                raise RuntimeError(f"Gradual: Failed to initialize - {error}")

            data = resp.json()
            if not data.get("valid"):
                raise RuntimeError(
                    f"Gradual: Invalid API key - {data.get('error', 'Unknown error')}"
                )

            if data.get("mauLimitReached"):
                self._mau_limit_reached = True

            self._fetch_snapshot()
            self._start_polling()
            self._initialize_event_buffer()
        except Exception as e:
            self._init_error = e
            logger.error("Gradual: Initialization failed: %s", e)
        finally:
            self._ready_event.set()

    def _initialize_event_buffer(self) -> None:
        if self._events_enabled and self._snapshot and self._snapshot.meta:
            self._event_buffer = EventBuffer(
                base_url=self._base_url,
                api_key=self._api_key,
                meta={
                    "projectId": self._snapshot.meta.project_id,
                    "organizationId": self._snapshot.meta.organization_id,
                    "environmentId": self._snapshot.meta.environment_id,
                    "sdkPlatform": "python",
                },
                flush_interval_ms=self._events_flush_interval_ms,
                max_batch_size=self._events_max_batch_size,
            )

    def _fetch_snapshot(self) -> None:
        headers: dict[str, str] = {
            "Authorization": f"Bearer {self._api_key}",
        }
        if self._etag:
            headers["If-None-Match"] = self._etag

        resp = self._http.get(
            f"{self._base_url}/sdk/snapshot",
            params={"environment": self._environment},
            headers=headers,
        )

        if resp.status_code == 304:
            return

        if resp.status_code != 200:
            error = resp.json().get("error", resp.reason_phrase)
            raise RuntimeError(f"Gradual: Failed to fetch snapshot - {error}")

        etag = resp.headers.get("ETag")
        if etag:
            self._etag = etag

        self._snapshot = _parse_snapshot(resp.json())

    def _start_polling(self) -> None:
        if not self._polling_enabled:
            return

        def poll() -> None:
            try:
                prev_version = self._snapshot.version if self._snapshot else None
                self._fetch_snapshot()
                if self._snapshot and self._snapshot.version != prev_version:
                    for cb in self._update_listeners:
                        cb()
            except Exception as e:
                logger.warning("Gradual: Polling refresh failed: %s", e)
            # Schedule next poll
            if not getattr(self, "_stopped", False):
                self._polling_timer = threading.Timer(
                    self._polling_interval_ms / 1000, poll
                )
                self._polling_timer.daemon = True
                self._polling_timer.start()

        self._polling_timer = threading.Timer(
            self._polling_interval_ms / 1000, poll
        )
        self._polling_timer.daemon = True
        self._polling_timer.start()

    def wait_until_ready(self, timeout: float | None = None) -> None:
        """Block until the SDK is initialized (snapshot fetched).

        Raises RuntimeError if initialization failed.
        """
        self._ready_event.wait(timeout=timeout)
        if self._init_error:
            raise self._init_error

    def is_ready(self) -> bool:
        """Check if the SDK has been initialized."""
        return self._snapshot is not None

    def _ensure_ready(self) -> EnvironmentSnapshot:
        if not self._snapshot:
            raise RuntimeError(
                "Gradual: SDK not ready. Call wait_until_ready() first."
            )
        return self._snapshot

    def _merge_context(
        self, context: EvaluationContext | None = None
    ) -> EvaluationContext:
        merged: EvaluationContext = {}
        all_kinds = set(self._identified_context.keys())
        if context:
            all_kinds |= set(context.keys())
        for kind in all_kinds:
            merged[kind] = {
                **self._identified_context.get(kind, {}),
                **(context or {}).get(kind, {}),
            }
        return merged

    def _evaluate_raw(
        self, key: str, context: EvaluationContext
    ) -> tuple[EvalOutput | None, EnvironmentSnapshot, float]:
        snapshot = self._ensure_ready()
        if not snapshot.flags:
            return None, snapshot, 0

        flag = snapshot.flags.get(key)
        if not flag:
            return None, snapshot, 0

        if self._mau_limit_reached and not self._identity_hash:
            off_variation = flag.variations.get(flag.off_variation_key)
            return (
                EvalOutput(
                    value=off_variation.value if off_variation else None,
                    variation_key=flag.off_variation_key,
                    reasons=[Reason(type="error", detail="MAU_LIMIT_REACHED")],
                ),
                snapshot,
                0,
            )

        start = time.perf_counter_ns()
        try:
            output = evaluate_flag(flag, context, snapshot.segments or {})
        except Exception as e:
            output = EvalOutput(
                value=None,
                variation_key=None,
                reasons=[Reason(type="error", detail=str(e))],
                error_detail=str(e),
            )
        duration_us = (time.perf_counter_ns() - start) / 1000

        return output, snapshot, duration_us

    def is_enabled(
        self,
        key: str,
        *,
        context: EvaluationContext | None = None,
    ) -> bool:
        """Check if a boolean flag is enabled."""
        self._ensure_ready()
        merged = self._merge_context(context)
        value = self._evaluate_and_track(key, merged)
        return value if isinstance(value, bool) else False

    def get(
        self,
        key: str,
        *,
        fallback: T,
        context: EvaluationContext | None = None,
    ) -> T:
        """Get a flag value with a fallback default."""
        self._ensure_ready()
        merged = self._merge_context(context)
        value = self._evaluate_and_track(key, merged)
        return value if value is not None else fallback  # type: ignore[return-value]

    def evaluate(
        self,
        key: str,
        *,
        context: EvaluationContext | None = None,
    ) -> dict[str, Any]:
        """Evaluate a flag and return the full structured result."""
        self._ensure_ready()
        merged = self._merge_context(context)
        output, snapshot, duration_us = self._evaluate_raw(key, merged)

        if not output:
            result = {
                "schemaVersion": 1,
                "key": key,
                "value": None,
                "variationKey": None,
                "reasons": [{"type": "error", "detail": "FLAG_NOT_FOUND"}],
                "flagVersion": snapshot.version,
                "evaluatedAt": datetime.now(timezone.utc).isoformat(),
                "traceId": str(uuid.uuid4()),
            }
            self._track_event(
                key=key,
                variation_key=None,
                value=None,
                reasons=[Reason(type="error", detail="FLAG_NOT_FOUND")],
                context=merged,
                flag_version=snapshot.version,
            )
            return result

        result = self._build_result(key, output, snapshot.version, duration_us)

        self._track_event(
            key=key,
            variation_key=output.variation_key,
            value=output.value,
            reasons=output.reasons,
            context=merged,
            matched_target_name=output.matched_target_name,
            flag_version=snapshot.version,
            error_detail=output.error_detail,
            duration_us=duration_us,
            inputs_used=output.inputs_used,
        )

        return result

    def _evaluate_and_track(self, key: str, context: EvaluationContext) -> Any:
        output, snapshot, duration_us = self._evaluate_raw(key, context)

        if not output:
            self._track_event(
                key=key,
                variation_key=None,
                value=None,
                reasons=[Reason(type="error", detail="FLAG_NOT_FOUND")],
                context=context,
                flag_version=snapshot.version,
            )
            return None

        self._track_event(
            key=key,
            variation_key=output.variation_key,
            value=output.value,
            reasons=output.reasons,
            context=context,
            matched_target_name=output.matched_target_name,
            flag_version=snapshot.version,
            error_detail=output.error_detail,
            duration_us=duration_us,
            inputs_used=output.inputs_used,
        )

        return output.value

    def _build_result(
        self,
        key: str,
        output: EvalOutput,
        flag_version: int,
        duration_us: float,
    ) -> dict[str, Any]:
        rule_match = next(
            (r for r in output.reasons if r.type == "rule_match"), None
        )
        return {
            "schemaVersion": 1,
            "key": key,
            "value": output.value,
            "variationKey": output.variation_key,
            "reasons": [r.to_dict() for r in output.reasons],
            "ruleId": rule_match.rule_id if rule_match else None,
            "flagVersion": flag_version,
            "evaluatedAt": datetime.now(timezone.utc).isoformat(),
            "evaluationDurationUs": int(duration_us),
            "inputsUsed": output.inputs_used,
            "traceId": str(uuid.uuid4()),
        }

    def _track_event(
        self,
        *,
        key: str,
        variation_key: str | None,
        value: Any,
        reasons: list[Reason],
        context: EvaluationContext,
        matched_target_name: str | None = None,
        flag_version: int | None = None,
        error_detail: str | None = None,
        duration_us: float | None = None,
        inputs_used: list[str] | None = None,
    ) -> None:
        if not self._event_buffer:
            return

        context_kinds = list(context.keys())
        context_keys: dict[str, list[str]] = {}
        for kind in context_kinds:
            context_keys[kind] = list((context.get(kind) or {}).keys())

        is_anonymous = not context_kinds or all(
            len((context.get(k) or {}).keys()) == 0 for k in context_kinds
        )

        self._event_buffer.push(
            {
                "schemaVersion": 1,
                "key": key,
                "variationKey": variation_key,
                "value": value,
                "reasons": [r.to_dict() for r in reasons],
                "evaluatedAt": datetime.now(timezone.utc).isoformat(),
                "flagVersion": flag_version or 0,
                "inputsUsed": inputs_used,
                "traceId": str(uuid.uuid4()),
                "contextKinds": context_kinds,
                "contextKeys": context_keys,
                "contextIdentityHash": self._get_or_compute_identity_hash(context),
                "timestamp": int(time.time() * 1000),
                "matchedTargetName": matched_target_name,
                "errorDetail": error_detail,
                "evaluationDurationUs": int(duration_us) if duration_us else None,
                "isAnonymous": is_anonymous,
            }
        )

    def _get_or_compute_identity_hash(
        self, context: EvaluationContext
    ) -> str | None:
        if self._identity_hash and not self._identity_hash_sent:
            self._identity_hash_sent = True
            return self._identity_hash

        h = _hash_context(context)
        if h and h not in self._sent_context_hashes:
            self._sent_context_hashes.add(h)
            return h

        return None

    def identify(self, context: EvaluationContext) -> None:
        """Set persistent user context for all evaluations."""
        self._identified_context = dict(context)
        self._identity_hash = _hash_context(context)
        self._identity_hash_sent = False

    def reset(self) -> None:
        """Clear the identified user context."""
        self._identified_context = {}
        self._identity_hash = None
        self._identity_hash_sent = False
        self._sent_context_hashes.clear()

    def refresh(self) -> None:
        """Refresh the snapshot from the server."""
        self._fetch_snapshot()

    def get_snapshot(self) -> EnvironmentSnapshot | None:
        """Get the current snapshot (for debugging)."""
        return self._snapshot

    def on_update(self, callback: Any) -> Any:
        """Subscribe to snapshot updates. Returns an unsubscribe function."""
        self._update_listeners.add(callback)

        def unsubscribe() -> None:
            self._update_listeners.discard(callback)

        return unsubscribe

    def close(self) -> None:
        """Flush pending events and stop background tasks."""
        self._stopped = True  # type: ignore[attr-defined]
        if self._polling_timer:
            self._polling_timer.cancel()
            self._polling_timer = None
        if self._event_buffer:
            self._event_buffer.destroy()
            self._event_buffer = None
        self._http.close()
