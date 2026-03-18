"""Gradual — Feature flag SDK for Python."""

from ._client import GradualClient
from ._evaluator import evaluate_flag
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

__all__ = [
    "GradualClient",
    "GradualOptions",
    "evaluate_flag",
    "EnvironmentSnapshot",
    "EnvironmentSnapshotMeta",
    "EvalOutput",
    "EvaluationContext",
    "Reason",
    "SnapshotFlag",
    "SnapshotIndividualEntry",
    "SnapshotRollout",
    "SnapshotRolloutVariation",
    "SnapshotRuleCondition",
    "SnapshotScheduleStep",
    "SnapshotSegment",
    "SnapshotTarget",
    "SnapshotVariation",
]

__version__ = "0.1.0"


def create_gradual(options: GradualOptions) -> GradualClient:
    """Create a Gradual feature flag client.

    Example::

        from gradual import create_gradual, GradualOptions

        client = create_gradual(GradualOptions(
            api_key="gra_xxx",
            environment="production",
        ))
        client.wait_until_ready()

        enabled = client.is_enabled("new-feature")
        theme = client.get("theme", fallback="dark")

        client.identify({"user": {"id": "123", "plan": "pro"}})
        client.close()
    """
    return GradualClient(options)
