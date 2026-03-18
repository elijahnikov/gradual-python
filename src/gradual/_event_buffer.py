"""Batched event delivery for evaluation tracking.

Port of packages/sdk/src/event-buffer.ts.
"""

from __future__ import annotations

import json
import logging
import threading
from typing import Any

import httpx

logger = logging.getLogger("gradual")

SDK_VERSION = "0.1.0"


class EventBuffer:
    """Batched event buffer that flushes evaluation events to the Gradual API."""

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        meta: dict[str, str],
        flush_interval_ms: int = 30_000,
        max_batch_size: int = 100,
    ) -> None:
        self._base_url = base_url
        self._api_key = api_key
        self._meta = meta
        self._max_batch_size = max_batch_size
        self._events: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._flush_interval = flush_interval_ms / 1000
        self._timer: threading.Timer | None = None
        self._stopped = False
        self._start_timer()

    def _start_timer(self) -> None:
        if self._stopped:
            return
        self._timer = threading.Timer(self._flush_interval, self._timer_flush)
        self._timer.daemon = True
        self._timer.start()

    def _timer_flush(self) -> None:
        self.flush()
        self._start_timer()

    def push(self, event: dict[str, Any]) -> None:
        """Add an evaluation event to the buffer."""
        batch = None
        with self._lock:
            self._events.append(event)
            if len(self._events) >= self._max_batch_size:
                batch = self._events[: self._max_batch_size]
                self._events = self._events[self._max_batch_size :]
        if batch:
            self._send(batch)

    def flush(self) -> None:
        """Flush all pending events."""
        with self._lock:
            if not self._events:
                return
            batch = self._events[: self._max_batch_size]
            self._events = self._events[self._max_batch_size :]
        self._send(batch)

    def _send(self, batch: list[dict[str, Any]]) -> None:
        payload = {
            "meta": {
                **self._meta,
                "sdkVersion": SDK_VERSION,
            },
            "events": batch,
        }
        try:
            httpx.post(
                f"{self._base_url}/sdk/evaluations",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self._api_key}",
                },
                content=json.dumps(payload),
                timeout=10,
            )
        except Exception:
            # Fire-and-forget: silently drop failed events
            logger.debug("Failed to send evaluation events", exc_info=True)

    def destroy(self) -> None:
        """Stop the timer and flush remaining events."""
        self._stopped = True
        if self._timer:
            self._timer.cancel()
            self._timer = None
        self.flush()
