# app/core/alerts.py

"""The alert channel (backend #92).

A single place for operational alerts that a human has to see. The transport is one
outbound webhook, configured by `ALERT_WEBHOOK_URL`, posting `{"text": ...}` — the shape
Slack incoming webhooks, Telegram bridges and Discord (`/slack` suffix) all accept, so
ts-arena #7 can pick the channel without touching this file.

Two properties matter more than the transport:

* **It never raises.** Every caller is a scheduler job or a ranking write. An alert that
  cannot be delivered must not take down the work that produced it — the whole point is to
  make failures visible, not to add a new one.
* **It always logs.** ts-arena #7 (the channel itself) is not done yet, so on this
  deployment `ALERT_WEBHOOK_URL` is unset and the channel is the container log. The full
  alert text goes out at WARNING either way, so an alert is never lost to a missing
  config value, and the log line stays the durable record once the webhook exists.

The VMs have outbound internet but no inbound access, so an outbound POST is the only
delivery mechanism available (see `infra-networking`).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Keep a single alert comfortably under what a chat webhook will accept; the digest
# format below is already grouped, so truncation only ever bites on a platform-wide
# outage, where the first lines carry the message anyway.
MAX_ALERT_CHARS = 3500

DEFAULT_TIMEOUT_SECONDS = 10.0


@dataclass
class Alert:
    """One operational alert: a title, a body, and where it came from."""

    title: str
    lines: List[str] = field(default_factory=list)
    source: str = "api-portal"

    def render(self) -> str:
        """Render to the plain text that goes to the channel and to the log."""
        body = "\n".join(self.lines)
        text = f"[{self.source}] {self.title}"
        if body:
            text = f"{text}\n{body}"
        if len(text) > MAX_ALERT_CHARS:
            text = text[: MAX_ALERT_CHARS - 3] + "..."
        return text


def _webhook_url() -> Optional[str]:
    url = os.getenv("ALERT_WEBHOOK_URL", "").strip()
    return url or None


def _post(url: str, payload: Dict[str, Any], timeout: float) -> None:
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:  # nosec B310
        # Read and discard: some webhook endpoints hold the connection until the body
        # is consumed.
        response.read()


def send_alert(alert: Alert, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> bool:
    """Send `alert` to the alert channel.

    Returns True when the webhook accepted it, False when it was only logged (no webhook
    configured, or delivery failed). Never raises.
    """
    text = alert.render()

    # Log first, unconditionally: the log line is the record that survives a webhook
    # outage, and it is the only channel at all until ts-arena #7 lands.
    logger.warning("ALERT %s", text)

    url = _webhook_url()
    if url is None:
        logger.info(
            "ALERT_WEBHOOK_URL is not set, so the alert above went to this log only. "
            "Set it on the api-portal app to route alerts to the channel (ts-arena #7)."
        )
        return False

    try:
        _post(url, {"text": text}, timeout)
        return True
    except (urllib.error.URLError, OSError, ValueError) as exc:
        # Includes timeouts, DNS failures and non-2xx responses (HTTPError is a URLError).
        logger.error("Failed to deliver alert to the alert channel: %s", exc)
        return False


async def send_alert_async(alert: Alert, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> bool:
    """`send_alert` for async callers.

    The POST is blocking, and every caller is a scheduler job running on the event loop
    that also serves the API, so it goes to a worker thread rather than stalling the loop
    for up to `timeout` seconds.
    """
    return await asyncio.to_thread(send_alert, alert, timeout)
