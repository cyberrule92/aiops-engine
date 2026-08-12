"""
Simple async event bus for broadcasting events to SSE clients.
"""
import asyncio
import json
from typing import Dict, List, Any
from collections import deque
import logging

logger = logging.getLogger(__name__)


class EventBus:
    def __init__(self, maxlen: int = 500):
        self._queues: Dict[str, List[asyncio.Queue]] = {}
        self._history: deque = deque(maxlen=maxlen)

    def subscribe(self, channel: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=200)
        self._queues.setdefault(channel, []).append(q)
        return q

    def unsubscribe(self, channel: str, q: asyncio.Queue):
        if channel in self._queues:
            try:
                self._queues[channel].remove(q)
            except ValueError:
                pass

    async def publish(self, channel: str, data: Any):
        payload = {"channel": channel, "data": data}
        self._history.append(payload)
        for q in list(self._queues.get(channel, [])):
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                logger.warning(f"EventBus queue full on channel {channel}, dropping message")

    async def publish_all(self, data: Any):
        """Publish to all channels."""
        for channel in list(self._queues.keys()):
            await self.publish(channel, data)

    def get_history(self, limit: int = 100) -> List[Any]:
        items = list(self._history)
        return items[-limit:]


event_bus = EventBus()
