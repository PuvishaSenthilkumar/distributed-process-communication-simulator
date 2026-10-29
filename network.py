"""Asynchronous simulated network with independently scheduled deliveries."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Callable

from models import Event, Message


class Network:
    def __init__(
        self,
        inboxes: dict[str, asyncio.Queue[Message | None]],
        log_event: Callable[..., None],
        min_delay: float,
        max_delay: float,
        seed: int,
        clock: Callable[[], float],
    ) -> None:
        self._inboxes = inboxes
        self._log_event = log_event
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._seed = seed
        self._clock = clock
        self.delivery_tasks: list[asyncio.Task[None]] = []

    def schedule(self, message: Message) -> None:
        """Schedule one uniquely delayed delivery without blocking its sender."""
        delay_rng = random.Random(f"{self._seed}:{message.msg_id}:delay")
        delay = delay_rng.uniform(self._min_delay, self._max_delay)
        task = asyncio.create_task(self._deliver(message, delay), name=f"deliver-{message.msg_id}")
        self.delivery_tasks.append(task)

    async def _deliver(self, message: Message, delay: float) -> None:
        await asyncio.sleep(delay)
        message.delay = delay
        message.delivery_time = self._clock()
        self._log_event(
            Event(
                physical_time=message.delivery_time,
                event_type="deliver",
                src=message.src,
                dst=message.dst,
                msg_id=message.msg_id,
                payload=message.payload,
                message_lamport=message.lamport,
                delay=delay,
            )
        )
        await self._inboxes[message.dst].put(message)

    async def wait_for_deliveries(self) -> None:
        if self.delivery_tasks:
            await asyncio.gather(*self.delivery_tasks)

    async def cancel_deliveries(self) -> None:
        pending = [task for task in self.delivery_tasks if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
