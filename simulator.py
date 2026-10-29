"""Logical processes and the lifecycle controller for one simulation run."""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Callable

from analysis import analyze_result, build_validation_results
from models import Event, Message, SimulationConfig, SimulationResult
from network import Network


class LogicalProcess:
    def __init__(
        self,
        process_id: str,
        peers: list[str],
        inbox: asyncio.Queue[Message | None],
        network: Network,
        log_event: Callable[[Event], None],
        clock: Callable[[], float],
    ) -> None:
        self.process_id = process_id
        self.peers = peers
        self.inbox = inbox
        self.network = network
        self._log_event = log_event
        self._clock = clock
        self.lamport_clock = 0
        self.messages_sent = 0
        self.messages_received = 0

    async def send(self, destination: str, message_id: str, payload: str) -> None:
        self.lamport_clock += 1
        message = Message(
            msg_id=message_id,
            src=self.process_id,
            dst=destination,
            payload=payload,
            lamport=self.lamport_clock,
            send_time=self._clock(),
        )
        self.messages_sent += 1
        self._log_event(
            Event(
                physical_time=message.send_time,
                event_type="send",
                src=message.src,
                dst=message.dst,
                msg_id=message.msg_id,
                payload=message.payload,
                message_lamport=message.lamport,
                process_id=self.process_id,
                process_clock=self.lamport_clock,
            )
        )
        self.network.schedule(message)

    async def receive_messages(self) -> None:
        while True:
            message = await self.inbox.get()
            try:
                if message is None:
                    return
                self.lamport_clock = max(self.lamport_clock, message.lamport) + 1
                self.messages_received += 1
                self._log_event(
                    Event(
                        physical_time=self._clock(),
                        event_type="recv",
                        src=message.src,
                        dst=message.dst,
                        msg_id=message.msg_id,
                        payload=message.payload,
                        message_lamport=message.lamport,
                        process_id=self.process_id,
                        process_clock=self.lamport_clock,
                        delay=message.delay,
                    )
                )
            finally:
                self.inbox.task_done()


class SimulationController:
    def __init__(self, config: SimulationConfig) -> None:
        config.validate()
        self.config = config
        self.process_ids = [f"P{i}" for i in range(config.process_count)]
        self.events: list[Event] = []
        self.messages: list[Message] = []
        self._sequence = 0
        self._started_at = 0.0
        self.inboxes: dict[str, asyncio.Queue[Message | None]] = {}
        self.network: Network | None = None
        self.processes: dict[str, LogicalProcess] = {}

    def _elapsed(self) -> float:
        return time.perf_counter() - self._started_at

    def _log_event(self, event: Event) -> None:
        self._sequence += 1
        event.sequence = self._sequence
        self.events.append(event)

    async def run(self) -> SimulationResult:
        try:
            return await asyncio.wait_for(self._run(), timeout=self.config.timeout)
        except asyncio.TimeoutError as exc:
            raise TimeoutError(
                f"Simulation exceeded its {self.config.timeout:g}-second timeout"
            ) from exc

    async def _run(self) -> SimulationResult:
        self._started_at = time.perf_counter()
        self.inboxes = {pid: asyncio.Queue() for pid in self.process_ids}
        self.network = Network(
            inboxes=self.inboxes,
            log_event=self._log_event,
            min_delay=self.config.min_delay,
            max_delay=self.config.max_delay,
            seed=self.config.seed,
            clock=self._elapsed,
        )
        self.processes = {
            pid: LogicalProcess(
                process_id=pid,
                peers=[peer for peer in self.process_ids if peer != pid],
                inbox=self.inboxes[pid],
                network=self.network,
                log_event=self._log_event,
                clock=self._elapsed,
            )
            for pid in self.process_ids
        }
        receiver_tasks = [
            asyncio.create_task(process.receive_messages(), name=f"receive-{pid}")
            for pid, process in self.processes.items()
        ]
        sender_tasks: list[asyncio.Task[None]] = []

        destination_rng = random.Random(self.config.seed)
        planned_destinations = {
            pid: [
                destination_rng.choice(self.processes[pid].peers)
                for _ in range(self.config.messages_per_process)
            ]
            for pid in self.process_ids
        }

        async def send_all(pid: str) -> None:
            process = self.processes[pid]
            for index, destination in enumerate(planned_destinations[pid]):
                message_id = f"MSG-{pid}-{index:04d}"
                payload = f"Message {index + 1} from {pid}"
                # The message ID gives each delivery a reproducible independent delay.
                self.messages.append(
                    Message(
                        msg_id=message_id,
                        src=pid,
                        dst=destination,
                        payload=payload,
                        lamport=0,
                        send_time=0.0,
                    )
                )
                await process.send(destination, message_id, payload)
                await asyncio.sleep(0)

        try:
            sender_tasks = [
                asyncio.create_task(send_all(pid), name=f"send-{pid}")
                for pid in self.process_ids
            ]
            await asyncio.gather(*sender_tasks)
            await self.network.wait_for_deliveries()
            await asyncio.gather(*(inbox.join() for inbox in self.inboxes.values()))
            for inbox in self.inboxes.values():
                await inbox.put(None)
            await asyncio.gather(*receiver_tasks)
        finally:
            for task in sender_tasks + receiver_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*(sender_tasks + receiver_tasks), return_exceptions=True)
            await self.network.cancel_deliveries()

        # Reuse the canonical message objects populated by send/delivery.
        event_sends = {event.msg_id: event for event in self.events if event.event_type == "send"}
        event_deliveries = {
            event.msg_id: event for event in self.events if event.event_type == "deliver"
        }
        for message in self.messages:
            sent = event_sends[message.msg_id]
            delivered = event_deliveries[message.msg_id]
            message.lamport = sent.message_lamport
            message.send_time = sent.physical_time
            message.delivery_time = delivered.physical_time
            message.delay = delivered.delay

        elapsed = self._elapsed()
        result = SimulationResult(
            config=self.config,
            process_ids=self.process_ids,
            messages=self.messages,
            events=self.events,
            final_clocks={pid: process.lamport_clock for pid, process in self.processes.items()},
            elapsed_time=elapsed,
        )
        analyze_result(result)
        result.validations = build_validation_results(result)
        failures = [check for check in result.validations if not check.passed]
        if failures:
            details = "; ".join(f"{check.test_id}: {check.actual}" for check in failures)
            raise RuntimeError(f"Simulation validation failed: {details}")
        return result


async def run_simulation_async(config: SimulationConfig) -> SimulationResult:
    """Run a simulation from an existing asyncio event loop."""
    return await SimulationController(config).run()


def run_simulation(config: SimulationConfig) -> SimulationResult:
    """Convenience entry point for synchronous callers such as Streamlit."""
    return asyncio.run(run_simulation_async(config))
