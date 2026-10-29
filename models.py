"""Typed data structures shared by the simulator and dashboard."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import math
from numbers import Real
from typing import Any


@dataclass(slots=True)
class SimulationConfig:
    process_count: int = 3
    messages_per_process: int = 5
    min_delay: float = 0.05
    max_delay: float = 0.35
    seed: int = 7
    timeout: float = 30.0

    def validate(self) -> None:
        if (
            isinstance(self.process_count, bool)
            or not isinstance(self.process_count, int)
            or not 3 <= self.process_count <= 100
        ):
            raise ValueError("process_count must be an integer between 3 and 100")
        if (
            isinstance(self.messages_per_process, bool)
            or not isinstance(self.messages_per_process, int)
            or self.messages_per_process < 0
        ):
            raise ValueError("messages_per_process must be a nonnegative integer")
        if (
            isinstance(self.min_delay, bool)
            or isinstance(self.max_delay, bool)
            or not isinstance(self.min_delay, Real)
            or not isinstance(self.max_delay, Real)
            or not math.isfinite(self.min_delay)
            or not math.isfinite(self.max_delay)
        ):
            raise ValueError("network delays must be finite numbers")
        if self.min_delay < 0 or self.max_delay < 0:
            raise ValueError("network delays must be nonnegative")
        if self.min_delay > self.max_delay:
            raise ValueError("min_delay must be less than or equal to max_delay")
        if (
            isinstance(self.timeout, bool)
            or not isinstance(self.timeout, Real)
            or not math.isfinite(self.timeout)
            or self.timeout <= 0
        ):
            raise ValueError("timeout must be greater than zero")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Message:
    msg_id: str
    src: str
    dst: str
    payload: str
    lamport: int
    send_time: float
    delivery_time: float | None = None
    delay: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Event:
    physical_time: float
    event_type: str
    src: str
    dst: str
    msg_id: str
    payload: str
    message_lamport: int
    process_id: str | None = None
    process_clock: int | None = None
    delay: float | None = None
    sequence: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ValidationCheck:
    test_id: str
    description: str
    expected: str
    actual: str
    passed: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SimulationResult:
    config: SimulationConfig
    process_ids: list[str]
    messages: list[Message]
    events: list[Event]
    final_clocks: dict[str, int]
    elapsed_time: float
    summary: dict[str, Any] = field(default_factory=dict)
    process_stats: dict[str, dict[str, int]] = field(default_factory=dict)
    traffic_matrix: dict[str, dict[str, int]] = field(default_factory=dict)
    validations: list[ValidationCheck] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "configuration": self.config.to_dict(),
            "summary": self.summary,
            "process_statistics": self.process_stats,
            "traffic_matrix": self.traffic_matrix,
            "messages": [message.to_dict() for message in self.messages],
            "event_log": [event.to_dict() for event in self.events],
            "validation_results": [check.to_dict() for check in self.validations],
            "final_lamport_clocks": self.final_clocks,
            "simulation_elapsed_time": self.elapsed_time,
        }
