import asyncio

import pytest

from models import SimulationConfig
from simulator import run_simulation, run_simulation_async


@pytest.mark.parametrize(
    ("process_count", "messages_per_process", "total"),
    [(3, 5, 15), (4, 5, 20), (4, 10, 40)],
)
def test_expected_messages_are_sent_delivered_and_received(
    process_count: int, messages_per_process: int, total: int
) -> None:
    result = run_simulation(
        SimulationConfig(
            process_count=process_count,
            messages_per_process=messages_per_process,
            min_delay=0.001,
            max_delay=0.004,
            seed=7,
        )
    )
    assert result.summary["total_messages_sent"] == total
    assert result.summary["total_messages_delivered"] == total
    assert result.summary["total_messages_received"] == total
    assert all(check.passed for check in result.validations)
    assert len(result.messages) == total


def test_zero_messages_terminates_with_empty_event_log() -> None:
    result = run_simulation(
        SimulationConfig(
            process_count=3,
            messages_per_process=0,
            min_delay=0,
            max_delay=0,
        )
    )
    assert result.events == []
    assert result.messages == []
    assert result.final_clocks == {"P0": 0, "P1": 0, "P2": 0}
    assert all(check.passed for check in result.validations)


def test_seed_reproduces_destinations_and_simulated_delays() -> None:
    config = SimulationConfig(
        process_count=4,
        messages_per_process=6,
        min_delay=0.002,
        max_delay=0.02,
        seed=7,
    )
    first = run_simulation(config)
    second = run_simulation(config)
    first_values = [(m.msg_id, m.src, m.dst, m.delay, m.lamport) for m in first.messages]
    second_values = [(m.msg_id, m.src, m.dst, m.delay, m.lamport) for m in second.messages]
    assert first_values == second_values


def test_narrow_delay_range_is_respected() -> None:
    result = run_simulation(
        SimulationConfig(
            process_count=3,
            messages_per_process=3,
            min_delay=0.005,
            max_delay=0.01,
        )
    )
    assert all(0.005 <= message.delay <= 0.01 for message in result.messages)


def test_async_entry_point_works_inside_async_application() -> None:
    async def execute():
        return await run_simulation_async(
            SimulationConfig(
                process_count=3,
                messages_per_process=1,
                min_delay=0,
                max_delay=0,
            )
        )

    result = asyncio.run(execute())
    assert result.summary["total_messages_received"] == 3


def test_simulation_timeout_cancels_pending_tasks() -> None:
    with pytest.raises(TimeoutError, match="timeout"):
        run_simulation(
            SimulationConfig(
                process_count=3,
                messages_per_process=1,
                min_delay=0.2,
                max_delay=0.2,
                timeout=0.01,
            )
        )

