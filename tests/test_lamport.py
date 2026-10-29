from models import Event, SimulationConfig, SimulationResult
from simulator import run_simulation


def test_every_process_local_clock_strictly_increases() -> None:
    result = run_simulation(
        SimulationConfig(
            process_count=4,
            messages_per_process=8,
            min_delay=0.001,
            max_delay=0.01,
            seed=19,
        )
    )
    for pid in result.process_ids:
        clocks = [
            event.process_clock
            for event in result.events
            if event.process_id == pid and event.event_type in ("send", "recv")
        ]
        assert all(left < right for left, right in zip(clocks, clocks[1:]))


def test_receive_clock_is_greater_than_original_sender_timestamp() -> None:
    result = run_simulation(
        SimulationConfig(
            process_count=3,
            messages_per_process=5,
            min_delay=0,
            max_delay=0.01,
        )
    )
    send_clocks = {
        event.msg_id: event.message_lamport
        for event in result.events
        if event.event_type == "send"
    }
    for event in result.events:
        if event.event_type == "recv":
            assert event.process_clock > send_clocks[event.msg_id]


def test_receiver_uses_max_local_and_sender_clock_plus_one() -> None:
    result = run_simulation(
        SimulationConfig(
            process_count=3,
            messages_per_process=4,
            min_delay=0,
            max_delay=0.02,
        )
    )
    local_clock = {pid: 0 for pid in result.process_ids}
    for event in sorted(result.events, key=lambda item: item.sequence):
        if event.event_type not in ("send", "recv"):
            continue
        if event.event_type == "send":
            assert event.process_clock == local_clock[event.process_id] + 1
        else:
            assert event.process_clock == max(
                local_clock[event.process_id], event.message_lamport
            ) + 1
        local_clock[event.process_id] = event.process_clock
    assert local_clock == result.final_clocks


def test_message_timestamp_remains_the_original_send_timestamp() -> None:
    result = run_simulation(
        SimulationConfig(
            process_count=3,
            messages_per_process=3,
            min_delay=0,
            max_delay=0,
        )
    )
    send_clocks = {
        event.msg_id: event.message_lamport
        for event in result.events
        if event.event_type == "send"
    }
    assert all(message.lamport == send_clocks[message.msg_id] for message in result.messages)

