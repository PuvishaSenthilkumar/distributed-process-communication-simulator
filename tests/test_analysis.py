import json

from analysis import (
    count_out_of_order,
    events_to_csv,
    report_to_json,
    traffic_matrix_to_csv,
    validations_to_csv,
)
from models import Event, SimulationConfig
from simulator import run_simulation


def test_traffic_matrix_total_matches_sent_messages() -> None:
    result = run_simulation(
        SimulationConfig(
            process_count=4,
            messages_per_process=5,
            min_delay=0,
            max_delay=0.005,
        )
    )
    matrix_total = sum(sum(row.values()) for row in result.traffic_matrix.values())
    assert matrix_total == result.summary["total_messages_sent"]
    assert all(result.traffic_matrix[pid][pid] == 0 for pid in result.process_ids)


def test_process_statistics_match_actual_events() -> None:
    result = run_simulation(
        SimulationConfig(
            process_count=3,
            messages_per_process=6,
            min_delay=0,
            max_delay=0.003,
        )
    )
    for pid in result.process_ids:
        sent = sum(
            event.event_type == "send" and event.src == pid for event in result.events
        )
        received = sum(
            event.event_type == "recv" and event.dst == pid for event in result.events
        )
        assert result.process_stats[pid]["sent"] == sent
        assert result.process_stats[pid]["received"] == received


def test_out_of_order_detector_counts_inversions_per_pair() -> None:
    events = [
        Event(0.0, "send", "P0", "P1", "m1", "", 1, sequence=1),
        Event(0.0, "send", "P0", "P1", "m2", "", 2, sequence=2),
        Event(0.1, "deliver", "P0", "P1", "m2", "", 2, sequence=3),
        Event(0.2, "deliver", "P0", "P1", "m1", "", 1, sequence=4),
    ]
    assert count_out_of_order(events) == 1


def test_download_exports_contain_current_run_data() -> None:
    result = run_simulation(
        SimulationConfig(
            process_count=3,
            messages_per_process=2,
            min_delay=0,
            max_delay=0,
        )
    )
    assert "event_type" in events_to_csv(result.events)
    assert "P0" in traffic_matrix_to_csv(result)
    assert "TC-01" in validations_to_csv(result)
    report = json.loads(report_to_json(result))
    assert report["summary"]["total_messages_sent"] == 6
    assert len(report["event_log"]) == 18


def test_zero_message_run_has_consistent_empty_analysis() -> None:
    result = run_simulation(
        SimulationConfig(
            process_count=3,
            messages_per_process=0,
            min_delay=0,
            max_delay=0,
        )
    )
    assert result.summary["mean_delay"] is None
    assert result.summary["traffic_matrix_total"] == 0
    assert all(check.passed for check in result.validations)

