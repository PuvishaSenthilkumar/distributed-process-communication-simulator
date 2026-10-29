"""Event-log analytics, correctness checks, and downloadable report helpers."""

from __future__ import annotations

import csv
import io
import json
import statistics
from collections import Counter, defaultdict
from typing import Any

from models import Event, SimulationResult, ValidationCheck


def analyze_result(result: SimulationResult) -> None:
    process_ids = result.process_ids
    sent = [event for event in result.events if event.event_type == "send"]
    delivered = [event for event in result.events if event.event_type == "deliver"]
    received = [event for event in result.events if event.event_type == "recv"]
    delays = [event.delay for event in delivered if event.delay is not None]
    sent_counts = Counter(event.src for event in sent)
    received_counts = Counter(event.dst for event in received)
    traffic = {src: {dst: 0 for dst in process_ids} for src in process_ids}
    for message in result.messages:
        if message.src in traffic and message.dst in traffic[message.src]:
            traffic[message.src][message.dst] += 1

    result.traffic_matrix = traffic
    result.process_stats = {
        pid: {
            "sent": sent_counts[pid],
            "received": received_counts[pid],
            "outgoing": sent_counts[pid],
            "incoming": received_counts[pid],
        }
        for pid in process_ids
    }
    result.summary = {
        "process_count": len(process_ids),
        "total_messages_sent": len(sent),
        "total_messages_delivered": len(delivered),
        "total_messages_received": len(received),
        "unique_message_ids": len({event.msg_id for event in sent}),
        "mean_delay": statistics.fmean(delays) if delays else None,
        "median_delay": statistics.median(delays) if delays else None,
        "min_delay": min(delays) if delays else None,
        "max_delay": max(delays) if delays else None,
        "delay_population_stddev": statistics.pstdev(delays) if delays else None,
        "out_of_order_delivery_count": count_out_of_order(result.events),
        "simulation_elapsed_time": result.elapsed_time,
        "traffic_matrix_total": sum(sum(row.values()) for row in traffic.values()),
    }


def count_out_of_order(events: list[Event]) -> int:
    """Count inversions in delivery order for each sender/receiver pair."""
    send_order: dict[tuple[str, str], list[str]] = defaultdict(list)
    deliver_order: dict[tuple[str, str], list[str]] = defaultdict(list)
    for event in sorted(events, key=lambda item: item.sequence):
        pair = (event.src, event.dst)
        if event.event_type == "send":
            send_order[pair].append(event.msg_id)
        elif event.event_type == "deliver":
            deliver_order[pair].append(event.msg_id)

    inversions = 0
    for pair, delivered in deliver_order.items():
        send_rank = {msg_id: index for index, msg_id in enumerate(send_order[pair])}
        ranks = [send_rank[msg_id] for msg_id in delivered if msg_id in send_rank]
        inversions += sum(
            1
            for index, rank in enumerate(ranks)
            for later_rank in ranks[index + 1 :]
            if later_rank < rank
        )
    return inversions


def _validate_clocks(result: SimulationResult) -> tuple[bool, str]:
    latest = {pid: 0 for pid in result.process_ids}
    event_by_message = {
        (event.msg_id, event.event_type): event
        for event in result.events
        if event.event_type in ("send", "recv")
    }
    for event in sorted(result.events, key=lambda item: item.sequence):
        if event.event_type not in ("send", "recv"):
            continue
        if event.process_id not in latest or event.process_clock is None:
            return False, f"{event.msg_id} has no valid process clock"
        previous = latest[event.process_id]
        if event.process_clock <= previous:
            return False, f"{event.process_id} clock did not strictly increase at {event.msg_id}"
        if event.event_type == "send":
            if event.process_clock != previous + 1:
                return False, f"{event.process_id} send clock increment is inconsistent"
        elif event.process_clock != max(previous, event.message_lamport) + 1:
            return False, f"{event.process_id} receive clock rule is inconsistent"
        latest[event.process_id] = event.process_clock

    for message in result.messages:
        send = event_by_message.get((message.msg_id, "send"))
        receive = event_by_message.get((message.msg_id, "recv"))
        if send is None or receive is None:
            continue
        if receive.process_clock is None or receive.process_clock <= send.message_lamport:
            return False, f"receive clock for {message.msg_id} is not greater than send clock"
    if latest != result.final_clocks:
        return False, "final process clocks do not match the event log"
    return True, "All logged clocks increase and follow the Lamport rules"


def build_validation_results(result: SimulationResult) -> list[ValidationCheck]:
    sent = [event for event in result.events if event.event_type == "send"]
    delivered = [event for event in result.events if event.event_type == "deliver"]
    received = [event for event in result.events if event.event_type == "recv"]
    expected_total = result.config.process_count * result.config.messages_per_process
    sent_ids = [event.msg_id for event in sent]
    delivery_counts = Counter(event.msg_id for event in delivered)
    receive_counts = Counter(event.msg_id for event in received)
    known_ids = set(sent_ids)
    valid_destinations = all(
        event.src in result.process_ids
        and event.dst in result.process_ids
        and event.src != event.dst
        for event in sent
    )
    clocks_ok, clock_message = _validate_clocks(result)
    checks = [
        ValidationCheck(
            "TC-01",
            "Configured message count is sent",
            f"{expected_total} send events",
            f"{len(sent)} send events",
            len(sent) == expected_total,
        ),
        ValidationCheck(
            "TC-02",
            "Every message is delivered and received exactly once",
            "One delivery and one receive per sent message",
            (
                f"{len(delivered)} deliveries, {len(received)} receives; "
                f"delivery duplicates={sum(n > 1 for n in delivery_counts.values())}, "
                f"receive duplicates={sum(n > 1 for n in receive_counts.values())}"
            ),
            len(delivered) == expected_total
            and len(received) == expected_total
            and set(delivery_counts) == known_ids
            and set(receive_counts) == known_ids
            and all(delivery_counts[msg_id] == receive_counts[msg_id] == 1 for msg_id in known_ids),
        ),
        ValidationCheck(
            "TC-03",
            "Message identifiers are unique",
            f"{expected_total} unique identifiers",
            f"{len(set(sent_ids))} unique identifiers",
            len(set(sent_ids)) == len(sent_ids) == expected_total,
        ),
        ValidationCheck(
            "TC-04",
            "Destinations are valid and exclude self-messaging",
            "Every message targets a different configured process",
            "All destinations valid" if valid_destinations else "Invalid or self destination found",
            valid_destinations,
        ),
        ValidationCheck(
            "TC-05",
            "Lamport clocks are monotonic and causally consistent",
            "Strictly increasing local clocks; receive follows max(local, send)+1",
            clock_message,
            clocks_ok,
        ),
        ValidationCheck(
            "TC-06",
            "Traffic matrix agrees with sent message count",
            f"Matrix total = {expected_total}",
            f"Matrix total = {sum(sum(row.values()) for row in result.traffic_matrix.values())}",
            sum(sum(row.values()) for row in result.traffic_matrix.values()) == expected_total,
        ),
        ValidationCheck(
            "TC-07",
            "Network delay values respect configured bounds",
            f"{result.config.min_delay:g} to {result.config.max_delay:g} seconds",
            (
                "No deliveries to check"
                if not delivered
                else f"Observed {min(event.delay for event in delivered if event.delay is not None):.4f} "
                f"to {max(event.delay for event in delivered if event.delay is not None):.4f} seconds"
            ),
            all(
                event.delay is not None
                and result.config.min_delay - 1e-12 <= event.delay <= result.config.max_delay + 1e-12
                for event in delivered
            ),
        ),
        ValidationCheck(
            "TC-08",
            "Out-of-order deliveries are measured, not required",
            "Report the observed inversion count",
            f"{result.summary.get('out_of_order_delivery_count', count_out_of_order(result.events))} "
            "out-of-order deliveries observed",
            True,
        ),
    ]
    return checks


def events_to_csv(events: list[Event]) -> str:
    columns = [
        "physical_time",
        "event_type",
        "src",
        "dst",
        "msg_id",
        "payload",
        "message_lamport",
        "process_id",
        "process_clock",
        "delay",
        "sequence",
    ]
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    writer.writerows(event.to_dict() for event in events)
    return output.getvalue()


def traffic_matrix_to_csv(result: SimulationResult) -> str:
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(["source", *result.process_ids, "total"])
    for src in result.process_ids:
        row = result.traffic_matrix[src]
        writer.writerow([src, *(row[dst] for dst in result.process_ids), sum(row.values())])
    writer.writerow(["total", *(sum(result.traffic_matrix[src][dst] for src in result.process_ids) for dst in result.process_ids), result.summary["traffic_matrix_total"]])
    return output.getvalue()


def validations_to_csv(result: SimulationResult) -> str:
    columns = ["test_id", "description", "expected", "actual", "status"]
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    writer.writerows(
        {
            "test_id": check.test_id,
            "description": check.description,
            "expected": check.expected,
            "actual": check.actual,
            "status": "PASS" if check.passed else "FAIL",
        }
        for check in result.validations
    )
    return output.getvalue()


def report_to_json(result: SimulationResult) -> str:
    return json.dumps(result.to_dict(), indent=2, allow_nan=False)


def event_frame_rows(events: list[Event]) -> list[dict[str, Any]]:
    """Return records in display order; the stored event list remains unchanged."""
    return [event.to_dict() for event in events]
