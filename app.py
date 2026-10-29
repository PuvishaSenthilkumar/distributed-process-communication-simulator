"""Streamlit dashboard for the Distributed Process Communication Simulator."""

from __future__ import annotations

import asyncio

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from analysis import (
    build_validation_results,
    event_frame_rows,
    events_to_csv,
    report_to_json,
    traffic_matrix_to_csv,
    validations_to_csv,
)
from models import SimulationConfig, SimulationResult
from simulator import run_simulation


st.set_page_config(
    page_title="Distributed Process Communication Simulator",
    page_icon="🔗",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    :root { color-scheme: dark; }
    .stApp { background: linear-gradient(150deg, #07111f 0%, #0b1728 52%, #0a1422 100%); }
    [data-testid="stSidebar"] { background: #0c1929; border-right: 1px solid #1c344b; }
    [data-testid="stMetric"] {
      background: linear-gradient(145deg, #102338, #0d1b2b);
      border: 1px solid #1e3b54; border-radius: 12px; padding: 14px 16px;
    }
    [data-testid="stMetricLabel"] { color: #a7bfd2; }
    h1, h2, h3 { color: #e8f4ff; }
    .hero-caption { color: #a9bfd2; font-size: 1.02rem; margin-top: -0.5rem; }
    .status-ready { color: #a7bfd2; }
    .status-complete { color: #52dfb2; font-weight: 700; }
    .status-failed { color: #ff828c; font-weight: 700; }
    div[data-testid="stVerticalBlockBorderWrapper"] { border-color: #1c344b; }
    </style>
    """,
    unsafe_allow_html=True,
)


def _format_seconds(value: float | None) -> str:
    return "—" if value is None else f"{value:.4f} s"


def _sequence_chart(result: SimulationResult) -> go.Figure:
    positions = {pid: index for index, pid in enumerate(result.process_ids)}
    events = {event.msg_id: event for event in result.events}
    sends = {event.msg_id: event for event in result.events if event.event_type == "send"}
    deliveries = {event.msg_id: event for event in result.events if event.event_type == "deliver"}
    fig = go.Figure()
    max_time = max((event.physical_time for event in result.events), default=0.1)
    for pid, x in positions.items():
        fig.add_trace(
            go.Scatter(
                x=[x, x],
                y=[0, max_time * 1.08 if max_time else 0.1],
                mode="lines",
                line={"color": "#31516b", "width": 2, "dash": "dot"},
                name=pid,
                hoverinfo="skip",
                showlegend=False,
            )
        )
        fig.add_annotation(
            x=x, y=max_time * 1.1 if max_time else 0.12, text=pid,
            showarrow=False, font={"color": "#a9d8f4", "size": 13},
        )
    for msg_id, send in sends.items():
        delivery = deliveries.get(msg_id)
        if delivery is None:
            continue
        x0, x1 = positions[send.src], positions[send.dst]
        color = "#40c7e8" if x1 > x0 else "#ae91ff"
        fig.add_annotation(
            x=x1,
            y=delivery.physical_time,
            ax=x0,
            ay=send.physical_time,
            xref="x",
            yref="y",
            axref="x",
            ayref="y",
            text="",
            showarrow=True,
            arrowhead=3,
            arrowsize=1,
            arrowwidth=1.5,
            arrowcolor=color,
        )
        midpoint_x = (x0 + x1) / 2
        midpoint_y = (send.physical_time + delivery.physical_time) / 2
        message = next(item for item in result.messages if item.msg_id == msg_id)
        fig.add_trace(
            go.Scatter(
                x=[midpoint_x],
                y=[midpoint_y],
                mode="markers+text",
                marker={"size": 8, "color": color, "opacity": 0.9},
                text=[msg_id.rsplit("-", 1)[0]],
                textposition="top center",
                textfont={"size": 9, "color": "#d7e9f5"},
                customdata=[[
                    message.src,
                    message.dst,
                    message.payload,
                    message.delay,
                    message.lamport,
                    send.physical_time,
                    delivery.physical_time,
                ]],
                hovertemplate=(
                    "<b>%{text}</b><br>Source: %{customdata[0]}<br>"
                    "Destination: %{customdata[1]}<br>Payload: %{customdata[2]}<br>"
                    "Simulated delay: %{customdata[3]:.4f} s<br>"
                    "Send Lamport: %{customdata[4]}<br>"
                    "Send elapsed: %{customdata[5]:.4f} s<br>"
                    "Delivery elapsed: %{customdata[6]:.4f} s<extra></extra>"
                ),
                showlegend=False,
            )
        )
    fig.update_layout(
        title="Messages across process lifelines",
        xaxis={
            "tickmode": "array",
            "tickvals": list(positions.values()),
            "ticktext": result.process_ids,
            "range": [-0.5, len(positions) - 0.5],
            "title": "Logical process",
            "gridcolor": "#172d40",
        },
        yaxis={"title": "Physical elapsed time (seconds)", "gridcolor": "#172d40"},
        height=520,
        margin={"l": 35, "r": 25, "t": 70, "b": 45},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": "#d7e9f5"},
        hoverlabel={"bgcolor": "#102338"},
    )
    return fig


def _traffic_chart(result: SimulationResult) -> go.Figure:
    matrix = [[result.traffic_matrix[src][dst] for dst in result.process_ids] for src in result.process_ids]
    fig = go.Figure(
        go.Heatmap(
            z=matrix,
            x=result.process_ids,
            y=result.process_ids,
            text=matrix,
            texttemplate="%{text}",
            colorscale=[[0, "#102338"], [0.5, "#167b9b"], [1, "#62e0d1"]],
            colorbar={"title": "Messages"},
            hovertemplate="From %{y} to %{x}: %{z} message(s)<extra></extra>",
        )
    )
    fig.update_layout(
        title="Traffic matrix · rows send, columns receive",
        xaxis_title="Destination process",
        yaxis_title="Source process",
        height=390,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": "#d7e9f5"},
    )
    return fig


def _delay_charts(result: SimulationResult) -> tuple[go.Figure, go.Figure]:
    records = [
        {"Message": msg.msg_id, "Delay (s)": msg.delay}
        for msg in result.messages
        if msg.delay is not None
    ]
    bar = go.Figure()
    histogram = go.Figure()
    if records:
        bar.add_trace(
            go.Bar(
                x=[row["Message"] for row in records],
                y=[row["Delay (s)"] for row in records],
                marker_color="#40c7e8",
                hovertemplate="%{x}<br>Delay: %{y:.4f} s<extra></extra>",
            )
        )
        histogram.add_trace(
            go.Histogram(
                x=[row["Delay (s)"] for row in records],
                nbinsx=min(12, max(4, len(records))),
                marker_color="#ae91ff",
                opacity=0.88,
                hovertemplate="Delay range: %{x}<br>Messages: %{y}<extra></extra>",
            )
        )
    for chart, title, x_title in (
        (bar, "Observed delay per message", "Message ID"),
        (histogram, "Observed delay distribution", "Simulated delay (seconds)"),
    ):
        chart.update_layout(
            title=title,
            xaxis_title=x_title,
            yaxis_title="Messages" if chart is histogram else "Delay (seconds)",
            height=390,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font={"color": "#d7e9f5"},
            showlegend=False,
        )
    return bar, histogram


def _load_chart(result: SimulationResult) -> go.Figure:
    fig = go.Figure()
    for label, key, color in (
        ("Sent", "sent", "#40c7e8"),
        ("Received", "received", "#ae91ff"),
    ):
        fig.add_trace(
            go.Bar(
                name=label,
                x=result.process_ids,
                y=[result.process_stats[pid][key] for pid in result.process_ids],
                marker_color=color,
            )
        )
    fig.update_layout(
        barmode="group",
        title="Per-process communication load",
        xaxis_title="Process",
        yaxis_title="Messages",
        height=390,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": "#d7e9f5"},
    )
    return fig


def _lamport_chart(result: SimulationResult) -> go.Figure:
    fig = go.Figure()
    colors = ["#40c7e8", "#ae91ff", "#4ed8a8", "#f7b955"]
    for index, pid in enumerate(result.process_ids):
        process_events = [
            event
            for event in result.events
            if event.process_id == pid and event.event_type in ("send", "recv")
        ]
        process_events.sort(key=lambda event: event.sequence)
        if process_events:
            fig.add_trace(
                go.Scatter(
                    x=[event.physical_time for event in process_events],
                    y=[event.process_clock for event in process_events],
                    mode="lines+markers",
                    name=pid,
                    line={"color": colors[index % len(colors)]},
                    customdata=[[event.event_type, event.msg_id] for event in process_events],
                    hovertemplate=(
                        f"<b>{pid}</b><br>Physical elapsed: %{{x:.4f}} s<br>"
                        "Lamport clock: %{y}<br>Event: %{customdata[0]}<br>"
                        "Message: %{customdata[1]}<extra></extra>"
                    ),
                )
            )
    fig.update_layout(
        title="Lamport logical clocks (not physical time)",
        xaxis_title="Physical elapsed time (seconds)",
        yaxis_title="Lamport clock value",
        height=440,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": "#d7e9f5"},
    )
    return fig


def _show_result(result: SimulationResult) -> None:
    summary = result.summary
    st.markdown(
        f'<p class="status-complete">● Simulation completed in '
        f'{result.elapsed_time:.4f} seconds</p>',
        unsafe_allow_html=True,
    )
    columns = st.columns(4)
    metric_data = [
        ("Processes", str(summary["process_count"])),
        ("Messages sent", str(summary["total_messages_sent"])),
        ("Delivered", str(summary["total_messages_delivered"])),
        ("Received", str(summary["total_messages_received"])),
        ("Mean simulated delay", _format_seconds(summary["mean_delay"])),
        ("Maximum simulated delay", _format_seconds(summary["max_delay"])),
        ("Network jitter · population σ", _format_seconds(summary["delay_population_stddev"])),
        ("Out-of-order deliveries", str(summary["out_of_order_delivery_count"])),
    ]
    for index, (label, value) in enumerate(metric_data):
        with columns[index % 4]:
            st.metric(label, value)
    st.caption(
        "Delay figures are calculated from each delivery's configured simulated sleep duration. "
        "Physical elapsed time is measured separately; OS scheduling can make it longer."
    )

    st.subheader("Process status")
    process_columns = st.columns(len(result.process_ids))
    for col, pid in zip(process_columns, result.process_ids):
        stats = result.process_stats[pid]
        with col:
            st.metric(pid, f"Clock {result.final_clocks[pid]}")
            st.caption(
                f"Sent {stats['sent']} · received {stats['received']} · "
                f"outgoing {stats['outgoing']} · incoming {stats['incoming']}"
            )

    st.subheader("Message sequence")
    st.plotly_chart(_sequence_chart(result), use_container_width=True)
    st.caption("Arrow positions use physical elapsed time; message labels retain the original sender clock.")

    left, right = st.columns(2)
    with left:
        st.plotly_chart(_traffic_chart(result), use_container_width=True)
        st.dataframe(pd.DataFrame(result.traffic_matrix).T, use_container_width=True)
    with right:
        st.plotly_chart(_load_chart(result), use_container_width=True)
        highest_out = max(result.process_stats, key=lambda pid: result.process_stats[pid]["sent"])
        highest_in = max(result.process_stats, key=lambda pid: result.process_stats[pid]["received"])
        st.caption(
            f"Highest outgoing: {highest_out} ({result.process_stats[highest_out]['sent']}) · "
            f"highest incoming: {highest_in} ({result.process_stats[highest_in]['received']}) · "
            f"system volume: {summary['total_messages_sent']} messages"
        )

    delay_left, delay_right = st.columns(2)
    bar, histogram = _delay_charts(result)
    with delay_left:
        st.plotly_chart(bar, use_container_width=True)
    with delay_right:
        st.plotly_chart(histogram, use_container_width=True)
    delay_stats = st.columns(5)
    for col, label, key in zip(
        delay_stats,
        ("Mean", "Median", "Minimum", "Maximum", "Population σ"),
        ("mean_delay", "median_delay", "min_delay", "max_delay", "delay_population_stddev"),
    ):
        col.metric(label, _format_seconds(summary[key]))
    st.caption(
        f"Configured delay limits: {result.config.min_delay:g}–"
        f"{result.config.max_delay:g} seconds."
    )

    st.plotly_chart(_lamport_chart(result), use_container_width=True)
    st.caption(
        "Lamport values order events logically. They are not timestamps and must not be "
        "converted to seconds."
    )

    st.subheader("Validation results")
    validation_rows = [
        {
            "Test ID": check.test_id,
            "Description": check.description,
            "Expected": check.expected,
            "Actual": check.actual,
            "Status": "PASS" if check.passed else "FAIL",
        }
        for check in result.validations
    ]
    st.dataframe(pd.DataFrame(validation_rows), use_container_width=True, hide_index=True)

    st.subheader("Event log")
    event_frame = pd.DataFrame(event_frame_rows(result.events))
    query = st.text_input("Search events", placeholder="Search process, event type, message ID, or payload")
    if query:
        mask = event_frame.astype(str).apply(
            lambda column: column.str.contains(query, case=False, na=False)
        ).any(axis=1)
        event_frame = event_frame[mask]
    st.dataframe(
        event_frame.sort_values("sequence"),
        use_container_width=True,
        hide_index=True,
        column_config={
            "physical_time": st.column_config.NumberColumn("Physical elapsed (s)", format="%.6f"),
            "delay": st.column_config.NumberColumn("Simulated delay (s)", format="%.6f"),
        },
    )

    st.subheader("Download current run")
    dl1, dl2, dl3, dl4 = st.columns(4)
    with dl1:
        st.download_button(
            "Event log · CSV",
            events_to_csv(result.events),
            file_name="event-log.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with dl2:
        st.download_button(
            "Simulation report · JSON",
            report_to_json(result),
            file_name="simulation-report.json",
            mime="application/json",
            use_container_width=True,
        )
    with dl3:
        st.download_button(
            "Traffic matrix · CSV",
            traffic_matrix_to_csv(result),
            file_name="traffic-matrix.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with dl4:
        st.download_button(
            "Validation results · CSV",
            validations_to_csv(result),
            file_name="validation-results.csv",
            mime="text/csv",
            use_container_width=True,
        )


st.title("Distributed Process Communication Simulator")
st.markdown(
    '<p class="hero-caption">Explore asynchronous message passing, network delay, and '
    "Lamport logical clocks across independent processes.</p>",
    unsafe_allow_html=True,
)

with st.sidebar:
    st.header("Simulation configuration")
    process_count = st.selectbox("Number of processes", [3, 4], index=0)
    messages_per_process = st.number_input(
        "Messages per process", min_value=0, max_value=500, value=5, step=1
    )
    min_delay = st.number_input("Minimum network delay (s)", min_value=0.0, value=0.05, step=0.01, format="%.3f")
    max_delay = st.number_input("Maximum network delay (s)", min_value=0.0, value=0.35, step=0.01, format="%.3f")
    seed = st.number_input("Random seed", min_value=0, max_value=2_147_483_647, value=7, step=1)
    timeout = st.number_input("Simulation timeout (s)", min_value=0.1, value=30.0, step=1.0, format="%.1f")
    start_clicked = st.button("▶  Start Simulation", type="primary", use_container_width=True)
    validate_clicked = st.button("✓  Run Validation Tests", use_container_width=True)
    reset_clicked = st.button("↻  Reset Simulation", use_container_width=True)

if "simulation_result" not in st.session_state:
    st.session_state.simulation_result = None
if "simulation_status" not in st.session_state:
    st.session_state.simulation_status = "Ready"

if reset_clicked:
    st.session_state.simulation_result = None
    st.session_state.simulation_status = "Ready"

if start_clicked or validate_clicked:
    try:
        config = SimulationConfig(
            process_count=int(process_count),
            messages_per_process=int(messages_per_process),
            min_delay=float(min_delay),
            max_delay=float(max_delay),
            seed=int(seed),
            timeout=float(timeout),
        )
        config.validate()
        if validate_clicked and not start_clicked and st.session_state.simulation_result is not None:
            result = st.session_state.simulation_result
            result.validations = build_validation_results(result)
            st.session_state.simulation_status = "Completed"
            st.success("Validation checks were rerun against the current simulation.")
        else:
            st.session_state.simulation_status = "Running"
            with st.spinner("Running independent processes and asynchronous network deliveries…"):
                st.session_state.simulation_result = run_simulation(config)
            st.session_state.simulation_status = "Completed"
    except (ValueError, RuntimeError, TimeoutError) as error:
        st.session_state.simulation_status = "Failed"
        st.session_state.simulation_result = None
        st.error(f"Simulation failed: {error}")

status = st.session_state.simulation_status
if status == "Ready":
    st.markdown('<p class="status-ready">● Ready · configure a run in the sidebar.</p>', unsafe_allow_html=True)
elif status == "Failed":
    st.markdown('<p class="status-failed">● Failed · correct the configuration or inspect the error above.</p>', unsafe_allow_html=True)

current_result = st.session_state.simulation_result
if current_result is not None:
    _show_result(current_result)
else:
    st.info(
        "Start a simulation to populate the event log, traffic matrix, delay analysis, "
        "process-load comparison, Lamport timeline, and downloadable reports."
    )

