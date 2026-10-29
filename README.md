# Distributed Process Communication Simulator

A local-first Streamlit application that simulates asynchronous unicast messaging between three or four independent logical processes. It demonstrates Lamport logical clocks, randomized network delay, message reordering, event-log analysis, and report downloads.

## Run from GitHub

GitHub repositories store the source code and can run automated tests with GitHub Actions. A normal GitHub repository page does **not** host a continuously running Streamlit app. To run the dashboard online using the GitHub repository as its source:

1. Push this project to a GitHub repository. Keep `app.py`, `simulator.py`, `network.py`, `models.py`, `analysis.py`, and `requirements.txt` at the repository root.
2. Sign in to [Streamlit Community Cloud](https://share.streamlit.io/) with GitHub and grant access to the repository.
3. Select **Create app**, choose the repository and branch, set the main file path to `app.py`, and deploy.
4. Open the generated app URL. Every deployment uses the dependencies in `requirements.txt`; no API key or secrets are required.

The included `.github/workflows/tests.yml` runs the pytest suite on pushes and pull requests with Python 3.11 and 3.12. GitHub Actions validates changes; it is not the always-on web host. Streamlit Community Cloud is the simplest free way to publish the dashboard from GitHub. GitHub Codespaces can also run the app interactively in a development environment.

## Run locally on Windows

To create and open a fresh project folder from PowerShell:

```powershell
mkdir distributed-process-communication-simulator
cd distributed-process-communication-simulator
code .
```

Place the project files from this repository in that folder, then open **Terminal → New Terminal** in VS Code and run:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
streamlit run app.py
```

If PowerShell blocks activation, enable it only for the current terminal:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process
.\.venv\Scripts\Activate.ps1
```

The browser normally opens at `http://localhost:8501`. To run all automated tests:

```powershell
python -m pytest -v
```

## Run in GitHub Codespaces

1. Open the repository on GitHub and select **Code → Codespaces → Create codespace on main**.
2. In the integrated terminal, install and launch:

```bash
python -m pip install -r requirements.txt
streamlit run app.py --server.address 0.0.0.0
```

3. In the Codespaces **Ports** tab, open port `8501` in the browser. Codespaces may ask whether to make the port private or public; choose the setting appropriate for your demonstration.

## How to use

Use the sidebar to select three or four processes, outgoing message count per process, delay range, random seed, and timeout. Select **Start Simulation**. The dashboard produces a fresh simulation run and displays:

- Send, delivery, and receive counts, observed delay summaries, process statistics, and final Lamport clocks.
- A searchable event table.
- A message sequence diagram, traffic matrix, per-process load chart, per-message delay chart and histogram, and Lamport timeline.
- Download buttons for the current run's event log CSV, simulation report JSON, traffic matrix CSV, and validation CSV.

The delay statistics are measured from the configured simulated delay assigned to each delivery. Physical elapsed time is measured independently and also includes task scheduling and application overhead.

## Architecture

```text
app.py                  Streamlit controls, charts, event table, and downloads
simulator.py            LogicalProcess and SimulationController
network.py              Independent asyncio delivery tasks and simulated delay
models.py               Config, Message, Event, ValidationCheck, and result types
analysis.py              Statistics, traffic matrix, validations, and exports
tests/
  test_simulator.py      Delivery, timeout, seed, and edge-case tests
  test_lamport.py        Lamport clock behavior tests
  test_analysis.py       Analytics and report export tests
.github/workflows/
  tests.yml              GitHub Actions pytest workflow
```

Each process owns an `asyncio.Queue` inbox and a private Lamport clock. All messages pass through `Network`; senders never update another process directly. Sender tasks and receiver loops run concurrently. The controller waits for all senders, all network deliveries, and all inbox queue work before putting one shutdown sentinel into each receiver queue. It does not predict or assume equal receive counts.

## Algorithm notes

### Message sending and receiving

```text
On send at process P:
    P.clock = P.clock + 1
    create message with sender timestamp P.clock
    record send event
    schedule asynchronous network delivery

On receive at process Q:
    Q.clock = max(Q.clock, message.sender_timestamp) + 1
    record receive event with Q.clock
```

Message destinations are selected from peers excluding the sender using a seeded random generator. Every message receives a separate reproducible delay. Deliveries are independently scheduled, allowing a later message to arrive first when its delay is shorter.

### Physical time versus logical time

- **Physical elapsed time** is a measured duration from simulation start. Delivery delay and chart positions use physical elapsed seconds.
- **Lamport logical time** is an integer counter used to preserve event ordering constraints. It is not a clock reading, does not measure duration, and must not be converted to seconds.
- If event A causally precedes event B, Lamport(A) is less than Lamport(B). The converse is not guaranteed: a lower Lamport timestamp does not prove a causal relationship.
- A receive event stores the receiver's updated clock separately from the original message send timestamp.

### Network jitter and reordering

Network delay is sampled uniformly from the configured inclusive range (using a deterministic per-message seed). The displayed jitter is the population standard deviation of observed simulated delivery delays. Reordering is counted only among messages from the same source to the same destination and is an observed outcome, not a guaranteed feature of every random run.

### Traffic matrix

Rows represent source processes and columns represent destination processes. The values are counts derived from the messages in the current run. The diagonal is zero because self-messaging is excluded; summing the matrix gives the total number of sent messages.

## Validation and tests

The dashboard checks message counts and unique IDs, exactly-once delivery and receipt, valid destinations, Lamport clock progression and receive rules, traffic-matrix totals, and observed delay bounds. Reordering is reported but is not treated as a pass/fail requirement.

Automated tests cover three- and four-process runs, larger message volume, zero messages and empty event logs, deterministic destinations and delays, delay bounds, Lamport clocks, exactly-once handling, exports, traffic matrix totals, process statistics, and timeout cleanup. Run them with:

```powershell
python -m pytest -v
```

The GitHub Actions workflow runs the same command for both supported Python versions on every push and pull request. Read the green **Tests** check on the commit or pull request to confirm the CI run succeeded.

## Project report and viva preparation

### 1. Title page

**Distributed Process Communication Simulator**  
Third-year Computer Science and Engineering project  
Implementation: Python, asyncio, Streamlit, Plotly, Pandas

### 2. Index

Problem definition · Architecture · Components · Communication model · Algorithms · Source code · Demonstration · Tests · Results · Limitations · Future enhancements · Conclusion · Viva questions.

### 3. Problem definition

Independent processes do not share memory and coordinate by exchanging messages over a network. Different propagation delays can change arrival order. A global physical clock is not sufficient to describe causality, so each process maintains a logical clock and logs its communication events.

### 4–5. Architecture and component responsibilities

`LogicalProcess` owns its clock, private inbox, peer list, and counters. `Network` schedules delayed delivery tasks and enqueues messages. `SimulationController` starts all senders and receivers and shuts them down safely after delivery and queue processing complete. `analysis.py` calculates statistics, traffic, validations, and downloadable reports. `app.py` provides controls and data-driven plots.

### 6–7. Communication model and algorithm

Communication is asynchronous, unicast, and all-to-all in reachability: every sender randomly chooses a peer, not itself. For N processes and M outgoing messages per process, total expected messages are N × M. Lamport send and receive updates follow the pseudocode above. Network delay does not mutate the original sender timestamp.

### 8. Source code explanation

Start with `models.py` for record formats, follow `simulator.py` for send/receive and lifecycle behavior, then `network.py` for delayed deliveries, `analysis.py` for validation, and `app.py` for the interactive UI. The test modules document expected behavior with runnable examples.

### 9. Application screenshots

No screenshots are committed because they should represent an actual local or deployed run. Capture the dashboard after running `streamlit run app.py` or opening the Streamlit Community Cloud deployment, then add your own screenshots to the project report.

### 10–11. Test cases and results

Run `python -m pytest -v` locally or inspect the GitHub Actions **Tests** workflow for the actual result. The dashboard validation panel shows message totals, clock correctness, matrix consistency, delay bounds, and run-specific reorder count. Do not copy illustrative counts as if they were measured results; record the counts from the run you demonstrate.

### 12. Limitations

This is a single-machine simulation, not a real multi-host network. `asyncio.sleep` models latency without packet loss, bandwidth contention, partitions, authentication, or transport protocol behavior. The seeded delay assignment is reproducible, but physical elapsed times are affected by the operating system and runtime scheduling.

### 13. Future enhancements

Add configurable loss and retry policies, process failure and recovery, topology-based routing, bandwidth limits, causal vector clocks, larger process counts, saved run comparisons, and richer test coverage for simulated faults.

### 14. Conclusion

The simulator demonstrates message-passing concurrency and the distinction between physical time and logical event ordering. Its event log, per-process metrics, traffic matrix, plots, validation checks, and exports make a run inspectable and repeatable.

### Viva questions and concise answers

1. **What is a distributed system?** Independent computers or processes that coordinate by exchanging messages.
2. **Why do processes use message passing?** They have separate memory and cannot rely on shared state.
3. **What is asynchronous communication?** A sender can continue without waiting for immediate receiver processing.
4. **What is a Lamport clock?** A logical counter that captures a partial ordering consistent with causality.
5. **What happens on a Lamport send?** Increment the local clock, then attach it to the message.
6. **What happens on receive?** Set the clock to `max(local, message timestamp) + 1`.
7. **Does a Lamport clock measure real time?** No, it is a logical ordering value.
8. **Does A's smaller Lamport timestamp prove A caused B?** No. Causality implies increasing timestamps, but the reverse is not guaranteed.
9. **What is network latency?** The time taken for a message to propagate through the modeled network.
10. **What is jitter?** Variation in delay; this project reports population standard deviation.
11. **What is message reordering?** A later sent message arrives before an earlier one on the same sender-recipient path.
12. **Why can messages reorder here?** Each delivery is scheduled independently with its own randomized delay.
13. **What is a traffic matrix?** A source-by-destination table of message counts.
14. **How does the simulator avoid a fixed receive count?** It awaits all deliveries and queue work, then sends one shutdown sentinel per process.
15. **How is message loss checked?** Each sent ID must occur exactly once in both delivery and receive events.
16. **What does the random seed guarantee?** Repeatable peer choices and simulated delays, not identical measured wall-clock timing.
17. **Why keep message and receiver Lamport values separately?** The message timestamp must preserve the sender's clock at send time.
18. **What does GitHub Actions do here?** It automatically installs dependencies and runs pytest on pushes and pull requests.
19. **Can the GitHub repository page host Streamlit?** No. GitHub stores source and runs CI; Streamlit Community Cloud or Codespaces runs the application.
20. **What are the simulator's limitations?** It models delay and concurrency, but not a real network, loss, bandwidth, partitions, or multi-host failures.

## Troubleshooting

- **`streamlit` is not recognized:** activate `.venv` and run `python -m pip install -r requirements.txt` again.
- **PowerShell activation is blocked:** use the process-scoped execution policy command above, or run `.venv\Scripts\python.exe -m streamlit run app.py`.
- **Port 8501 is in use:** run `streamlit run app.py --server.port 8502`.
- **No messages appear:** check that messages per process is greater than zero; a zero-message run is valid and produces an empty event log.
- **Deployment dependency error:** confirm `requirements.txt` is in the repository root and trigger a redeploy from Streamlit Community Cloud.
- **GitHub Actions fails:** open the failed workflow run, expand the `Run test suite` step, and use the failing test output to identify the regression.
