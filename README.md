# QueueCTL — Background Job Queue System CLI

**QueueCTL** (`queuectl`) is a production-grade, CLI-based background job queue system built with **Node.js** and **Express.js**. It manages background job execution across parallel worker processes, provides automatic retries with exponential backoff, maintains a Dead Letter Queue (DLQ) for permanently failed jobs, and guarantees crash recovery under 60 seconds.

---

## Key Features

- ⚡ **Multi-Process Concurrency:** Run multiple worker processes in parallel from separate terminal sessions without duplicate execution.
- 🔒 **Atomic Concurrency Safety:** Powered by SQLite WAL transactions (`BEGIN IMMEDIATE`) to guarantee that no two workers claim the same job.
- 🔁 **Exponential Backoff Retries:** Failed jobs retry automatically with configurable backoff delay (`delay = base ^ attempts` seconds).
- 💀 **Dead Letter Queue (DLQ):** Moves jobs exceeding `max_retries` to DLQ (`dead` state) for manual operator review and re-enqueueing.
- 🛡️ **Crash Recovery:** Automatic stale worker heartbeat scanner detects `SIGKILL` or power crashes and recovers stuck jobs within 15 seconds.
- 🛑 **Graceful Shutdown:** Workers capture `SIGINT`/`SIGTERM` to finish in-flight jobs before exiting.
- 💻 **Strict CLI Contract:** Full compliance with `--json` stdout formatting for automated test suite compatibility.

---

## Installation & Setup

### Prerequisites
- **Node.js:** v18.0.0 or higher (compatible with Node 22/24 built-in `node:sqlite`)

### Setup Commands
```bash
# Clone repository
git clone https://github.com/balajirapolu/QueueCTL.git
cd QueueCTL

# Make queuectl executable
npm link # or node bin/queuectl.js
```

---

## CLI Reference & Usage Examples

### 1. Enqueueing Jobs
```bash
queuectl enqueue '{"id":"job1","command":"echo Hello World"}'
queuectl enqueue '{"id":"job2","command":"sleep 2","max_retries":5}'
```

### 2. Running Workers
```bash
# Start 3 worker processes in the foreground (blocks until stopped)
queuectl worker start --count 3

# Gracefully stop all workers from ANOTHER terminal session
queuectl worker stop
```

### 3. Checking System Status
```bash
queuectl status
```

### 4. Listing Jobs (with JSON support)
```bash
# Human readable list
queuectl list --state pending

# Raw JSON array output to stdout (strictly formatted for test suite)
queuectl list --state pending --json
```

### 5. Managing Dead Letter Queue (DLQ)
```bash
# View all dead jobs in DLQ
queuectl dlq list

# Re-enqueue a dead job back to pending (resets attempts to 0)
queuectl dlq retry job1
```

### 6. Configuration Management
```bash
# Set maximum retry count (persisted)
queuectl config set max-retries 3

# Set exponential backoff base (persisted)
queuectl config set backoff-base 2
```

---

## Architecture & Crash Recovery Overview

```
                   +--------------------------------+
                   |       queuectl CLI / API       |
                   +---------------+----------------+
                                   |
           +-----------------------+-----------------------+
           |                       |                       |
           v                       v                       v
   +---------------+       +---------------+       +---------------+
   |   Worker 1    |       |   Worker 2    |       |   Worker N    |
   | (OS Process)  |       | (OS Process)  |       | (OS Process)  |
   +-------+-------+       +-------+-------+       +-------+-------+
           |                       |                       |
           +-----------------------+-----------------------+
                                   |
                     BEGIN IMMEDIATE | SQLite WAL Lock
                                   v
                   +--------------------------------+
                   |  SQLite Database (queuectl.db) |
                   +--------------------------------+
```

### Crash Recovery Mechanism
If a worker process is terminated via `SIGKILL` (`kill -9`), no application cleanup handler runs. QueueCTL handles this automatically:
1. Workers update a `heartbeat_at` timestamp every 2 seconds while processing a job.
2. During each poll cycle, the recovery engine scans for jobs in `processing` state whose `heartbeat_at` is older than **15 seconds**.
3. Stale jobs are automatically recovered back to `failed` (applying exponential backoff) or moved to DLQ (`dead`) if retries are exhausted.
4. **Worst-case recovery time:** ~15-16 seconds (well within the 60-second requirement).

---

## Verification & Automated Test Suite

Run the comprehensive integration test suite verifying all 5 required test scenarios:

```bash
npm test
```

### Verified Scenarios
1. ✅ **Basic Job Completion:** Enqueued job executes successfully and transitions to `completed`.
2. ✅ **Failure & Exponential Backoff:** Failing job retries with delays (2s, 4s, 8s) and lands in DLQ (`dead`).
3. ✅ **Multi-Worker Concurrency:** Multiple workers process jobs in parallel with zero duplicate executions.
4. ✅ **SIGKILL Crash Recovery:** Worker killed mid-job is detected and recovered under 60 seconds.
5. ✅ **Restart Survival:** Job state and config survive full process and system restarts.

---

## Technical Decisions Document

Detailed answers to all architecture questions (concurrency proof, SIGKILL walkthrough, DLQ retry rationale, cross-process signaling, and priority queue extensibility) are documented in [DECISIONS.md](file:///c:/Users/Balaji%20Rapolu/OneDrive/Desktop/QueueCTL/DECISIONS.md).

---

## Demo Recording
- **Demo Video Link:** *(Link to be added after recording demo)*
