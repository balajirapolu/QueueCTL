import { exec } from 'node:child_process';
import { dbRun, dbGet, dbAll, dbTransaction } from './db.js';
import { JOB_STATES, formatJob } from './job.js';
import { getNumericConfig } from './config.js';
import { startControlServer, stopControlServer } from './server.js';
import { runCrashRecovery } from './recovery.js';

/**
 * Worker class executing background jobs atomically
 */
export class Worker {
  constructor(workerId = `worker-${process.pid}-${Math.random().toString(36).substring(2, 7)}`) {
    this.workerId = workerId;
    this.pid = process.pid;
    this.isStopping = false;
    this.currentJob = null;
    this.heartbeatTimer = null;
    this.runningChildProcess = null;
  }

  /**
   * Start worker loop
   */
  async start() {
    await this.registerWorker();

    // Setup signal handlers for graceful shutdown
    const shutdownHandler = async (signal) => {
      if (this.isStopping) return;
      this.isStopping = true;
      console.log(`Worker [${this.workerId}] received ${signal}. Initiating graceful shutdown...`);

      if (this.currentJob) {
        console.log(`Worker [${this.workerId}] waiting for in-flight job '${this.currentJob.id}' to complete...`);
      } else {
        await this.unregisterWorker();
        process.exit(0);
      }
    };

    process.on('SIGINT', () => shutdownHandler('SIGINT'));
    process.on('SIGTERM', () => shutdownHandler('SIGTERM'));

    this.runLoop();
  }

  /**
   * Worker continuous polling loop
   */
  async runLoop() {
    while (!this.isStopping) {
      try {
        // Run periodic crash recovery check to pick up crashed worker jobs
        await runCrashRecovery();

        const job = await this.claimNextJob();
        if (job) {
          this.currentJob = job;
          await this.executeJob(job);
          this.currentJob = null;

          if (this.isStopping) {
            console.log(`Worker [${this.workerId}] in-flight job finished. Graceful shutdown complete.`);
            await this.unregisterWorker();
            process.exit(0);
          }
        } else {
          // No pending/retryable job available, sleep briefly
          await new Promise((r) => setTimeout(r, 500));
        }
      } catch (err) {
        console.error(`Worker [${this.workerId}] loop error:`, err.message);
        await new Promise((r) => setTimeout(r, 1000));
      }
    }

    await this.unregisterWorker();
  }

  /**
   * Claim next pending or retryable job atomically across OS processes
   */
  async claimNextJob() {
    return await dbTransaction(async (db) => {
      const now = new Date();
      const nowIso = now.toISOString();

      // Find candidate job: pending OR failed with elapsed backoff delay
      const candidates = db.prepare(`
        SELECT * FROM jobs 
        WHERE state = 'pending' 
           OR (state = 'failed' AND last_run_at IS NOT NULL)
        ORDER BY created_at ASC
      `).all();

      const backoffBase = await getNumericConfig('backoff-base');

      for (const row of candidates) {
        if (row.state === JOB_STATES.FAILED) {
          // Check if backoff delay has elapsed: delay = base ^ attempts seconds
          const attempts = Number(row.attempts);
          const delaySec = Math.pow(Number(backoffBase), Math.max(0, attempts - 1));
          const lastRunTime = new Date(row.last_run_at).getTime();
          const elapsedSec = (now.getTime() - lastRunTime) / 1000;

          if (elapsedSec < delaySec) {
            continue; // Backoff delay has not elapsed yet
          }
        }

        // Atomically claim candidate job
        const updateStmt = db.prepare(`
          UPDATE jobs 
          SET state = ?, locked_by = ?, heartbeat_at = ?, updated_at = ?, last_run_at = ?
          WHERE id = ? AND (state = 'pending' OR state = 'failed')
        `);

        const result = updateStmt.run(
          JOB_STATES.PROCESSING,
          this.workerId,
          nowIso,
          nowIso,
          nowIso,
          row.id
        );

        if (result.changes > 0) {
          const claimedRow = db.prepare('SELECT * FROM jobs WHERE id = ?').get(row.id);
          return formatJob(claimedRow);
        }
      }

      return null;
    });
  }

  /**
   * Execute job command via shell and update status based on exit code
   */
  async executeJob(job) {
    console.log(`Worker [${this.workerId}] processing job '${job.id}' -> command: "${job.command}"`);

    // Start periodic heartbeat updates while processing
    this.startHeartbeat(job.id);

    const startTime = Date.now();

    return new Promise((resolve) => {
      this.runningChildProcess = exec(job.command, async (error, stdout, stderr) => {
        this.stopHeartbeat();
        this.runningChildProcess = null;

        const duration = Date.now() - startTime;
        const nowIso = new Date().toISOString();

        if (!error) {
          // Execution Success (exit code 0)
          console.log(`Worker [${this.workerId}] job '${job.id}' completed in ${duration}ms.`);
          await dbRun(
            `UPDATE jobs 
             SET state = ?, locked_by = NULL, heartbeat_at = NULL, updated_at = ?, error = NULL
             WHERE id = ?`,
            [JOB_STATES.COMPLETED, nowIso, job.id]
          );
        } else {
          // Execution Failure (non-zero exit code or error)
          const newAttempts = job.attempts + 1;
          const maxRetries = job.max_retries;

          if (newAttempts >= maxRetries) {
            // Exceeded max retries -> move to Dead Letter Queue (dead)
            console.log(`Worker [${this.workerId}] job '${job.id}' failed attempt ${newAttempts}/${maxRetries}. Moved to DLQ.`);
            await dbRun(
              `UPDATE jobs 
               SET state = ?, attempts = ?, locked_by = NULL, heartbeat_at = NULL, updated_at = ?, error = ?
               WHERE id = ?`,
              [JOB_STATES.DEAD, newAttempts, nowIso, error.message || stderr, job.id]
            );
          } else {
            // Retryable failure -> move to failed state with updated attempt count
            console.log(`Worker [${this.workerId}] job '${job.id}' failed attempt ${newAttempts}/${maxRetries}. Scheduling retry.`);
            await dbRun(
              `UPDATE jobs 
               SET state = ?, attempts = ?, locked_by = NULL, heartbeat_at = NULL, updated_at = ?, error = ?
               WHERE id = ?`,
              [JOB_STATES.FAILED, newAttempts, nowIso, error.message || stderr, job.id]
            );
          }
        }

        resolve();
      });
    });
  }

  /**
   * Heartbeat updater while job is running
   */
  startHeartbeat(jobId) {
    this.heartbeatTimer = setInterval(async () => {
      const nowIso = new Date().toISOString();
      await dbRun(
        'UPDATE jobs SET heartbeat_at = ?, updated_at = ? WHERE id = ? AND locked_by = ?',
        [nowIso, nowIso, jobId, this.workerId]
      ).catch(() => {});
      await dbRun(
        'UPDATE workers SET heartbeat_at = ? WHERE id = ?',
        [nowIso, this.workerId]
      ).catch(() => {});
    }, 2000);
  }

  stopHeartbeat() {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  async registerWorker() {
    const nowIso = new Date().toISOString();
    await dbRun(
      `INSERT INTO workers (id, pid, state, started_at, heartbeat_at)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(id) DO UPDATE SET state = 'running', heartbeat_at = excluded.heartbeat_at`,
      [this.workerId, this.pid, 'running', nowIso, nowIso]
    );
  }

  async unregisterWorker() {
    this.stopHeartbeat();
    await dbRun("UPDATE workers SET state = 'stopped' WHERE id = ?", [this.workerId]).catch(() => {});
  }
}

/**
 * Worker Pool Manager running foreground workers
 */
export class WorkerPoolController {
  constructor(count = 1) {
    this.count = count;
    this.workers = [];
  }

  async start() {
    console.log(`Starting QueueCTL worker pool (${this.count} worker processes in foreground)...`);
    
    // Start control HTTP server for cross-terminal signaling
    startControlServer(this);

    for (let i = 0; i < this.count; i++) {
      const worker = new Worker(`worker-${process.pid}-${i + 1}`);
      this.workers.push(worker);
      worker.start();
    }
  }

  async stop() {
    console.log('Stopping worker pool gracefully...');
    for (const worker of this.workers) {
      worker.isStopping = true;
    }
    stopControlServer();
  }
}
