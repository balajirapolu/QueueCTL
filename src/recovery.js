import { dbAll, dbRun } from './db.js';
import { JOB_STATES } from './job.js';

/**
 * Crash Recovery Scanner: Detects jobs stuck in 'processing' whose worker died (e.g. SIGKILL).
 * Worst-case recovery timeout threshold: 15 seconds (well under 60 seconds limit).
 */
export async function runCrashRecovery(staleThresholdSec = 15) {
  try {
    const now = new Date();

    const processingJobs = await dbAll(
      "SELECT * FROM jobs WHERE state = 'processing'"
    );

    for (const job of processingJobs) {
      // Check last heartbeat timestamp
      const lastBeat = job.heartbeat_at || job.updated_at;
      if (!lastBeat) continue;

      const elapsedSec = (now.getTime() - new Date(lastBeat).getTime()) / 1000;

      if (elapsedSec > staleThresholdSec) {
        const nowIso = now.toISOString();
        const newAttempts = job.attempts + 1;
        const maxRetries = job.max_retries;

        console.log(`[Crash Recovery] Found stale processing job '${job.id}' (no heartbeat for ${elapsedSec.toFixed(1)}s). Worker crashed!`);

        if (newAttempts >= maxRetries) {
          // Move to DLQ if max retries exceeded
          await dbRun(
            `UPDATE jobs 
             SET state = ?, attempts = ?, locked_by = NULL, heartbeat_at = NULL, updated_at = ?, error = ?
             WHERE id = ? AND state = 'processing'`,
            [JOB_STATES.DEAD, newAttempts, nowIso, 'Worker process crashed or terminated (SIGKILL)', job.id]
          );
        } else {
          // Reset to failed with attempt increment to apply exponential backoff delay
          await dbRun(
            `UPDATE jobs 
             SET state = ?, attempts = ?, locked_by = NULL, heartbeat_at = NULL, updated_at = ?, last_run_at = ?, error = ?
             WHERE id = ? AND state = 'processing'`,
            [JOB_STATES.FAILED, newAttempts, nowIso, nowIso, 'Worker process crashed or terminated (SIGKILL)', job.id]
          );
        }
      }
    }
  } catch (err) {
    // Ignore crash recovery check errors during shutdown
  }
}
