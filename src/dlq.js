import { dbGet, dbRun, dbAll } from './db.js';
import { JOB_STATES, formatJob } from './job.js';

/**
 * List all jobs in the Dead Letter Queue (state = 'dead')
 */
export async function listDlqJobs() {
  const rows = await dbAll(
    'SELECT * FROM jobs WHERE state = ? ORDER BY updated_at DESC',
    [JOB_STATES.DEAD]
  );
  return rows.map(formatJob);
}

/**
 * Retry a job from the Dead Letter Queue by re-enqueueing it as 'pending'.
 * Resets 'attempts' to 0 because manual intervention acknowledges operator fix.
 */
export async function retryDlqJob(jobId) {
  const job = await dbGet('SELECT * FROM jobs WHERE id = ?', [jobId]);

  if (!job) {
    throw new Error(`Job with ID '${jobId}' not found.`);
  }

  if (job.state !== JOB_STATES.DEAD) {
    throw new Error(`Job '${jobId}' is not in DLQ (current state: '${job.state}').`);
  }

  const now = new Date().toISOString();

  // Reset job to pending and reset attempts count for a fresh lifecycle
  await dbRun(
    `UPDATE jobs
     SET state = ?, attempts = 0, error = NULL, updated_at = ?, heartbeat_at = NULL, locked_by = NULL
     WHERE id = ?`,
    [JOB_STATES.PENDING, now, jobId]
  );

  const updatedRow = await dbGet('SELECT * FROM jobs WHERE id = ?', [jobId]);
  return formatJob(updatedRow);
}
