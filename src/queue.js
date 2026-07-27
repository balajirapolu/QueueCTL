import { dbRun, dbGet, dbAll } from './db.js';
import { JOB_STATES, formatJob, validateJobInput } from './job.js';
import { getNumericConfig } from './config.js';

/**
 * Calculate exponential backoff delay in seconds: delay = base ^ attempts
 */
export function calculateBackoffDelay(attempts, backoffBase = 2) {
  const exp = Math.max(0, Number(attempts));
  return Math.pow(Number(backoffBase), exp);
}

/**
 * Enqueue a new background job into the database
 */
export async function enqueueJob(payload) {
  const validated = validateJobInput(payload);
  
  // Use custom max_retries if passed in job object, else default from config
  const defaultMaxRetries = await getNumericConfig('max-retries');
  const maxRetries = validated.max_retries !== null ? validated.max_retries : defaultMaxRetries;

  const now = new Date().toISOString();

  // Check if job ID already exists
  const existing = await dbGet('SELECT id FROM jobs WHERE id = ?', [validated.id]);
  if (existing) {
    throw new Error(`Job with ID '${validated.id}' already exists.`);
  }

  await dbRun(
    `INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [validated.id, validated.command, JOB_STATES.PENDING, 0, maxRetries, now, now]
  );

  const row = await dbGet('SELECT * FROM jobs WHERE id = ?', [validated.id]);
  return formatJob(row);
}

/**
 * List jobs by state (or all jobs if state is omitted)
 */
export async function listJobs(state = null) {
  let rows;
  if (state) {
    const validStates = Object.values(JOB_STATES);
    if (!validStates.includes(state)) {
      throw new Error(`Invalid state filter '${state}'. Allowed: ${validStates.join(', ')}`);
    }
    rows = await dbAll('SELECT * FROM jobs WHERE state = ? ORDER BY created_at ASC', [state]);
  } else {
    rows = await dbAll('SELECT * FROM jobs ORDER BY created_at ASC');
  }

  return rows.map(formatJob);
}

/**
 * Get summary breakdown of job counts by state
 */
export async function getJobSummary() {
  const rows = await dbAll('SELECT state, COUNT(*) as count FROM jobs GROUP BY state');
  
  const summary = {
    pending: 0,
    processing: 0,
    completed: 0,
    failed: 0,
    dead: 0,
    total: 0
  };

  for (const row of rows) {
    if (summary[row.state] !== undefined) {
      summary[row.state] = Number(row.count);
    }
    summary.total += Number(row.count);
  }

  return summary;
}
