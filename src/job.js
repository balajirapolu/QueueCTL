/**
 * Supported Job States
 */
export const JOB_STATES = {
  PENDING: 'pending',
  PROCESSING: 'processing',
  COMPLETED: 'completed',
  FAILED: 'failed',
  DEAD: 'dead'
};

/**
 * Format a database row into standard QueueCTL job specification format
 */
export function formatJob(row) {
  if (!row) return null;

  return {
    id: row.id,
    command: row.command,
    state: row.state,
    attempts: Number(row.attempts),
    max_retries: Number(row.max_retries),
    created_at: row.created_at,
    updated_at: row.updated_at
  };
}

/**
 * Validate job payload for enqueueing
 */
export function validateJobInput(payload) {
  let jobData = payload;

  if (typeof payload === 'string') {
    try {
      jobData = JSON.parse(payload);
    } catch (err) {
      throw new Error(`Invalid JSON job payload: ${err.message}`);
    }
  }

  if (!jobData || typeof jobData !== 'object') {
    throw new Error('Job payload must be a valid JSON object');
  }

  if (!jobData.id || typeof jobData.id !== 'string' || !jobData.id.trim()) {
    throw new Error('Job must contain a non-empty string "id"');
  }

  if (!jobData.command || typeof jobData.command !== 'string' || !jobData.command.trim()) {
    throw new Error('Job must contain a non-empty string "command"');
  }

  return {
    id: jobData.id.trim(),
    command: jobData.command.trim(),
    max_retries: jobData.max_retries !== undefined ? Number(jobData.max_retries) : null
  };
}
