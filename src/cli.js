import { enqueueJob, listJobs, getJobSummary } from './queue.js';
import { listDlqJobs, retryDlqJob } from './dlq.js';
import { setConfig, getAllConfig } from './config.js';
import { WorkerPoolController } from './worker.js';
import { dbAll } from './db.js';
import http from 'node:http';
import { CONTROL_PORT } from './server.js';

/**
 * Main CLI router handling all QueueCTL commands
 */
export async function runCli(args = process.argv.slice(2)) {
  if (args.length === 0) {
    printHelp();
    process.exit(0);
  }

  const primaryCmd = args[0];

  try {
    switch (primaryCmd) {
      case 'enqueue':
        await handleEnqueue(args.slice(1));
        break;

      case 'worker':
        await handleWorker(args.slice(1));
        break;

      case 'status':
        await handleStatus();
        break;

      case 'list':
        await handleList(args.slice(1));
        break;

      case 'dlq':
        await handleDlq(args.slice(1));
        break;

      case 'config':
        await handleConfig(args.slice(1));
        break;

      case '--help':
      case '-h':
      case 'help':
        printHelp();
        break;

      default:
        console.error(`Error: Unknown command '${primaryCmd}'. Use --help for usage.`);
        process.exit(1);
    }
  } catch (err) {
    console.error(`Error: ${err.message}`);
    process.exit(1);
  }
}

/**
 * Command: queuectl enqueue '<json>'
 */
async function handleEnqueue(args) {
  if (args.length === 0) {
    throw new Error('Usage: queuectl enqueue \'<json_object>\'');
  }

  const jsonString = args.join(' ');
  const job = await enqueueJob(jsonString);
  console.log(`Enqueued job '${job.id}' (state: ${job.state}, max_retries: ${job.max_retries})`);
}

/**
 * Command: queuectl worker start --count N | queuectl worker stop
 */
async function handleWorker(args) {
  const subCmd = args[0];

  if (subCmd === 'start') {
    let count = 1;
    for (let i = 1; i < args.length; i++) {
      if ((args[i] === '--count' || args[i] === '-c') && args[i + 1]) {
        count = parseInt(args[i + 1], 10);
        if (isNaN(count) || count < 1) {
          throw new Error('--count must be a positive integer');
        }
        break;
      }
    }

    const pool = new WorkerPoolController(count);
    await pool.start();
  } else if (subCmd === 'stop') {
    // Send HTTP POST signal to live control server on port CONTROL_PORT
    await sendControlStopSignal();
  } else {
    throw new Error('Usage: queuectl worker start [--count N] | queuectl worker stop');
  }
}

/**
 * Send stop request to control server across terminals
 */
function sendControlStopSignal() {
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname: 'localhost',
        port: CONTROL_PORT,
        path: '/stop',
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      },
      (res) => {
        let data = '';
        res.on('data', (chunk) => (data += chunk));
        res.on('end', () => {
          console.log('Successfully sent graceful stop signal to active workers.');
          resolve();
        });
      }
    );

    req.on('error', () => {
      console.log('No active worker control server found running on port ' + CONTROL_PORT);
      resolve();
    });

    req.end();
  });
}

/**
 * Command: queuectl status
 */
async function handleStatus() {
  const summary = await getJobSummary();
  const activeWorkers = await dbAll("SELECT * FROM workers WHERE state = 'running'");

  console.log('=== QueueCTL System Status ===');
  console.log(`Active Worker Processes: ${activeWorkers.length}`);
  if (activeWorkers.length > 0) {
    activeWorkers.forEach((w) => console.log(`  - Worker [${w.id}] PID ${w.pid} (started: ${w.started_at})`));
  }
  console.log('\nJob Breakdown:');
  console.log(`  - Pending:    ${summary.pending}`);
  console.log(`  - Processing: ${summary.processing}`);
  console.log(`  - Completed:  ${summary.completed}`);
  console.log(`  - Failed:     ${summary.failed}`);
  console.log(`  - Dead (DLQ): ${summary.dead}`);
  console.log(`  - Total Jobs: ${summary.total}`);
}

/**
 * Command: queuectl list --state <state> [--json]
 * Requirement: If --json flag is set, prints ONLY the JSON array to stdout!
 */
async function handleList(args) {
  let stateFilter = null;
  let isJson = false;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--state' && args[i + 1]) {
      stateFilter = args[i + 1];
      i++;
    } else if (args[i] === '--json') {
      isJson = true;
    }
  }

  const jobs = await listJobs(stateFilter);

  if (isJson) {
    // Pure JSON array output to stdout (strictly as required by interface contract)
    process.stdout.write(JSON.stringify(jobs) + '\n');
  } else {
    if (jobs.length === 0) {
      console.log(stateFilter ? `No jobs found in state '${stateFilter}'.` : 'No jobs found.');
      return;
    }

    console.log(`Jobs (${stateFilter ? stateFilter : 'all'}):`);
    jobs.forEach((job) => {
      console.log(`  [${job.id}] state: ${job.state} | attempts: ${job.attempts}/${job.max_retries} | cmd: "${job.command}"`);
    });
  }
}

/**
 * Command: queuectl dlq list / queuectl dlq retry <job_id>
 */
async function handleDlq(args) {
  const subCmd = args[0];

  if (subCmd === 'list') {
    const deadJobs = await listDlqJobs();
    if (deadJobs.length === 0) {
      console.log('Dead Letter Queue is empty.');
      return;
    }
    console.log('=== Dead Letter Queue (DLQ) ===');
    deadJobs.forEach((job) => {
      console.log(`  [${job.id}] attempts: ${job.attempts}/${job.max_retries} | cmd: "${job.command}" | updated: ${job.updated_at}`);
    });
  } else if (subCmd === 'retry') {
    const jobId = args[1];
    if (!jobId) {
      throw new Error('Usage: queuectl dlq retry <job_id>');
    }
    const retried = await retryDlqJob(jobId);
    console.log(`Successfully re-enqueued job '${retried.id}' from DLQ to pending state (attempts reset to 0).`);
  } else {
    throw new Error('Usage: queuectl dlq list | queuectl dlq retry <job_id>');
  }
}

/**
 * Command: queuectl config set <key> <value>
 */
async function handleConfig(args) {
  const subCmd = args[0];

  if (subCmd === 'set') {
    const key = args[1];
    const value = args[2];
    if (!key || value === undefined) {
      throw new Error('Usage: queuectl config set <key> <value>');
    }
    const updated = await setConfig(key, value);
    console.log(`Configuration updated: ${updated.key} = ${updated.value}`);
  } else if (subCmd === 'get' || subCmd === 'list') {
    const all = await getAllConfig();
    console.log('=== QueueCTL Configuration ===');
    Object.entries(all).forEach(([k, v]) => console.log(`  ${k} = ${v}`));
  } else {
    throw new Error('Usage: queuectl config set <key> <value>');
  }
}

/**
 * Print CLI Help message
 */
function printHelp() {
  console.log(`
QueueCTL — Background Job Queue System CLI

Usage:
  queuectl enqueue '<json_object>'              Enqueue a background job
  queuectl worker start [--count N]             Start worker processes in foreground
  queuectl worker stop                          Stop all workers from another terminal
  queuectl status                               View system status & worker/job breakdown
  queuectl list [--state <state>] [--json]       List jobs by state (json output available)
  queuectl dlq list                             List dead jobs in DLQ
  queuectl dlq retry <job_id>                   Re-enqueue a dead job from DLQ
  queuectl config set <key> <value>             Set configuration (max-retries, backoff-base)
`);
}
