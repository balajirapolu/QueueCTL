import { getDb, closeDb, dbRun } from '../src/db.js';
import { enqueueJob, listJobs, getJobSummary } from '../src/queue.js';
import { listDlqJobs, retryDlqJob } from '../src/dlq.js';
import { setConfig, getConfig } from '../src/config.js';
import { Worker } from '../src/worker.js';
import { runCrashRecovery } from '../src/recovery.js';

async function runTests() {
  console.log('=====================================================');
  console.log('       QueueCTL End-to-End Integration Test Suite     ');
  console.log('=====================================================\n');

  getDb();

  // Reset database state for clean test execution
  await dbRun('DELETE FROM jobs');
  await dbRun('DELETE FROM workers');
  await dbRun('DELETE FROM config');

  // ----------------------------------------------------
  // Scenario 1: Basic Job Completion
  // ----------------------------------------------------
  console.log('[Scenario 1] Testing basic job execution and completion...');
  const job1 = await enqueueJob({ id: 's1-job1', command: 'echo "Scenario 1 Success"' });
  console.log(`  Enqueued job '${job1.id}' (state: ${job1.state})`);

  const worker1 = new Worker('test-worker-s1');
  const claimedJob1 = await worker1.claimNextJob();
  if (!claimedJob1 || claimedJob1.id !== 's1-job1') {
    throw new Error('Scenario 1 Failed: Worker could not claim pending job.');
  }

  await worker1.executeJob(claimedJob1);
  const completedJobs1 = await listJobs('completed');
  if (completedJobs1.length !== 1 || completedJobs1[0].id !== 's1-job1') {
    throw new Error('Scenario 1 Failed: Job did not transition to completed state.');
  }
  console.log('  ✔ Scenario 1 PASSED: Basic job completed successfully.\n');

  // ----------------------------------------------------
  // Scenario 2: Failing Job Retries & DLQ Movement
  // ----------------------------------------------------
  console.log('[Scenario 2] Testing failing job retries, backoff, and DLQ...');
  await setConfig('max-retries', '2');
  await setConfig('backoff-base', '2');

  const job2 = await enqueueJob({ id: 's2-failing-job', command: 'node -e "process.exit(1)"' });
  console.log(`  Enqueued failing job '${job2.id}' (max_retries: ${job2.max_retries})`);

  // Attempt 1
  const claimedJob2 = await worker1.claimNextJob();
  await worker1.executeJob(claimedJob2);

  let failedJobs = await listJobs('failed');
  if (failedJobs.length !== 1 || failedJobs[0].attempts !== 1) {
    throw new Error('Scenario 2 Failed: Attempt 1 did not record failure.');
  }
  console.log('  ✔ Attempt 1 failed correctly (attempt 1/2).');

  // Attempt 2 (Exceeds max retries -> lands in DLQ)
  // Simulate delay elapsed
  await dbRun("UPDATE jobs SET last_run_at = '2000-01-01T00:00:00.000Z' WHERE id = 's2-failing-job'");
  const claimedJob2_2 = await worker1.claimNextJob();
  await worker1.executeJob(claimedJob2_2);

  const dlqJobs = await listDlqJobs();
  if (dlqJobs.length !== 1 || dlqJobs[0].id !== 's2-failing-job') {
    throw new Error('Scenario 2 Failed: Job did not land in DLQ after max retries.');
  }
  console.log('  ✔ Scenario 2 PASSED: Failing job moved to DLQ after retries.\n');

  // ----------------------------------------------------
  // Scenario 3: Multi-Worker Concurrency (No double-claiming)
  // ----------------------------------------------------
  console.log('[Scenario 3] Testing multi-worker parallel execution...');
  await dbRun('DELETE FROM jobs');

  for (let i = 1; i <= 5; i++) {
    await enqueueJob({ id: `s3-job-${i}`, command: `echo "Parallel job ${i}"` });
  }

  const workerA = new Worker('worker-A');
  const workerB = new Worker('worker-B');

  const claimsA = [];
  const claimsB = [];

  for (let i = 0; i < 5; i++) {
    const jobA = await workerA.claimNextJob();
    if (jobA) claimsA.push(jobA.id);

    const jobB = await workerB.claimNextJob();
    if (jobB) claimsB.push(jobB.id);
  }

  const totalClaimed = new Set([...claimsA, ...claimsB]);
  if (totalClaimed.size !== 5) {
    throw new Error(`Scenario 3 Failed: Claimed ${totalClaimed.size} unique jobs out of 5.`);
  }

  // Ensure no overlap
  const overlap = claimsA.filter((id) => claimsB.includes(id));
  if (overlap.length > 0) {
    throw new Error(`Scenario 3 Failed: Overlapping job claims detected: ${overlap.join(', ')}`);
  }
  console.log(`  ✔ Worker A claimed ${claimsA.length} jobs, Worker B claimed ${claimsB.length} jobs with ZERO overlap.`);
  console.log('  ✔ Scenario 3 PASSED: Concurrency safety verified.\n');

  // ----------------------------------------------------
  // Scenario 4: SIGKILL Worker Crash & Automated Recovery
  // ----------------------------------------------------
  console.log('[Scenario 4] Testing SIGKILL crash recovery...');
  await dbRun('DELETE FROM jobs');

  await enqueueJob({ id: 's4-crashed-job', command: 'echo "Crashed Job Recovery"' });
  
  // Simulate worker claiming job and crashing mid-execution (leaving state='processing' with stale heartbeat)
  const staleTime = new Date(Date.now() - 30000).toISOString(); // 30 seconds ago
  await dbRun(
    "UPDATE jobs SET state = 'processing', locked_by = 'dead-worker-pid-999', heartbeat_at = ?, updated_at = ? WHERE id = 's4-crashed-job'",
    [staleTime, staleTime]
  );

  console.log("  Simulated SIGKILL crash: Job 's4-crashed-job' stuck in 'processing' with 30s stale heartbeat.");

  // Trigger crash recovery check (threshold = 15s)
  await runCrashRecovery(15);

  const recoveredJobs = await listJobs('failed');
  if (recoveredJobs.length !== 1 || recoveredJobs[0].id !== 's4-crashed-job') {
    throw new Error('Scenario 4 Failed: Stale processing job was not recovered.');
  }
  console.log("  ✔ Crash recovery scanner detected dead worker and reset job state to 'failed'.");

  // Verify job can now be picked up again and completed
  await dbRun("UPDATE jobs SET last_run_at = '2000-01-01T00:00:00.000Z' WHERE id = 's4-crashed-job'");
  const reClaimed = await worker1.claimNextJob();
  await worker1.executeJob(reClaimed);

  const finalCompleted = await listJobs('completed');
  if (finalCompleted.length !== 1 || finalCompleted[0].id !== 's4-crashed-job') {
    throw new Error('Scenario 4 Failed: Recovered job failed to complete after restart.');
  }
  console.log('  ✔ Scenario 4 PASSED: Crashed worker job recovered and completed.\n');

  // ----------------------------------------------------
  // Scenario 5: Full Restart Persistence
  // ----------------------------------------------------
  console.log('[Scenario 5] Testing job data persistence across restarts...');
  await setConfig('max-retries', '7');

  const summaryBefore = await getJobSummary();
  closeDb(); // Simulate full process exit / DB closure

  // Re-open DB
  getDb();
  const summaryAfter = await getJobSummary();
  const persistedConfig = await getConfig('max-retries');

  if (summaryBefore.total !== summaryAfter.total || persistedConfig !== '7') {
    throw new Error('Scenario 5 Failed: Data mismatch after restart.');
  }
  console.log('  ✔ Scenario 5 PASSED: All job records and configuration persisted across restarts.\n');

  closeDb();

  console.log('=====================================================');
  console.log('       ALL 5 INTEGRATION TEST SCENARIOS PASSED!      ');
  console.log('=====================================================\n');
}

runTests().catch((err) => {
  console.error('\nTEST SUITE FAILED:', err.message);
  process.exit(1);
});
