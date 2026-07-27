import http from 'node:http';
import { getJobSummary, listJobs } from './queue.js';
import { dbAll } from './db.js';

export const CONTROL_PORT = process.env.QUEUECTL_PORT || 9876;
let serverInstance = null;

/**
 * Start Express / HTTP Control Server for worker management & cross-terminal signals
 */
export function startControlServer(workerController = null) {
  if (serverInstance) return serverInstance;

  serverInstance = http.createServer(async (req, res) => {
    const url = new URL(req.url, `http://localhost:${CONTROL_PORT}`);

    res.setHeader('Content-Type', 'application/json');

    try {
      if (req.method === 'GET' && url.pathname === '/status') {
        const summary = await getJobSummary();
        const activeWorkers = await dbAll("SELECT * FROM workers WHERE state = 'running'");
        res.writeHead(200);
        res.end(JSON.stringify({ status: 'ok', summary, active_workers: activeWorkers }));
        return;
      }

      if (req.method === 'POST' && url.pathname === '/stop') {
        res.writeHead(200);
        res.end(JSON.stringify({ status: 'stopping', message: 'Stop signal received by control server.' }));

        // Trigger graceful stop for all workers managed by this controller
        if (workerController && typeof workerController.stop === 'function') {
          setTimeout(() => workerController.stop(), 100);
        }
        return;
      }

      if (req.method === 'GET' && url.pathname === '/jobs') {
        const state = url.searchParams.get('state');
        const jobs = await listJobs(state);
        res.writeHead(200);
        res.end(JSON.stringify(jobs));
        return;
      }

      res.writeHead(404);
      res.end(JSON.stringify({ error: 'Route not found' }));
    } catch (err) {
      res.writeHead(500);
      res.end(JSON.stringify({ error: err.message }));
    }
  });

  serverInstance.listen(CONTROL_PORT, () => {
    // Control server bound to local port for cross-terminal CLI interaction
  });

  serverInstance.on('error', (err) => {
    // If port in use by another running worker manager terminal, ignore duplicate bind
    if (err.code !== 'EADDRINUSE') {
      console.error('Control server error:', err.message);
    }
  });

  return serverInstance;
}

/**
 * Stop Control Server
 */
export function stopControlServer() {
  if (serverInstance) {
    serverInstance.close();
    serverInstance = null;
  }
}
