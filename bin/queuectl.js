#!/usr/bin/env node
import { runCli } from '../src/cli.js';

runCli().catch((err) => {
  console.error('QueueCTL CLI Error:', err.message);
  process.exit(1);
});
