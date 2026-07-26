import { DatabaseSync } from 'node:sqlite';
import path from 'path';
import fs from 'fs';

// Database file path configuration
const DB_DIR = process.env.QUEUECTL_DIR || process.cwd();
if (!fs.existsSync(DB_DIR)) {
  fs.mkdirSync(DB_DIR, { recursive: true });
}
export const DB_PATH = process.env.QUEUECTL_DB || path.join(DB_DIR, 'queuectl.db');

let dbInstance = null;

/**
 * Get or initialize SQLite database connection
 */
export function getDb() {
  if (dbInstance) return dbInstance;

  dbInstance = new DatabaseSync(DB_PATH);
  
  // High-performance WAL mode with busy timeout for multi-process safety
  dbInstance.exec('PRAGMA journal_mode = WAL;');
  dbInstance.exec('PRAGMA busy_timeout = 5000;');
  dbInstance.exec('PRAGMA synchronous = NORMAL;');

  initSchema(dbInstance);
  return dbInstance;
}

/**
 * Initialize database tables
 */
function initSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      command TEXT NOT NULL,
      state TEXT NOT NULL DEFAULT 'pending',
      attempts INTEGER NOT NULL DEFAULT 0,
      max_retries INTEGER NOT NULL DEFAULT 3,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      last_run_at TEXT,
      locked_by TEXT,
      heartbeat_at TEXT,
      error TEXT
    );

    CREATE TABLE IF NOT EXISTS config (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS workers (
      id TEXT PRIMARY KEY,
      pid INTEGER NOT NULL,
      state TEXT NOT NULL,
      started_at TEXT NOT NULL,
      heartbeat_at TEXT NOT NULL
    );
  `);
}

/**
 * Execute an SQL write statement (INSERT, UPDATE, DELETE)
 */
export async function dbRun(sql, params = []) {
  const db = getDb();
  const stmt = db.prepare(sql);
  const result = stmt.run(...params);
  return { changes: result.changes, lastInsertRowid: result.lastInsertRowid };
}

/**
 * Fetch a single SQL row
 */
export async function dbGet(sql, params = []) {
  const db = getDb();
  const stmt = db.prepare(sql);
  return stmt.get(...params) || null;
}

/**
 * Fetch all matching SQL rows
 */
export async function dbAll(sql, params = []) {
  const db = getDb();
  const stmt = db.prepare(sql);
  return stmt.all(...params);
}

/**
 * Execute an immediate transaction for cross-process atomic claiming
 */
export async function dbTransaction(actionFn) {
  const db = getDb();
  db.exec('BEGIN IMMEDIATE;');
  try {
    const result = await actionFn(db);
    db.exec('COMMIT;');
    return result;
  } catch (err) {
    try {
      db.exec('ROLLBACK;');
    } catch (_) {}
    throw err;
  }
}

/**
 * Close database connection
 */
export function closeDb() {
  if (dbInstance) {
    dbInstance.close();
    dbInstance = null;
  }
}
