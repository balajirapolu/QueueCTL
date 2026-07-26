import { dbGet, dbRun } from './db.js';

export const DEFAULT_CONFIG = {
  'max-retries': '3',
  'backoff-base': '2'
};

/**
 * Get configuration value by key, falling back to default if not set
 */
export async function getConfig(key) {
  const row = await dbGet('SELECT value FROM config WHERE key = ?', [key]);
  if (row && row.value !== undefined && row.value !== null) {
    return row.value;
  }
  return DEFAULT_CONFIG[key] !== undefined ? DEFAULT_CONFIG[key] : null;
}

/**
 * Get numerical configuration value
 */
export async function getNumericConfig(key) {
  const val = await getConfig(key);
  const num = parseInt(val, 10);
  return isNaN(num) ? parseInt(DEFAULT_CONFIG[key], 10) : num;
}

/**
 * Set configuration key-value pair in database
 */
export async function setConfig(key, value) {
  const validKeys = ['max-retries', 'backoff-base'];
  if (!validKeys.includes(key)) {
    throw new Error(`Invalid configuration key: '${key}'. Allowed keys: ${validKeys.join(', ')}`);
  }

  const numValue = parseInt(value, 10);
  if (isNaN(numValue) || numValue < 0) {
    throw new Error(`Configuration value for '${key}' must be a non-negative integer.`);
  }

  await dbRun(
    `INSERT INTO config (key, value) VALUES (?, ?)
     ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
    [key, String(numValue)]
  );

  return { key, value: String(numValue) };
}

/**
 * Get all current configuration settings
 */
export async function getAllConfig() {
  const configMap = { ...DEFAULT_CONFIG };
  try {
    const rows = await dbGet('SELECT key, value FROM config');
    if (Array.isArray(rows)) {
      for (const row of rows) {
        configMap[row.key] = row.value;
      }
    }
  } catch (err) {
    // If table read fails, return defaults
  }
  return configMap;
}
