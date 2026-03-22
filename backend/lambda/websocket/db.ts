import { SecretsManagerClient, GetSecretValueCommand } from '@aws-sdk/client-secrets-manager';
import type { PoolClient } from 'pg';
import { Pool } from 'pg';

let poolPromise: Promise<Pool> | undefined;

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing env ${name}`);
  return value;
}

async function getPool(): Promise<Pool> {
  if (!poolPromise) {
    poolPromise = createPool();
  }
  return poolPromise;
}

async function createPool(): Promise<Pool> {
  const sm = new SecretsManagerClient({});
  const out = await sm.send(new GetSecretValueCommand({ SecretId: requireEnv('SECRET_ARN') }));
  const raw = out.SecretString;
  if (!raw) throw new Error('Empty DB secret');
  const { username, password } = JSON.parse(raw) as { username: string; password: string };
  return new Pool({
    host: requireEnv('PGHOST'),
    port: parseInt(requireEnv('PGPORT'), 10),
    database: requireEnv('PGDATABASE'),
    user: username,
    password,
    max: 4,
    ssl: { rejectUnauthorized: false },
    idleTimeoutMillis: 10_000,
    connectionTimeoutMillis: 15_000,
  });
}

export async function executeSql(
  text: string,
  values: unknown[] = [],
  client?: PoolClient,
): Promise<{ rows: Record<string, unknown>[] }> {
  const pool = await getPool();
  const res = client ? await client.query(text, values) : await pool.query(text, values);
  return { rows: res.rows as Record<string, unknown>[] };
}

export async function withTransaction<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
  const pool = await getPool();
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

/** First row, string column (Postgres may return UUID as string). */
export function scalarString(rows: Record<string, unknown>[], key: string): string | undefined {
  const cell = rows[0]?.[key];
  if (cell == null) return undefined;
  return String(cell);
}

/** First row, numeric column. */
export function scalarNumber(rows: Record<string, unknown>[], key: string): number | undefined {
  const cell = rows[0]?.[key];
  if (cell == null) return undefined;
  return typeof cell === 'number' ? cell : Number(cell);
}
