import { SecretsManagerClient, GetSecretValueCommand } from '@aws-sdk/client-secrets-manager';
import type { PoolClient } from 'pg';
import { Pool } from 'pg';

let poolPromise: Promise<Pool> | undefined;

function env(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env ${name}`);
  return v;
}

async function getPool(): Promise<Pool> {
  if (!poolPromise) {
    poolPromise = createPool();
  }
  return poolPromise;
}

async function createPool(): Promise<Pool> {
  const sm = new SecretsManagerClient({});
  const out = await sm.send(new GetSecretValueCommand({ SecretId: env('SECRET_ARN') }));
  const raw = out.SecretString;
  if (!raw) throw new Error('Empty DB secret');
  const { username, password } = JSON.parse(raw) as { username: string; password: string };
  return new Pool({
    host: env('PGHOST'),
    port: parseInt(env('PGPORT'), 10),
    database: env('PGDATABASE'),
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

export function col(rows: Record<string, unknown>[], key: string): string | undefined {
  const v = rows[0]?.[key];
  if (v == null) return undefined;
  return String(v);
}

export function colNum(rows: Record<string, unknown>[], key: string): number | undefined {
  const v = rows[0]?.[key];
  if (v == null) return undefined;
  return typeof v === 'number' ? v : Number(v);
}
