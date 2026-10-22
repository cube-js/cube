/**
 * Local (docker-less) E2E environment for the #9638 / #3486 repro tests:
 * real Postgres (localhost:5432, test/test), a real Cube Store binary and a
 * real Cube server started from packages/cubejs-server.
 */
import { spawn, ChildProcess } from 'child_process';
import fs from 'fs';
import os from 'os';
import path from 'path';
import fetch from 'node-fetch';
import { Client } from 'pg';

export const PORTS = {
  api: Number(process.env.ISSUE_REPRO_API_PORT || 14020),
  sql: Number(process.env.ISSUE_REPRO_SQL_PORT || 15420),
  csMysql: Number(process.env.ISSUE_REPRO_CS_MYSQL_PORT || 13311),
  csHttp: Number(process.env.ISSUE_REPRO_CS_HTTP_PORT || 13321),
  csMeta: Number(process.env.ISSUE_REPRO_CS_META_PORT || 13331),
  csWorker: Number(process.env.ISSUE_REPRO_CS_WORKER_PORT || 13341),
  csStatus: Number(process.env.ISSUE_REPRO_CS_STATUS_PORT || 13351),
};

const REPO_ROOT = path.resolve(__dirname, '../../../..');
const CUBESTORE_BIN = process.env.CUBESTORE_BIN ||
  path.join(REPO_ROOT, 'rust/cubestore/downloaded/latest/bin/cubestored');
const CUBE_SERVER_BIN = path.join(REPO_ROOT, 'packages/cubejs-server/bin/server');

export const FIXTURES_DIR = path.join(REPO_ROOT, 'packages/cubejs-testing/birdbox-fixtures/issue-repros-9638-3486');

export function pgClient(): Client {
  return new Client({ host: 'localhost', port: 5432, user: 'test', password: 'test', database: 'test' });
}

export async function pgExec(sql: string) {
  const client = pgClient();
  await client.connect();

  try {
    return await client.query(sql);
  } finally {
    await client.end();
  }
}

function waitForOutput(proc: ChildProcess, needle: string, timeoutMs: number, logs: string[]): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Timeout waiting for "${needle}". Logs:\n${logs.join('').slice(-4000)}`)), timeoutMs);
    const onData = (d: Buffer) => {
      if (logs.join('').includes(needle)) {
        clearTimeout(timer);
        resolve();
      }
      return d;
    };
    proc.stdout?.on('data', onData);
    proc.stderr?.on('data', onData);
    proc.on('exit', (code) => {
      clearTimeout(timer);
      reject(new Error(`Process exited with ${code} before "${needle}". Logs:\n${logs.join('').slice(-4000)}`));
    });
  });
}

export interface ReproEnv {
  logs: string[];
  load: (query: any) => Promise<any>;
  loadUntilReady: (query: any, timeoutMs?: number) => Promise<any>;
  meta: () => Promise<any>;
  stop: () => Promise<void>;
}

export async function startReproEnv(fixture: string, extraEnv: Record<string, string> = {}): Promise<ReproEnv> {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), `issue-repro-${fixture}-`));
  const csLogs: string[] = [];
  const cubestore = spawn(CUBESTORE_BIN, [], {
    env: {
      ...process.env,
      CUBESTORE_PORT: String(PORTS.csMysql),
      CUBESTORE_HTTP_PORT: String(PORTS.csHttp),
      CUBESTORE_META_PORT: String(PORTS.csMeta),
      CUBESTORE_WORKER_PORTS: String(PORTS.csWorker),
      CUBESTORE_STATUS_PORT: String(PORTS.csStatus),
      CUBESTORE_SERVER_NAME: `127.0.0.1:${PORTS.csMeta}`,
      CUBESTORE_DATA_DIR: path.join(tmp, 'data'),
      CUBESTORE_REMOTE_DIR: path.join(tmp, 'remote'),
    },
  });
  cubestore.stdout.on('data', (d) => csLogs.push(d.toString()));
  cubestore.stderr.on('data', (d) => csLogs.push(d.toString()));
  await waitForOutput(cubestore, 'Http Server is listening', 30000, csLogs);

  const logs: string[] = [];
  const server = spawn('node', [CUBE_SERVER_BIN], {
    cwd: path.join(FIXTURES_DIR, fixture),
    env: {
      ...process.env,
      CUBEJS_DB_TYPE: 'postgres',
      CUBEJS_DB_HOST: 'localhost',
      CUBEJS_DB_PORT: '5432',
      CUBEJS_DB_USER: 'test',
      CUBEJS_DB_PASS: 'test',
      CUBEJS_DB_NAME: 'test',
      CUBEJS_DEV_MODE: 'true',
      CUBEJS_API_SECRET: 'secret',
      CUBEJS_TELEMETRY: 'false',
      CUBEJS_CUBESTORE_HOST: '127.0.0.1',
      CUBEJS_CUBESTORE_PORT: String(PORTS.csHttp),
      PORT: String(PORTS.api),
      CUBEJS_PG_SQL_PORT: String(PORTS.sql),
      CUBEJS_LOG_LEVEL: 'trace',
      ...extraEnv,
    },
  });
  server.stdout.on('data', (d) => logs.push(d.toString()));
  server.stderr.on('data', (d) => logs.push(d.toString()));
  await waitForOutput(server, 'Cube API server', 60000, logs);

  const base = `http://127.0.0.1:${PORTS.api}/cubejs-api/v1`;
  const load = async (query: any) => {
    const res = await fetch(`${base}/load?query=${encodeURIComponent(JSON.stringify(query))}`);
    return res.json();
  };
  const loadUntilReady = async (query: any, timeoutMs = 90000) => {
    const start = Date.now();

    for (;;) {
      const r = await load(query);
      if (r.error !== 'Continue wait') {
        return r;
      }
      if (Date.now() - start > timeoutMs) {
        throw new Error('Timed out waiting for query');
      }
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  };
  const meta = async () => (await fetch(`${base}/meta`)).json();

  const kill = (p: ChildProcess) => new Promise<void>((resolve) => {
    if (p.exitCode !== null) {
      resolve();
      return;
    }
    p.once('exit', () => resolve());
    p.kill('SIGKILL');
  });

  return {
    logs,
    load,
    loadUntilReady,
    meta,
    stop: async () => {
      await kill(server);
      await kill(cubestore);
      fs.rmSync(tmp, { recursive: true, force: true });
    },
  };
}
