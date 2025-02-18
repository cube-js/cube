import { StartedTestContainer } from 'testcontainers';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { Client as PgClient } from 'pg';
import { PostgresDBRunner } from '@cubejs-backend/testing-shared';
import { getBirdbox } from '../src';
import {
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
} from './smoke-tests';

describe('graceful shutdown', () => {
  jest.setTimeout(60 * 5 * 1000);
  let db: StartedTestContainer;

  // For when graceful shutdown is not supposed to timeout, vs. for when it is supposed
  // to timeout.
  const longGracefulTimeoutSecs = 30;
  const shortGracefulTimeoutSecs = 1;

  const pgPort = 5656; // Make random?  (Value and comment taken from smoke-cubesql.)
  let connectionId = 0;

  // Since we use 'error' and 'end' events for some tests, it is necessary or wise to let the event
  // loop spin around once before asserting.
  const yieldImmediate = () => new Promise(setImmediate);

  function unconnectedPostgresClient(user: string, password: string) {
    connectionId++;
    const currentConnId = connectionId;

    console.debug(`[pg] new connection ${currentConnId}`);

    const conn = new PgClient({
      database: 'db',
      port: pgPort,
      host: 'localhost',
      user,
      password,
      ssl: false,
    });
    conn.on('end', () => {
      console.debug(`[pg] end ${currentConnId}`);
    });

    return conn;
  }

  const makeBirdbox = (gracefulTimeoutSecs: number) => getBirdbox(
    'postgres',
    {
      ...DEFAULT_CONFIG,
      //
      CUBESQL_LOG_LEVEL: 'trace',
      //
      CUBEJS_DB_TYPE: 'postgres',
      CUBEJS_DB_HOST: db.getHost(),
      CUBEJS_DB_PORT: `${db.getMappedPort(5432)}`,
      CUBEJS_DB_NAME: 'test',
      CUBEJS_DB_USER: 'test',
      CUBEJS_DB_PASS: 'test',
      //
      CUBEJS_PG_SQL_PORT: `${pgPort}`,
      CUBESQL_SQL_PUSH_DOWN: 'true',
      CUBESQL_STREAM_MODE: 'true',

      CUBEJS_GRACEFUL_SHUTDOWN: gracefulTimeoutSecs.toString(),
    },
    {
      schemaDir: 'smoke/schema',
      cubejsConfig: 'smoke/cube.js',
    },
  );

  beforeAll(async () => {
    db = await PostgresDBRunner.startContainer({});
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await db.stop();
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  const clientless = async (signal: 'SIGTERM' | 'SIGINT') => {
    const birdbox = await makeBirdbox(longGracefulTimeoutSecs);
    try {
      birdbox.killCube(signal);
      const code = await birdbox.onCubeExit();
      expect(code).toEqual(0);
    } finally {
      await birdbox.stop();
    }
  };

  test('Clientless Graceful Shutdown SIGTERM', async () => {
    await clientless('SIGTERM');
  });

  test('Clientless Graceful Shutdown SIGINT', async () => {
    await clientless('SIGINT');
  });

  const betweenQueries = async (signal: 'SIGTERM' | 'SIGINT') => {
    const birdbox = await makeBirdbox(longGracefulTimeoutSecs);
    try {
      const connection: PgClient = unconnectedPostgresClient('admin', 'admin_password');

      let endResolve: () => void;
      const endPromise = new Promise<void>((res, _rej) => {
        endResolve = res;
      });
      await connection.connect();

      connection.on('end', () => { endResolve(); });
      let logTerminationErrors = true;
      let shutdownErrors = 0;
      connection.on('error', (e: Error) => {
        const err = e as any;
        if (err.severity === 'FATAL' && err.code === '57P01') {
          shutdownErrors += 1;
        } else if (logTerminationErrors && err.message !== 'Connection terminated unexpectedly') {
          console.log(err);
        }
      });
      try {
        const res = await connection.query(
          'SELECT COUNT(*) as cn, "status" FROM Orders GROUP BY 2 ORDER BY cn DESC'
        );
        expect(res.rows).toMatchSnapshot('sql_orders');

        logTerminationErrors = false;
        birdbox.killCube(signal);
        const code = await birdbox.onCubeExit();
        expect(code).toEqual(0);
      } finally {
        // Normally the connection ends by server shutdown, and this .end() call returns
        // a Promise which never gets fulfilled.
        const _ = connection.end();
        await endPromise;
      }
      expect(shutdownErrors).toEqual(1);
    } finally {
      await birdbox.stop();
    }
  };

  test('PgClient Graceful Shutdown SIGTERM', async () => {
    await betweenQueries('SIGTERM');
  });

  test('PgClient Graceful Shutdown SIGINT', async () => {
    await betweenQueries('SIGINT');
  });

  const midTransaction = async (signal: 'SIGTERM' | 'SIGINT') => {
    const birdbox = await makeBirdbox(signal === 'SIGTERM' ? shortGracefulTimeoutSecs : longGracefulTimeoutSecs);
    try {
      const connection: PgClient = unconnectedPostgresClient('admin', 'admin_password');

      let endResolve: () => void;
      const endPromise = new Promise<void>((res, _rej) => {
        endResolve = res;
      });
      await connection.connect();

      let connectionEnded = false;
      connection.on('end', () => {
        connectionEnded = true;
        endResolve();
      });
      let logTerminationErrors = true;
      let shutdownErrors = 0;
      let expectedShutdownErrors: number;
      connection.on('error', (e: Error) => {
        const err = e as any;
        if (err.severity === 'FATAL' && err.code === '57P01') {
          shutdownErrors += 1;
        } else if (logTerminationErrors && err.message !== 'Connection terminated unexpectedly') {
          console.log(err);
        }
      });
      try {
        const res = await connection.query(
          'BEGIN'
        );
        expect(res.command).toEqual('BEGIN');

        // Sanity check: our SQL api client connection is still open.  (I mean, we haven't even
        // killed Cube.)
        await yieldImmediate();
        expect(connectionEnded).toBe(false);

        logTerminationErrors = false;
        birdbox.killCube(signal);
        const code = await birdbox.onCubeExit();

        /* This test may be overspecifying -- we have no requirement that the exit code be non-zero
        if graceful shutdown times out.  But for testing purposes, it does provide a handy way to
        determine which mechanism caused the server to shut down. */
        if (signal === 'SIGTERM') {
          expectedShutdownErrors = 0;
          expect(code).not.toEqual(0);
        } else {
          expectedShutdownErrors = 1;
          expect(code).toEqual(0);
        }
      } finally {
        // Normally the connection ends by server shutdown, and this .end() call returns
        // a Promise which never gets fulfilled.  So we sign up for and wait for the event.
        const _ = connection.end();
        await endPromise;
      }

      await yieldImmediate();
      expect(shutdownErrors).toEqual(expectedShutdownErrors);
    } finally {
      await birdbox.stop();
    }
  };

  test('PgClient Graceful Shutdown Mid-Transaction SIGTERM', async () => {
    await midTransaction('SIGTERM');
  });

  test('PgClient Graceful Shutdown Mid-Transaction SIGINT', async () => {
    await midTransaction('SIGINT');
  });

  const waitForTransaction = async (signal: 'SIGTERM') => {
    const birdbox = await makeBirdbox(longGracefulTimeoutSecs);
    try {
      const connection: PgClient = unconnectedPostgresClient('admin', 'admin_password');

      let endResolve: () => void;
      const endPromise = new Promise<void>((res, _rej) => {
        endResolve = res;
      });
      await connection.connect();

      let connectionEnded = false;
      connection.on('end', () => {
        connectionEnded = true;
        endResolve();
      });
      let logTerminationErrors = true;
      let shutdownErrors = 0;
      connection.on('error', (e: Error) => {
        const err = e as any;
        if (err.severity === 'FATAL' && err.code === '57P01') {
          shutdownErrors += 1;
        } else if (logTerminationErrors && err.message !== 'Connection terminated unexpectedly') {
          console.log(err);
        }
      });
      try {
        // 1. Begin a transaction
        const beginRes = await connection.query(
          'BEGIN'
        );
        expect(beginRes.command).toEqual('BEGIN');

        // 2. Kill Cube with SIGTERM.
        birdbox.killCube(signal);

        // 3. Run a query (because why not?).
        const selectRes = await connection.query(
          'SELECT COUNT(*) as cn, "status" FROM Orders GROUP BY 2 ORDER BY cn DESC'
        );
        expect(selectRes.rows).toMatchSnapshot('sql_orders');

        // Our SQL api client connection is still open.
        await yieldImmediate();
        expect(connectionEnded).toBe(false);

        logTerminationErrors = false;

        // 4. Commit the transaction (or rollback).
        const commitRes = await connection.query(
          'COMMIT'
        );
        expect(commitRes.command).toEqual('COMMIT');

        // 5. Now wait for the Cube exit result.
        const code = await birdbox.onCubeExit();
        expect(code).toEqual(0);
      } finally {
        // Normally the connection ends by server shutdown, and this .end() call returns
        // a Promise which never gets fulfilled.  So we sign up for and wait for the event.
        const _ = connection.end();
        await endPromise;
      }

      await yieldImmediate();
      expect(shutdownErrors).toEqual(1);
    } finally {
      await birdbox.stop();
    }
  };

  test('PgClient Graceful Shutdown Finishing Transaction', async () => {
    await waitForTransaction('SIGTERM');
  });
});
