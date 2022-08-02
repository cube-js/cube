// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, jest, expect } from '@jest/globals';
import { Client as PgClient } from 'pg';
import { PostgresDBRunner } from '@cubejs-backend/testing-shared';
import type { StartedTestContainer } from 'testcontainers';

import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_CONFIG } from './smoke-tests';

describe('SQL API', () => {
  jest.setTimeout(60 * 5 * 1000);

  let connection: PgClient;
  let birdbox: BirdBox;
  let db: StartedTestContainer;

  // TODO: Random port?
  const pgPort = 5656;
  let connectionId = 0;

  async function createPostgresClient(user: string, password: string) {
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
    conn.on('error', (err) => {
      console.log(err);
    });
    conn.on('end', () => {
      console.debug(`[pg] end ${currentConnId}`);
    });

    await conn.connect();

    return conn;
  }

  beforeAll(async () => {
    db = await PostgresDBRunner.startContainer({});
    birdbox = await getBirdbox(
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
      },
      {
        schemaDir: 'postgresql/schema',
        cubejsConfig: 'postgresql/single/sqlapi.js',
      }
    );
    connection = await createPostgresClient('admin', 'admin_password');
  });

  afterAll(async () => {
    await birdbox.stop();
    await db.stop();
    // await not working properly
    await connection.end();
  });

  describe('Postgres (Auth)', () => {
    test('Success Admin', async () => {
      const conn = await createPostgresClient('admin', 'admin_password');

      try {
        const res = await conn.query('SELECT "user", "uid" FROM SecurityContextTest');
        expect(res.rows).toEqual([{
          user: 'admin',
          uid: '1'
        }]);
      } finally {
        await conn.end();
      }
    });

    test('Error Admin Password', async () => {
      try {
        await createPostgresClient('admin', 'wrong_password');

        throw new Error('Code must thrown auth error, something wrong...');
      } catch (e) {
        expect(e.message).toContain('password authentication failed for user "admin"');
      }
    });

    test('Security Context (Admin -> Moderator) - allowed superuser', async () => {
      const conn = await createPostgresClient('admin', 'admin_password');

      try {
        const res = await conn.query('SELECT "user", "uid" FROM SecurityContextTest WHERE __user = \'moderator\'');
        expect(res.rows).toEqual([{
          user: 'moderator',
          uid: '2'
        }]);
      } finally {
        await conn.end();
      }
    });

    test('Security Context (Moderator -> Usr1) - allowed sqlCanChangeUser', async () => {
      const conn = await createPostgresClient('moderator', 'moderator_password');

      try {
        const res = await conn.query('SELECT "user", "uid" FROM SecurityContextTest WHERE __user = \'usr1\'');
        expect(res.rows).toEqual([{
          user: 'usr1',
          uid: '3'
        }]);
      } finally {
        await conn.end();
      }
    });

    test('Security Context (Moderator -> Usr2) - not allowed', async () => {
      const conn = await createPostgresClient('moderator', 'moderator_password');

      try {
        await conn.query('SELECT "user", "uid" FROM SecurityContextTest WHERE __user = \'usr2\'');

        throw new Error('Code must thrown auth error, something wrong...');
      } catch (e) {
        expect(e.message).toContain('You cannot change security context via __user from moderator to usr2, because it\'s not allowed');
      } finally {
        await conn.end();
      }
    });

    test('Security Context (Usr1 -> Moderator) - not allowed', async () => {
      const conn = await createPostgresClient('usr1', 'user1_password');

      try {
        await conn.query('SELECT "user", "uid" FROM SecurityContextTest WHERE __user = \'moderator\'');

        throw new Error('Code must thrown auth error, something wrong...');
      } catch (e) {
        expect(e.message).toContain('You cannot change security context via __user from usr1 to moderator, because it\'s not allowed');
      } finally {
        await conn.end();
      }
    });
  });

  describe('Postgres (Data)', () => {
    test('SELECT COUNT(*) as cn, "status" FROM Orders GROUP BY 2 ORDER BY cn DESC', async () => {
      const res = await connection.query('SELECT COUNT(*) as cn, "status" FROM Orders GROUP BY 2 ORDER BY cn DESC');
      expect(res.rows).toMatchSnapshot('sql_orders');
    });
  });
});
