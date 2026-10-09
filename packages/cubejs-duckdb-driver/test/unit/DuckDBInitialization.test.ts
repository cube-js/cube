import { DuckDBInstance } from '@duckdb/node-api';
import { DuckDBDriver } from '../../src';

const { version } = require('../../../package.json');

class TestDuckDBDriver extends DuckDBDriver {
  public async initialize() {
    await this.getInitiatedState();
  }
}

describe('DuckDBDriver initialization', () => {
  const originalEnv = { ...process.env };
  let driver: TestDuckDBDriver;
  let connection: { run: jest.Mock, closeSync: jest.Mock };
  let instance: { connect: jest.Mock, closeSync: jest.Mock };

  const clearDuckDBEnv = () => {
    for (const key of Object.keys(process.env)) {
      if (key.startsWith('CUBEJS_DB_DUCKDB_')) {
        delete process.env[key];
      }
    }
  };

  beforeEach(() => {
    clearDuckDBEnv();
    connection = { run: jest.fn().mockResolvedValue({}), closeSync: jest.fn() };
    instance = { connect: jest.fn().mockResolvedValue(connection), closeSync: jest.fn() };
    jest.spyOn(DuckDBInstance, 'create').mockResolvedValue(instance as unknown as DuckDBInstance);
    driver = new TestDuckDBDriver();
  });

  afterEach(async () => {
    await driver.release();
    jest.restoreAllMocks();
    clearDuckDBEnv();
    Object.assign(process.env, originalEnv);
  });

  test.each([
    [{}, ':memory:', undefined],
    [{ databasePath: '/test.duckdb' }, '/test.duckdb', undefined],
    [{ motherDuckToken: 'test-token' }, `md:?motherduck_token=test-token&custom_user_agent=Cube/${version}`, { custom_user_agent: `Cube/${version}` }],
    [{ databasePath: '/test.duckdb', motherDuckToken: 'test-token' }, '/test.duckdb', { custom_user_agent: `Cube/${version}` }],
  ])('selects the database for %j', async (config, path, options) => {
    driver = new TestDuckDBDriver(config);
    await driver.initialize();
    expect(DuckDBInstance.create).toHaveBeenCalledWith(path, options);
  });

  test('applies settings, credential chain, extensions and initSql in order', async () => {
    process.env.CUBEJS_DB_DUCKDB_S3_REGION = 'eu-west-1';
    process.env.CUBEJS_DB_DUCKDB_MEMORY_LIMIT = '256MB';
    process.env.CUBEJS_DB_DUCKDB_EXTENSIONS = 'httpfs, json';
    process.env.CUBEJS_DB_DUCKDB_COMMUNITY_EXTENSIONS = 'h3';
    driver = new TestDuckDBDriver({ duckdbS3UseCredentialChain: true, initSql: 'SELECT 1; SELECT 2;' });

    await driver.initialize();

    expect(connection.run.mock.calls.map(([sql]) => sql)).toEqual([
      "SET s3_region='eu-west-1'",
      "SET memory_limit='256MB'",
      "CREATE SECRET (TYPE S3, PROVIDER 'CREDENTIAL_CHAIN')",
      'INSTALL httpfs', 'INSTALL json', 'LOAD httpfs', 'LOAD json',
      'INSTALL h3 FROM community', 'LOAD h3',
      'SELECT 1; SELECT 2;',
    ]);
  });

  test('continues after setting and initSql errors', async () => {
    process.env.CUBEJS_DB_DUCKDB_MEMORY_LIMIT = 'invalid';
    driver = new TestDuckDBDriver({ initSql: 'INVALID SQL' });
    connection.run.mockRejectedValue(new Error('invalid SQL'));

    await driver.initialize();

    expect(connection.run).toHaveBeenCalledWith('INVALID SQL');
    expect(instance.closeSync).not.toHaveBeenCalled();
  });

  test.each(['INSTALL json', 'LOAD json', "CREATE SECRET (TYPE S3, PROVIDER 'CREDENTIAL_CHAIN')"])(
    'closes the instance and retries after init fails at %s',
    async (statement) => {
      process.env.CUBEJS_DB_DUCKDB_EXTENSIONS = 'json';
      driver = new TestDuckDBDriver({ duckdbS3UseCredentialChain: true });
      connection.run.mockImplementation(async (sql: string) => {
        if (sql === statement) {
          throw new Error('initialization failed');
        }
      });

      await expect(driver.initialize()).rejects.toThrow('initialization failed');
      expect(connection.closeSync).toHaveBeenCalledTimes(1);
      expect(instance.closeSync).toHaveBeenCalledTimes(1);
      expect(connection.closeSync.mock.invocationCallOrder[0]).toBeLessThan(instance.closeSync.mock.invocationCallOrder[0]);

      connection.run.mockResolvedValue({});
      await driver.initialize();
      expect(DuckDBInstance.create).toHaveBeenCalledTimes(2);
    }
  );

  test('closes the instance when connecting fails and allows retry', async () => {
    instance.connect.mockRejectedValueOnce(new Error('cannot connect'));

    await expect(driver.initialize()).rejects.toThrow('cannot connect');
    expect(instance.closeSync).toHaveBeenCalledTimes(1);

    await driver.initialize();
    expect(DuckDBInstance.create).toHaveBeenCalledTimes(2);
  });

  test('release closes the connection before the instance, once', async () => {
    await driver.initialize();

    await Promise.all([driver.release(), driver.release()]);

    expect(connection.closeSync).toHaveBeenCalledTimes(1);
    expect(instance.closeSync).toHaveBeenCalledTimes(1);
    expect(connection.closeSync.mock.invocationCallOrder[0]).toBeLessThan(instance.closeSync.mock.invocationCallOrder[0]);
  });
});
