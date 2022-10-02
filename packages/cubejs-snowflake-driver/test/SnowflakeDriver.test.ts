/* eslint-disable no-use-before-define */
import { DriverInterface } from '@cubejs-backend/base-driver';
import type { SnowflakeDriverExportBucket, SnowflakeDriverOptions } from '../src/SnowflakeDriver';
// eslint-disable-next-line global-require
import { SnowflakeDriver as SnowflakeDriverType } from '../src/SnowflakeDriver';

describe('SnowflakeDriver', () => {
  beforeEach(() => {
    jest.resetModules();
  });
  afterEach(() => {
    jest.clearAllMocks();
  });
  describe('downloadQueryResults', () => {
    describe('in memory', () => {
      it('success', async () => {
        const stubs = [
          { regexp: /downloadQueryResults_test_table/, rows: [{ id: 1, name: 'test' }] },
          { regexp: /DESC RESULT last_query_id\(\)/, rows: [{ type: 'NUMBER(1,1)', name: 'id' }, { type: 'VARCHAR(16)', name: 'test' }] }
        ];
        mockSnowflake(stubs);
        
        const driver = createSnowflakeDriver();
  
        const result = await driver.downloadQueryResults('SELECT * FROM \'downloadQueryResults_test_table\'', [], { maxFileSize: 60, highWaterMark: 100 });
  
        expect(result).toEqual({ rows: stubs[0].rows, types: [{ type: 'decimal(1,1)', name: 'id' }, { type: 'VARCHAR(16)', name: 'test' }] });
      });
    });

    describe('unload', () => {
      it('success', async () => {
        const bucket: SnowflakeDriverExportBucket = { bucketType: 's3', bucketName: 'some_random_name', keyId: 'random_key', secretKey: 'secrect', region: 'us-east-2' };
        const sql = 'SELECT * FROM table';
        const stubs = [
          { regexp: new RegExp(`COPY INTO '${bucket.bucketType}://${bucket.bucketName}/(.+)' FROM \\(${sql.replace('*', '\\*')}\\)`), rows: [{ rows_unloaded: 1 }] },
          { regexp: new RegExp(`${sql.replace('*', '\\*')} LIMIT 1`), rows: [] },
          { regexp: new RegExp('DESC RESULT last_query_id()'), rows: [{ type: 'NUMBER(1,1)', name: 'id' }, { type: 'VARCHAR(16)', name: 'test' }] }
        ];
        const contents = [{ Key: 'file1' }, { Key: 'file2' }];
        mockAwsS3(contents);
        mockSnowflake(stubs);

        const driver = createSnowflakeDriver({ exportBucket: bucket });

        const result = await driver.downloadQueryResults(sql, [], { maxFileSize: 60, highWaterMark: 100 });

        expect(result).toEqual({ csvFile: contents.map(c => c.Key), types: [{ type: 'decimal(1,1)', name: 'id' }, { type: 'VARCHAR(16)', name: 'test' }] });
      });

      it('throws an error if unload doesn\'t return anything', async () => {
        const bucket: SnowflakeDriverExportBucket = { bucketType: 's3', bucketName: 'some_random_name', keyId: 'random_key', secretKey: 'secrect', region: 'us-east-2' };
        const sql = 'SELECT * FROM table';
        const stubs = [
          { regexp: new RegExp(`COPY INTO '${bucket.bucketType}://${bucket.bucketName}/(.+)' FROM \\(${sql.replace('*', '\\*')}\\)`), rows: null },
          { regexp: new RegExp(`${sql.replace('*', '\\*')} LIMIT 1`), rows: [] },
          { regexp: new RegExp('DESC RESULT last_query_id()'), rows: [{ type: 'NUMBER(1,1)', name: 'id' }, { type: 'VARCHAR(16)', name: 'test' }] }
        ];
        const contents = [{ Key: 'file1' }, { Key: 'file2' }];
        mockAwsS3(contents);
        mockSnowflake(stubs);

        const driver = createSnowflakeDriver({ exportBucket: bucket });

        await expect(driver.downloadQueryResults(sql, [], { maxFileSize: 60, highWaterMark: 100 })).rejects.toThrow(/Snowflake doesn't return anything on UNLOAD operation/);
      });

      it('throws an error if rows unloaded equals zero', async () => {
        const bucket: SnowflakeDriverExportBucket = { bucketType: 's3', bucketName: 'some_random_name', keyId: 'random_key', secretKey: 'secrect', region: 'us-east-2' };
        const sql = 'SELECT * FROM table';
        const stubs = [
          { regexp: new RegExp(`COPY INTO '${bucket.bucketType}://${bucket.bucketName}/(.+)' FROM \\(${sql.replace('*', '\\*')}\\)`), rows: [{ rows_unloaded: '0' }] },
          { regexp: new RegExp(`${sql.replace('*', '\\*')} LIMIT 1`), rows: [] },
          { regexp: new RegExp('DESC RESULT last_query_id()'), rows: [{ type: 'NUMBER(1,1)', name: 'id' }, { type: 'VARCHAR(16)', name: 'test' }] }
        ];
        const contents = [{ Key: 'file1' }, { Key: 'file2' }];
        mockAwsS3(contents);
        mockSnowflake(stubs);

        const driver = createSnowflakeDriver({ exportBucket: bucket });

        await expect(driver.downloadQueryResults(sql, [], { maxFileSize: 60, highWaterMark: 100 })).rejects.toThrow(/Snowflake unloads zero rows on UNLOAD operation/);
      });

      it('test different type casts', async () => {
        const bucket: SnowflakeDriverExportBucket = { bucketType: 's3', bucketName: 'some_random_name', keyId: 'random_key', secretKey: 'secrect', region: 'us-east-2' };
        const sql = 'SELECT * FROM table';
        const stubs = [
          { regexp: new RegExp(`COPY INTO '${bucket.bucketType}://${bucket.bucketName}/(.+)' FROM \\(${sql.replace('*', '\\*')}\\)`), rows: [{ rows_unloaded: 1 }] },
          { regexp: new RegExp(`${sql.replace('*', '\\*')} LIMIT 1`), rows: [] },
          { regexp: new RegExp('DESC RESULT last_query_id()'),
            rows: [
              { type: 'NUMBER(1,1)', name: 'count' },
              { type: 'DECIMAL(1,1)', name: 'count1' },
              { type: 'NUMERIC(1,1)', name: 'count2' },
              { type: 'NUMBER(1,0)', name: 'count' },
              { type: 'DECIMAL(1,0)', name: 'count1' },
              { type: 'NUMERIC(1,0)', name: 'count2' },
              { type: 'float', name: 'float_count' },
              { type: 'VARCHAR(16)', name: 'test' }
            ]
          }
        ];
        const contents = [{ Key: 'file1' }, { Key: 'file2' }];
        mockAwsS3(contents);
        mockSnowflake(stubs);

        const driver = createSnowflakeDriver({ exportBucket: bucket });

        const result = await driver.downloadQueryResults(sql, [], { maxFileSize: 60, highWaterMark: 100 });

        const expectedTypes = [
          { type: 'decimal(1,1)', name: 'count' },
          { type: 'decimal(1,1)', name: 'count1' },
          { type: 'decimal(1,1)', name: 'count2' },
          { type: 'int', name: 'count' },
          { type: 'int', name: 'count1' },
          { type: 'int', name: 'count2' },
          { type: 'float', name: 'float_count' },
          { type: 'VARCHAR(16)', name: 'test' }
        ];
        expect(result).toEqual({ csvFile: contents.map(c => c.Key), types: expectedTypes });
      });
    });
  });

  describe('unload', () => {
    it('throws an error if no export bucket was passed', async () => {
      const stubs: any[] = [];
      mockSnowflake(stubs);
      
      const driver = createSnowflakeDriver();

      const promise = () => driver.unload('table_1', { maxFileSize: 60 });

      await expect(promise).rejects.toThrow(/Unload is not configured/);
    });

    it('unloadFromTable success', async () => {
      const bucket: SnowflakeDriverExportBucket = { bucketType: 's3', bucketName: 'some_random_name', keyId: 'random_key', secretKey: 'secrect', region: 'us-east-2' };
      const table = 'my_main_schema.table';
      const stubs = [
        { regexp: new RegExp(`COPY INTO '${bucket.bucketType}://${bucket.bucketName}/(.+)' FROM ${table}`), rows: [{ rows_unloaded: 1 }] },
        {
          regexp: new RegExp('ORDER\\s+BY\\s+ORDINAL_POSITION'),
          statement: createSnowflakeStatementMock([]),
          rows: [{ COLUMN_NAME: 'id', DATA_TYPE: 'TEXT' }, { COLUMN_NAME: 'count', DATA_TYPE: 'int' }, { COLUMN_NAME: 'date', DATA_TYPE: 'TIMESTAMP_NTZ' }] },
      ];
      const contents = [{ Key: 'file1' }, { Key: 'file2' }];
      mockAwsS3(contents);
      mockSnowflake(stubs);

      const driver = createSnowflakeDriver({ exportBucket: bucket });

      const result = await driver.unload(table, { maxFileSize: 60 });

      expect(result).toEqual({ csvFile: contents.map(c => c.Key), types: [{ type: 'text', name: 'id' }, { name: 'count', type: 'int' }, { name: 'date', type: 'timestamp' }] });
    });

    it('unloadFromTable throws an error if unload doesn\'t return anything', async () => {
      const bucket: SnowflakeDriverExportBucket = { bucketType: 's3', bucketName: 'some_random_name', keyId: 'random_key', secretKey: 'secrect', region: 'us-east-2' };
      const table = 'my_main_schema.table';
      const stubs = [
        { regexp: new RegExp(`COPY INTO '${bucket.bucketType}://${bucket.bucketName}/(.+)' FROM ${table}`), rows: null },
        {
          regexp: new RegExp('ORDER\\s+BY\\s+ORDINAL_POSITION'),
          statement: createSnowflakeStatementMock([]),
          rows: [{ COLUMN_NAME: 'id', DATA_TYPE: 'TEXT' }, { COLUMN_NAME: 'count', DATA_TYPE: 'int' }, { COLUMN_NAME: 'date', DATA_TYPE: 'TIMESTAMP_NTZ' }] },
      ];
      const contents = [{ Key: 'file1' }, { Key: 'file2' }];
      mockAwsS3(contents);
      mockSnowflake(stubs);

      const driver = createSnowflakeDriver({ exportBucket: bucket });

      await expect(driver.unload(table, { maxFileSize: 60 })).rejects.toThrow(/Snowflake doesn't return anything on UNLOAD operation/);
    });

    it('unloadFromTable throws an error if rows unloaded equals zero', async () => {
      const bucket: SnowflakeDriverExportBucket = { bucketType: 's3', bucketName: 'some_random_name', keyId: 'random_key', secretKey: 'secrect', region: 'us-east-2' };
      const table = 'my_main_schema.table';
      const stubs = [
        { regexp: new RegExp(`COPY INTO '${bucket.bucketType}://${bucket.bucketName}/(.+)' FROM ${table}`), rows: [{ rows_unloaded: '0' }] },
        {
          regexp: new RegExp('ORDER\\s+BY\\s+ORDINAL_POSITION'),
          statement: createSnowflakeStatementMock([]),
          rows: [{ COLUMN_NAME: 'id', DATA_TYPE: 'TEXT' }, { COLUMN_NAME: 'count', DATA_TYPE: 'int' }, { COLUMN_NAME: 'date', DATA_TYPE: 'TIMESTAMP_NTZ' }] },
      ];
      const contents = [{ Key: 'file1' }, { Key: 'file2' }];
      mockAwsS3(contents);
      mockSnowflake(stubs);

      const driver = createSnowflakeDriver({ exportBucket: bucket });

      await expect(driver.unload(table, { maxFileSize: 60 })).rejects.toThrow(/Snowflake unloads zero rows on UNLOAD operation/);
    });

    it('unloadFromSql success', async () => {
      const bucket: SnowflakeDriverExportBucket = { bucketType: 's3', bucketName: 'some_random_name', keyId: 'random_key', secretKey: 'secrect', region: 'us-east-2' };
      const table = 'my_main_schema.table';
      const sql = 'SELECT * FROM my_main_schema.table';
      const stubs = [
        { regexp: new RegExp(`COPY INTO '${bucket.bucketType}://${bucket.bucketName}/(.+)' FROM \\(${sql.replace('*', '\\*')}\\)`), rows: [{ rows_unloaded: 1 }] },
        { regexp: new RegExp(`${sql.replace('*', '\\*')} LIMIT 1`), rows: [] },
        { regexp: new RegExp('DESC RESULT last_query_id()'), rows: [{ type: 'NUMBER(1,1)', name: 'id' }, { type: 'VARCHAR(16)', name: 'test' }] }
      ];
      const contents = [{ Key: 'file1' }, { Key: 'file2' }];
      mockAwsS3(contents);
      mockSnowflake(stubs);

      const driver = createSnowflakeDriver({ exportBucket: bucket });

      const result = await driver.unload(table, { maxFileSize: 60, query: { sql, params: [] } });

      expect(result).toEqual({ csvFile: contents.map(c => c.Key), types: [{ type: 'decimal(1,1)', name: 'id' }, { type: 'VARCHAR(16)', name: 'test' }] });
    });

    it('unloadFromSql test different type casts', () => {});

    it('unloadFromSql throws an error if unload doesn\'t return anything', async () => {
      const bucket: SnowflakeDriverExportBucket = { bucketType: 's3', bucketName: 'some_random_name', keyId: 'random_key', secretKey: 'secrect', region: 'us-east-2' };
      const table = 'my_main_schema.table';
      const sql = 'SELECT * FROM my_main_schema.table';
      const stubs = [
        { regexp: new RegExp(`COPY INTO '${bucket.bucketType}://${bucket.bucketName}/(.+)' FROM \\(${sql.replace('*', '\\*')}\\)`), rows: null },
        { regexp: new RegExp(`${sql.replace('*', '\\*')} LIMIT 1`), rows: [] },
        { regexp: new RegExp('DESC RESULT last_query_id()'), rows: [{ type: 'NUMBER(1,1)', name: 'id' }, { type: 'VARCHAR(16)', name: 'test' }] }
      ];
      const contents = [{ Key: 'file1' }, { Key: 'file2' }];
      mockAwsS3(contents);
      mockSnowflake(stubs);

      const driver = createSnowflakeDriver({ exportBucket: bucket });

      await expect(driver.unload(table, { maxFileSize: 60, query: { sql, params: [] } })).rejects.toThrow(/Snowflake doesn't return anything on UNLOAD operation/);
    });

    it('unloadFromSql throws an error if rows unloaded equals zero', async () => {
      const bucket: SnowflakeDriverExportBucket = { bucketType: 's3', bucketName: 'some_random_name', keyId: 'random_key', secretKey: 'secrect', region: 'us-east-2' };
      const table = 'my_main_schema.table';
      const sql = 'SELECT * FROM my_main_schema.table';
      const stubs = [
        { regexp: new RegExp(`COPY INTO '${bucket.bucketType}://${bucket.bucketName}/(.+)' FROM \\(${sql.replace('*', '\\*')}\\)`), rows: [{ rows_unloaded: '0' }] },
        { regexp: new RegExp(`${sql.replace('*', '\\*')} LIMIT 1`), rows: [] },
        { regexp: new RegExp('DESC RESULT last_query_id()'), rows: [{ type: 'NUMBER(1,1)', name: 'id' }, { type: 'VARCHAR(16)', name: 'test' }] }
      ];
      const contents = [{ Key: 'file1' }, { Key: 'file2' }];
      mockAwsS3(contents);
      mockSnowflake(stubs);

      const driver = createSnowflakeDriver({ exportBucket: bucket });

      await expect(driver.unload(table, { maxFileSize: 60, query: { sql, params: [] } })).rejects.toThrow(/Snowflake unloads zero rows on UNLOAD operation/);
    });
  });
});

function createSnowflakeDriver(config: Partial<SnowflakeDriverOptions> = {} as SnowflakeDriverOptions) {
  // eslint-disable-next-line global-require, @typescript-eslint/no-shadow
  const { SnowflakeDriver } = require('../src/SnowflakeDriver');
  const driver: SnowflakeDriverType = new SnowflakeDriver(config);

  return driver;
}

/* eslint-disable no-use-before-define */
type Stub = {regexp: RegExp, rows: unknown[] | null, statement?: SnowflakeStatementMock};

const createExecuteMock = (stubs: Stub[]) => ({ sqlText, complete }: {connection: any, sqlText: string, binds: unknown[], rehydrate?: boolean, complete: Function}) => {
  if (sqlText === 'ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 600') {
    return complete(undefined, undefined, []);
  }
  if (sqlText === 'ALTER SESSION SET TIMEZONE = \'UTC\'') {
    return complete(undefined, undefined, []);
  }
  for (const s of stubs) {
    if (sqlText.match(s.regexp)) {
      // eslint-disable-next-line consistent-return
      return complete(undefined, s.statement, s.rows);
    }
  }

  throw new Error(`unmatched query: ${sqlText}`);
};

function mockSnowflake(stubs: Stub[]) {
  const mock = createExecuteMock(stubs);
  jest.mock('snowflake-sdk', () => ({ createConnection: () => ({ execute: mock, connect: (cb: Function) => { cb(); }, isUp: () => true }) }));
}

function mockAwsS3(contents: {Key: string}[]) {
  class MockS3 {
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    public constructor() {}

    public async listObjectsV2() {
      return { Contents: contents };
    }
  }

  class MockGetObjectCommand {
    public Key: string;

    public constructor({
      Key,
    }: {Key: string}) {
      this.Key = Key;
    }
  }

  jest.mock('@aws-sdk/client-s3', () => ({ S3: MockS3, GetObjectCommand: MockGetObjectCommand }));

  jest.mock('@aws-sdk/s3-request-presigner', () => ({ getSignedUrl: (storage: any, command: MockGetObjectCommand) => command.Key }));
}

type ColumnArg = {type: string; name: string};

type Column = {
  getType(): string
  getName(): string
  
};
type SnowflakeStatementMock = {
  getSqlText(): string;
  getColumns: () => Column[]
};

function createSnowflakeStatementMock(columns: ColumnArg[]): SnowflakeStatementMock {
  const statement = {
    getSqlText() {
      return 'random_string';
    },
    getColumns() {
      return columns.map(c => ({
        getType() {
          return c.type;
        },
        getName() {
          return c.name;
        },
      
      }));
    }
  };

  return statement;
}
