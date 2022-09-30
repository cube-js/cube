import type { SnowflakeDriverExportBucket, SnowflakeDriverOptions } from '../src/SnowflakeDriver';

/* eslint-disable no-use-before-define */
type Stub = {regexp: RegExp, result: any};

const createExecuteMock = (stubs: Stub[]) => ({ sqlText, complete }: {connection: any, sqlText: string, binds: unknown[], rehydrate?: boolean, complete: Function}) => {
  if (sqlText === 'ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 600') {
    return complete(undefined, undefined, []);
  }
  if (sqlText === 'ALTER SESSION SET TIMEZONE = \'UTC\'') {
    return complete(undefined, undefined, []);
  }

  console.log('sqlTextsqlText', sqlText);
  for (const s of stubs) {
    if (sqlText.match(s.regexp)) {
      // eslint-disable-next-line consistent-return
      return complete(undefined, undefined, s.result);
    }
  }

  throw new Error('unmatched query');
};

function mockSnowflake(stubs: Stub[]) {
  const mock = createExecuteMock(stubs);
  jest.mock('snowflake-sdk', () => ({ createConnection: () => ({ execute: mock, connect: (cb: Function) => { cb(); } }) }));
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
          { regexp: /downloadQueryResults_test_table/, result: [{ id: 1, name: 'test' }] },
          { regexp: /DESC RESULT last_query_id\(\)/, result: [{ type: 'NUMBER(1,1)', name: 'id' }, { type: 'VARCHAR(16)', name: 'test' }] }
        ];
        mockSnowflake(stubs);
        
        const driver = createSnowflakeDriver();
  
        const result = await driver.downloadQueryResults('SELECT * FROM \'downloadQueryResults_test_table\'', [], { maxFileSize: 60, highWaterMark: 100 });
  
        expect(result).toEqual({ rows: stubs[0].result, types: [{ type: 'decimal(1,1)', name: 'id' }, { type: 'VARCHAR(16)', name: 'test' }] });
      });
    });

    describe('unload', () => {
      it('success', async () => {
        const bucket: SnowflakeDriverExportBucket = { bucketType: 's3', bucketName: 'some_random_name', keyId: 'random_key', secretKey: 'secrect', region: 'us-east-2' };
        const sql = 'SELECT * FROM table';
        const stubs = [
          { regexp: new RegExp(`COPY INTO '${bucket.bucketType}://${bucket.bucketName}/(.+)' FROM \\(${sql.replace('*', '\\*')}\\)`), result: [{ rows_unloaded: 1 }] },
          { regexp: new RegExp(`${sql.replace('*', '\\*')} LIMIT 1`), result: [] },
          { regexp: new RegExp('DESC RESULT last_query_id()'), result: [{ type: 'NUMBER(1,1)', name: 'id' }, { type: 'VARCHAR(16)', name: 'test' }] }
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
          { regexp: new RegExp(`COPY INTO '${bucket.bucketType}://${bucket.bucketName}/(.+)' FROM \\(${sql.replace('*', '\\*')}\\)`), result: null },
          { regexp: new RegExp(`${sql.replace('*', '\\*')} LIMIT 1`), result: [] },
          { regexp: new RegExp('DESC RESULT last_query_id()'), result: [{ type: 'NUMBER(1,1)', name: 'id' }, { type: 'VARCHAR(16)', name: 'test' }] }
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
          { regexp: new RegExp(`COPY INTO '${bucket.bucketType}://${bucket.bucketName}/(.+)' FROM \\(${sql.replace('*', '\\*')}\\)`), result: [{ rows_unloaded: '0' }] },
          { regexp: new RegExp(`${sql.replace('*', '\\*')} LIMIT 1`), result: [] },
          { regexp: new RegExp('DESC RESULT last_query_id()'), result: [{ type: 'NUMBER(1,1)', name: 'id' }, { type: 'VARCHAR(16)', name: 'test' }] }
        ];
        const contents = [{ Key: 'file1' }, { Key: 'file2' }];
        mockAwsS3(contents);
        mockSnowflake(stubs);

        const driver = createSnowflakeDriver({ exportBucket: bucket });

        await expect(driver.downloadQueryResults(sql, [], { maxFileSize: 60, highWaterMark: 100 })).rejects.toThrow(/Snowflake unloads zero rows on UNLOAD operation/);
      });
    });
  });

  describe('unload', () => {
    it('throws error if no export bucket was passed', () => {
      
    });
    it('unloadFromTable success', () => {
      
    });

    it('unloadFromTable throws an error if unload doesn\'t return anything', () => {
      
    });

    it('unloadFromTable throws an error if rows unloaded equals zero', () => {
      
    });

    it('unloadFromSql success', () => {
      
    });

    it('unloadFromSql throws an error if unload doesn\'t return anything', () => {
      
    });

    it('unloadFromSql throws an error if rows unloaded equals zero', () => {
      
    });
  });
});

function createSnowflakeDriver(config: Partial<SnowflakeDriverOptions> = {} as SnowflakeDriverOptions) {
  // eslint-disable-next-line global-require
  const { SnowflakeDriver } = require('../src/SnowflakeDriver');
  const driver = new SnowflakeDriver(config);

  return driver;
}
