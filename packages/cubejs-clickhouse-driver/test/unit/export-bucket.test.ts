import type { TableStructure, UnloadOptions } from '@cubejs-backend/base-driver';

import { ClickHouseDriver } from '../../src';

jest.mock('@clickhouse/client', () => ({
  ...jest.requireActual('@clickhouse/client'),
  createClient: jest.fn(() => ({
    close: jest.fn(),
  })),
}));

jest.mock('uuid', () => ({
  ...jest.requireActual('uuid'),
  v4: () => 'export-uuid',
}));

type S3Options = Record<string, unknown>;

class TestClickHouseDriver extends ClickHouseDriver {
  public commands: string[] = [];

  public s3Options: S3Options[] = [];

  public credentialRequests: S3Options[] = [];

  public override async command(query: string): Promise<void> {
    this.commands.push(query);
  }

  public override async queryColumnTypes(): Promise<TableStructure> {
    return [{ name: 'id', type: 'int' }];
  }

  public override async extractUnloadedFilesFromS3(options: S3Options): Promise<string[]> {
    this.s3Options.push(options);
    return ['https://signed/export.csv.gz'];
  }

  public override async resolveS3Credentials(options: S3Options) {
    this.credentialRequests.push(options);
    return super.resolveS3Credentials(options);
  }
}

const ENV_KEYS = [
  'CUBEJS_DB_EXPORT_BUCKET_TYPE',
  'CUBEJS_DB_EXPORT_BUCKET',
  'CUBEJS_DB_EXPORT_BUCKET_AWS_REGION',
  'CUBEJS_DB_EXPORT_BUCKET_AWS_KEY',
  'CUBEJS_DB_EXPORT_BUCKET_AWS_SECRET',
  'AWS_ACCESS_KEY_ID',
  'AWS_SECRET_ACCESS_KEY',
  'AWS_SESSION_TOKEN',
  'AWS_REGION',
];

const unloadOptions: UnloadOptions = {
  maxFileSize: 64,
  query: { sql: 'SELECT id FROM t WHERE id > ?', params: [1] },
};

function createDriver() {
  return new TestClickHouseDriver({ host: 'localhost', port: '8123', dataSource: 'default' });
}

describe('ClickHouseDriver S3 export bucket credentials', () => {
  const saved: Record<string, string | undefined> = {};

  beforeEach(() => {
    ENV_KEYS.forEach((k) => {
      saved[k] = process.env[k];
      delete process.env[k];
    });
    process.env.CUBEJS_DB_EXPORT_BUCKET_TYPE = 's3';
    process.env.CUBEJS_DB_EXPORT_BUCKET = 'my-bucket';
    process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_REGION = 'us-west-2';
  });

  afterEach(() => {
    ENV_KEYS.forEach((k) => {
      if (saved[k] === undefined) {
        delete process.env[k];
      } else {
        process.env[k] = saved[k];
      }
    });
  });

  it('keeps the generated SQL and read-side credentials unchanged for static keys', async () => {
    process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_KEY = 'AKIASTATIC';
    process.env.CUBEJS_DB_EXPORT_BUCKET_AWS_SECRET = 'static/secret+key';

    const driver = createDriver();
    await driver.unload('t', unloadOptions);

    expect(driver.commands).toEqual([`
      INSERT INTO FUNCTION
         s3(
             'https://my-bucket.s3.us-west-2.amazonaws.com/export-uuid/export.csv.gz',
             'AKIASTATIC',
             'static/secret+key',
             'CSV'
          )
      SELECT id FROM t WHERE id > 1
    `]);
    expect(driver.credentialRequests).toEqual([]);
    expect(driver.s3Options).toEqual([{
      credentials: { accessKeyId: 'AKIASTATIC', secretAccessKey: 'static/secret+key' },
      region: 'us-west-2',
    }]);
  });

  it('passes temporary credentials from the default chain to s3() when no static keys are set', async () => {
    process.env.AWS_ACCESS_KEY_ID = 'ASIATEMP';
    process.env.AWS_SECRET_ACCESS_KEY = 'temp-secret';
    process.env.AWS_SESSION_TOKEN = 'temp-token';

    const driver = createDriver();
    await driver.unload('t', unloadOptions);

    expect(driver.commands).toHaveLength(1);
    const sql = driver.commands[0];
    expect(sql).toContain(
      'amazonaws.com/export-uuid/export.csv.gz\',\n' +
      '             \'ASIATEMP\',\n' +
      '             \'temp-secret\',\n' +
      '             \'temp-token\',\n' +
      '             \'CSV\''
    );
    expect(sql).toContain('WHERE id > 1');
    expect(sql).not.toContain('undefined');
  });

  it('omits the session token argument when the resolved credentials have none', async () => {
    process.env.AWS_ACCESS_KEY_ID = 'AKIALONGLIVED';
    process.env.AWS_SECRET_ACCESS_KEY = 'long-lived-secret';

    const driver = createDriver();
    await driver.unload('t', unloadOptions);

    expect(driver.commands[0]).toContain(
      '\'AKIALONGLIVED\',\n             \'long-lived-secret\',\n             \'CSV\''
    );
    expect(driver.commands[0]).not.toContain('undefined');
  });

  it('escapes resolved credentials as SQL string literals', async () => {
    process.env.AWS_ACCESS_KEY_ID = 'ASIATEMP';
    process.env.AWS_SECRET_ACCESS_KEY = 'it\'s\\secret';
    process.env.AWS_SESSION_TOKEN = 'temp-token';

    const driver = createDriver();
    await driver.unload('t', unloadOptions);

    expect(driver.commands[0]).toContain('\'it\'\'s\\\\secret\'');
  });

  it('resolves credentials in AWS_REGION, falling back to the export bucket region', async () => {
    process.env.AWS_ACCESS_KEY_ID = 'ASIATEMP';
    process.env.AWS_SECRET_ACCESS_KEY = 'temp-secret';

    const driver = createDriver();
    await driver.unload('t', unloadOptions);
    process.env.AWS_REGION = 'eu-central-1';
    await driver.unload('t', unloadOptions);

    expect(driver.credentialRequests).toEqual([{ region: 'us-west-2' }, { region: 'eu-central-1' }]);
  });

  it('passes no static credentials to the read side when no static keys are set', async () => {
    process.env.AWS_ACCESS_KEY_ID = 'ASIATEMP';
    process.env.AWS_SECRET_ACCESS_KEY = 'temp-secret';

    const driver = createDriver();
    await driver.unload('t', unloadOptions);

    expect(driver.s3Options).toEqual([{ region: 'us-west-2' }]);
  });

  it.each([
    ['CUBEJS_DB_EXPORT_BUCKET_AWS_KEY', 'AKIAONLY'],
    ['CUBEJS_DB_EXPORT_BUCKET_AWS_SECRET', 'secret-only'],
  ])('fails the export, not driver construction, with only %s set', async (key, value) => {
    process.env[key] = value;

    const driver = createDriver();
    await expect(driver.unload('t', unloadOptions)).rejects.toThrow('keyId and secretKey must be set together');
    expect(driver.commands).toEqual([]);
  });
});
