import type { S3Credentials, TableStructure, UnloadOptions } from '@cubejs-backend/base-driver';

import { ClickHouseDriver, exportBucketSessionPolicy } from '../../src/ClickHouseDriver';

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

  public webIdentityRequests: { policy: string, region: string }[] = [];

  public chainCredentials: S3Credentials | null = null;

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
    return this.chainCredentials ?? super.resolveS3Credentials(options);
  }

  public realWebIdentityCredentials(policy: string, region: string) {
    return super.resolveWebIdentityCredentials(policy, region);
  }

  public setCachedCredentials(credentials: S3Credentials) {
    this.exportCredentials = Promise.resolve(credentials);
  }

  public override async resolveWebIdentityCredentials(policy: string, region: string): Promise<S3Credentials> {
    this.webIdentityRequests.push({ policy, region });
    return { accessKeyId: 'ASIAWEB', secretAccessKey: 'web-secret', sessionToken: 'web-token' };
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
  'AWS_WEB_IDENTITY_TOKEN_FILE',
  'AWS_ROLE_ARN',
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

    const bucketRegionDriver = createDriver();
    await bucketRegionDriver.unload('t', unloadOptions);
    process.env.AWS_REGION = 'eu-central-1';
    const awsRegionDriver = createDriver();
    await awsRegionDriver.unload('t', unloadOptions);

    expect(bucketRegionDriver.credentialRequests).toEqual([{ region: 'us-west-2' }]);
    expect(awsRegionDriver.credentialRequests).toEqual([{ region: 'eu-central-1' }]);
  });

  it('reuses temporary credentials only while an export could still run for 50 minutes', async () => {
    const driver = createDriver();
    driver.chainCredentials = {
      accessKeyId: 'ASIATEMP',
      secretAccessKey: 'temp-secret',
      sessionToken: 'temp-token',
      expiration: new Date(Date.now() + 55 * 60 * 1000),
    };
    await Promise.all([driver.unload('t', unloadOptions), driver.unload('t', unloadOptions)]);
    expect(driver.credentialRequests).toHaveLength(1);

    driver.setCachedCredentials({ ...driver.chainCredentials, expiration: new Date(Date.now() + 45 * 60 * 1000) });
    await driver.unload('t', unloadOptions);
    expect(driver.credentialRequests).toHaveLength(2);
  });

  it('scopes web identity credentials to the export bucket path with a session policy', async () => {
    process.env.AWS_WEB_IDENTITY_TOKEN_FILE = '/var/run/secrets/token';
    process.env.AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/cube';
    process.env.CUBEJS_DB_EXPORT_BUCKET = 'my-bucket/exports';

    const driver = createDriver();
    await driver.unload('t', unloadOptions);

    expect(driver.webIdentityRequests).toEqual([
      { policy: exportBucketSessionPolicy('my-bucket', 'exports'), region: 'us-west-2' },
    ]);
    expect(driver.credentialRequests).toEqual([]);
    expect(driver.commands[0]).toContain('\'ASIAWEB\',\n             \'web-secret\',\n             \'web-token\'');
  });

  it('loads the SDK web identity provider, which reads the token file', async () => {
    process.env.AWS_WEB_IDENTITY_TOKEN_FILE = '/nonexistent/cube-test-token';
    process.env.AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/cube';

    await expect(createDriver().realWebIdentityCredentials('{}', 'us-west-2')).rejects.toThrow('/nonexistent/cube-test-token');
  });

  it('puts the role partition into the session policy', async () => {
    process.env.AWS_WEB_IDENTITY_TOKEN_FILE = '/var/run/secrets/token';
    process.env.AWS_ROLE_ARN = 'arn:aws-us-gov:iam::123456789012:role/cube';

    const driver = createDriver();
    await driver.unload('t', unloadOptions);

    expect(driver.webIdentityRequests[0].policy).toBe(exportBucketSessionPolicy('my-bucket', '', 'aws-us-gov'));
    expect(driver.webIdentityRequests[0].policy).toContain('arn:aws-us-gov:s3:::my-bucket/*');
  });

  it.each([
    ['static keys', { CUBEJS_DB_EXPORT_BUCKET_AWS_KEY: 'AKIASTATIC', CUBEJS_DB_EXPORT_BUCKET_AWS_SECRET: 'static-secret' }],
    ['the default chain', { AWS_ACCESS_KEY_ID: 'ASIATEMP', AWS_SECRET_ACCESS_KEY: 'temp-secret', AWS_SESSION_TOKEN: 'temp-token' }],
  ])('keeps a ClickHouse ternary in parameterless SQL intact with %s', async (_label, env) => {
    Object.assign(process.env, env);
    const sql = 'SELECT x > 0 ? 1 : 0 AS flag FROM t';

    const driver = createDriver();
    await driver.unload('t', { maxFileSize: 64, query: { sql, params: [] } });
    await driver.unload('t', { maxFileSize: 64, query: { sql, params: undefined as unknown as unknown[] } });

    expect(driver.commands).toHaveLength(2);
    driver.commands.forEach((command) => expect(command).toContain(`'CSV'\n          )\n      ${sql}\n    `));
  });

  it('still scopes web identity when only AWS_ACCESS_KEY_ID is set, as the SDK skips half-set env keys', async () => {
    process.env.AWS_WEB_IDENTITY_TOKEN_FILE = '/var/run/secrets/token';
    process.env.AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/cube';
    process.env.AWS_ACCESS_KEY_ID = 'AKIALEFTOVER';

    const driver = createDriver();
    await driver.unload('t', unloadOptions);

    expect(driver.webIdentityRequests).toHaveLength(1);
    expect(driver.credentialRequests).toEqual([]);
  });

  it('uses the default chain when static env keys would win over web identity', async () => {
    process.env.AWS_WEB_IDENTITY_TOKEN_FILE = '/var/run/secrets/token';
    process.env.AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/cube';
    process.env.AWS_ACCESS_KEY_ID = 'AKIAENV';
    process.env.AWS_SECRET_ACCESS_KEY = 'env-secret';

    const driver = createDriver();
    await driver.unload('t', unloadOptions);

    expect(driver.webIdentityRequests).toEqual([]);
    expect(driver.commands[0]).toContain('\'AKIAENV\'');
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

describe('exportBucketSessionPolicy', () => {
  it('limits object access to the export path and listing to the bucket', () => {
    expect(JSON.parse(exportBucketSessionPolicy('my-bucket', 'exports/cube'))).toEqual({
      Version: '2012-10-17',
      Statement: [
        {
          Effect: 'Allow',
          Action: ['s3:PutObject', 's3:GetObject', 's3:AbortMultipartUpload', 's3:ListMultipartUploadParts'],
          Resource: 'arn:aws:s3:::my-bucket/exports/cube/*',
        },
        {
          Effect: 'Allow',
          Action: ['s3:ListBucket', 's3:GetBucketLocation'],
          Resource: 'arn:aws:s3:::my-bucket',
        },
        {
          Effect: 'Allow',
          Action: ['kms:GenerateDataKey', 'kms:Decrypt'],
          Resource: '*',
          Condition: {
            StringLike: {
              'kms:ViaService': 's3.*.amazonaws.com*',
              'kms:EncryptionContext:aws:s3:arn': [
                'arn:aws:s3:::my-bucket',
                'arn:aws:s3:::my-bucket/exports/cube/*',
              ],
            },
          },
        },
      ],
    });
  });

  it('covers the whole bucket when no path is configured', () => {
    expect(JSON.parse(exportBucketSessionPolicy('my-bucket', '')).Statement[0].Resource).toBe('arn:aws:s3:::my-bucket/*');
  });
});
