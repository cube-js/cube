import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, jest } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_CONFIG, testQueryMeasure } from './smoke-tests';

// Suite that can be re-used with different ENV configs
const reusableSuite = (env: { [key: string]: string} = {}) => () => {
  jest.setTimeout(60 * 5 * 1000);
  let birdbox: BirdBox;
  let client: CubejsApi;

  beforeAll(async () => {
    Object.keys(env).forEach((key) => {
      process.env[key] = env[key];
    });

    birdbox = await getBirdbox(
      'redshift',
      {
        CUBEJS_DB_TYPE: 'redshift',

        ...DEFAULT_CONFIG,
      },
      {
        schemaDir: 'postgresql/schema',
      }
    );
    client = cubejs(async () => 'test', {
      apiUrl: birdbox.configuration.apiUrl,
    });
  });

  afterAll(async () => {
    await birdbox.stop();
    Object.keys(env).forEach((key) => {
      delete process.env[key];
    });
  });

  test('query measure', () => testQueryMeasure(client));
};

// Base environment vars that are required if S3 export is to be enabled.
const baseS3Env: { [ key: string ]: string } = {
  CUBEJS_DB_EXPORT_BUCKET_TYPE: 's3',
  CUBEJS_DB_EXPORT_BUCKET: 'foo-bucket',
  CUBEJS_DB_EXPORT_BUCKET_AWS_REGION: 'bar-region'
};
// No export/unload configured
describe('redshift', reusableSuite());
// Use a key_id/secret combo for unload and export
describe('redshift (S3 key_id/secret)', reusableSuite({
  ...baseS3Env,
  CUBEJS_DB_EXPORT_BUCKET_AWS_KEY: 'someKey',
  CUBEJS_DB_EXPORT_BUCKET_AWS_SECRET: 'itsASecret'
}));
// Use an iam_role arn for unload and export
describe('redshift (unload arn)', reusableSuite({
  ...baseS3Env,
  CUBEJS_DB_REDSHIFT_UNLOAD_ARN: 'bogus_arn'
}));
