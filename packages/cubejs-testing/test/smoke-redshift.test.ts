import cubejs, { CubeApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, jest } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_API_TOKEN,
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
  testQueryMeasure,
} from './smoke-tests';

// Suite that can be re-used with different ENV configs
const reusableSuite = (env: { [key: string]: string} = {}) => () => {
  jest.setTimeout(60 * 5 * 1000);
  let birdbox: BirdBox;
  let client: CubeApi;

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
    client = cubejs(async () => DEFAULT_API_TOKEN, {
      apiUrl: birdbox.configuration.apiUrl,
    });
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await birdbox.stop();
    Object.keys(env).forEach((key) => {
      delete process.env[key];
    });
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

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
  CUBEJS_DB_EXPORT_BUCKET_REDSHIFT_ARN: 'bogus_arn'
}));
