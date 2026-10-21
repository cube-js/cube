import fs from 'fs';
import os from 'os';
import path from 'path';
import { BigQuery } from '@google-cloud/bigquery';
import { IdentityPoolClient } from 'google-auth-library';

import { BigQueryDriver } from '../src';
import { workloadIdentityAudience } from '../src/AccessTokenUpdate';

const PROVIDER = '//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/pool/providers/cube';

const jwt = (claims: Record<string, unknown>) => [
  'e30',
  Buffer.from(JSON.stringify(claims)).toString('base64url'),
  'signature',
].join('.');

class TestBigQueryDriver extends BigQueryDriver {
  public client(): BigQuery {
    return this.bigquery;
  }
}

describe('BigQueryDriver.updateAccessToken', () => {
  // CI's integration env sets a project and credentials; getEnv reads this same process.env object
  const saved = Object.entries(process.env)
    .filter(([key]) => key.startsWith('CUBEJS_DB_BQ_') || ['GOOGLE_CLOUD_PROJECT', 'GCLOUD_PROJECT'].includes(key));
  beforeEach(() => saved.forEach(([key]) => delete process.env[key]));
  afterEach(() => saved.forEach(([key, value]) => {
    process.env[key] = value;
  }));

  test('is advertised on the class', () => {
    expect(BigQueryDriver.supportsAccessTokenUpdate()).toBe(true);
  });

  test('switches to a Workload Identity client for the token\'s audience, reused while it stays', () => {
    const driver = new TestBigQueryDriver({ projectId: 'project' });
    const configured = driver.client();

    driver.updateAccessToken(jwt({ aud: `https:${PROVIDER}`, sub: 'alice' }));
    const updated = driver.client();

    expect(updated).not.toBe(configured);
    expect((<any>updated).authClient.cachedCredential).toBeInstanceOf(IdentityPoolClient);
    expect((<any>updated).authClient.cachedCredential.audience).toBe(PROVIDER);

    driver.updateAccessToken(jwt({ aud: PROVIDER, sub: 'alice', iat: 2 }));
    expect(driver.client()).toBe(updated);
  });

  test('sends Google STS the newest token, without a new client', async () => {
    const driver = new TestBigQueryDriver({ projectId: 'project' });
    const first = jwt({ aud: PROVIDER, iat: 1 });
    const second = jwt({ aud: PROVIDER, iat: 2 });

    driver.updateAccessToken(first);
    const authClient = <IdentityPoolClient>(<any>driver.client()).authClient.cachedCredential;
    driver.updateAccessToken(second);

    await expect(authClient.retrieveSubjectToken()).resolves.toBe(second);
  });

  test('keeps the project of the configured credentials when no project ID is set', () => {
    const driver = new TestBigQueryDriver(<any>{ credentials: { project_id: 'from-credentials' } });

    driver.updateAccessToken(jwt({ aud: PROVIDER }));

    expect(driver.client().projectId).toBe('from-credentials');
  });

  test('reads the project from a key file, and keeps the driver\'s scopes', () => {
    const keyFile = path.join(os.tmpdir(), `bq-key-${process.pid}.json`);
    fs.writeFileSync(keyFile, JSON.stringify({ project_id: 'from-key-file' }));

    try {
      const driver = new TestBigQueryDriver(<any>{ keyFilename: keyFile });
      driver.updateAccessToken(jwt({ aud: PROVIDER }));
      const authClient = <IdentityPoolClient>(<any>driver.client()).authClient.cachedCredential;

      expect(driver.client().projectId).toBe('from-key-file');
      expect((<any>authClient).scopes).toEqual(expect.arrayContaining(['https://www.googleapis.com/auth/drive']));
    } finally {
      fs.unlinkSync(keyFile);
    }
  });

  test('refuses a token without a project to bill, before any user query reaches Resource Manager', () => {
    const driver = new TestBigQueryDriver(<any>{});
    const configured = driver.client();

    expect(() => driver.updateAccessToken(jwt({ aud: PROVIDER }))).toThrow(/project ID/);
    expect(driver.client()).toBe(configured);
  });

  test('refuses a token that names no Workload Identity provider', () => {
    expect(() => workloadIdentityAudience(jwt({ sub: 'alice' }))).toThrow(/aud claim/);
    expect(() => workloadIdentityAudience('not-a-jwt')).toThrow(/aud claim/);
    expect(() => workloadIdentityAudience(`x.${Buffer.from('null').toString('base64url')}.y`)).toThrow(/aud claim/);
  });
});
