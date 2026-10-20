import { BigQuery } from '@google-cloud/bigquery';
import { IdentityPoolClient } from 'google-auth-library';

import { BigQueryDriver } from '../src';
import { workloadIdentityAudience } from '../src/OAuthTokenPassthrough';

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

describe('BigQueryDriver.setOAuthToken', () => {
  test('switches to a Workload Identity client for the token\'s audience, reused while it stays', () => {
    const driver = new TestBigQueryDriver({ projectId: 'project' });
    const configured = driver.client();

    driver.setOAuthToken(jwt({ aud: `https:${PROVIDER}`, sub: 'alice' }));
    const passthrough = driver.client();

    expect(passthrough).not.toBe(configured);
    expect((<any>passthrough).authClient.cachedCredential).toBeInstanceOf(IdentityPoolClient);
    expect((<any>passthrough).authClient.cachedCredential.audience).toBe(PROVIDER);

    driver.setOAuthToken(jwt({ aud: PROVIDER, sub: 'alice', iat: 2 }));
    expect(driver.client()).toBe(passthrough);
  });

  test('sends Google STS the newest token, without a new client', async () => {
    const driver = new TestBigQueryDriver({ projectId: 'project' });
    const first = jwt({ aud: PROVIDER, iat: 1 });
    const second = jwt({ aud: PROVIDER, iat: 2 });

    driver.setOAuthToken(first);
    const authClient = <IdentityPoolClient>(<any>driver.client()).authClient.cachedCredential;
    driver.setOAuthToken(second);

    await expect(authClient.retrieveSubjectToken()).resolves.toBe(second);
  });

  test('keeps the project of the configured credentials when no project ID is set', () => {
    const driver = new TestBigQueryDriver(<any>{ credentials: { project_id: 'from-credentials' } });

    driver.setOAuthToken(jwt({ aud: PROVIDER }));

    expect(driver.client().projectId).toBe('from-credentials');
  });

  test('refuses a token that names no Workload Identity provider', () => {
    expect(() => workloadIdentityAudience(jwt({ sub: 'alice' }))).toThrow(/aud claim/);
    expect(() => workloadIdentityAudience('not-a-jwt')).toThrow(/aud claim/);
  });
});
