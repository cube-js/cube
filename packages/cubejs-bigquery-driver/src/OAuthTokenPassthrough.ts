import { IdentityPoolClient } from 'google-auth-library';

const JWT_TOKEN_TYPE = 'urn:ietf:params:oauth:token-type:jwt';
const STS_TOKEN_URL = 'https://sts.googleapis.com/v1/token';
const CLOUD_PLATFORM_SCOPE = 'https://www.googleapis.com/auth/cloud-platform';

/**
 * The Workload Identity provider a token is for: its `aud` claim. STS wants the
 * provider resource name, which an `aud` may carry with an `https:` prefix.
 */
export function workloadIdentityAudience(token: string): string {
  const payload = token.split('.')[1];
  let aud: unknown;

  try {
    ({ aud } = JSON.parse(Buffer.from(payload ?? '', 'base64url').toString('utf8')));
  } catch (e) {
    if (!(e instanceof SyntaxError)) {
      throw e;
    }
  }

  const audience = Array.isArray(aud) ? aud[0] : aud;
  if (typeof audience !== 'string' || !audience) {
    throw new Error(
      'OAuth token passthrough for BigQuery needs a JWT whose aud claim names the Workload Identity provider'
    );
  }

  return audience.replace(/^https:\/\/iam\.googleapis\.com\//, '//iam.googleapis.com/');
}

/** Exchanges whatever `subjectToken` returns when it refreshes, so a newer token needs no new client. */
export function createWorkloadIdentityClient(
  audience: string,
  subjectToken: () => string,
): IdentityPoolClient {
  return new IdentityPoolClient({
    type: 'external_account',
    audience,
    subject_token_type: JWT_TOKEN_TYPE,
    token_url: STS_TOKEN_URL,
    scopes: [CLOUD_PLATFORM_SCOPE],
    subject_token_supplier: {
      getSubjectToken: async () => subjectToken(),
    },
  });
}
