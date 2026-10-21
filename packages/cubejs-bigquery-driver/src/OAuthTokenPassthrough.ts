import { readFileSync } from 'fs';
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
    aud = JSON.parse(Buffer.from(payload ?? '', 'base64url').toString('utf8'))?.aud;
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
  scopes: string | string[] = [CLOUD_PLATFORM_SCOPE],
): IdentityPoolClient {
  return new IdentityPoolClient({
    type: 'external_account',
    audience,
    subject_token_type: JWT_TOKEN_TYPE,
    token_url: STS_TOKEN_URL,
    scopes: typeof scopes === 'string' ? [scopes] : scopes,
    subject_token_supplier: {
      getSubjectToken: async () => subjectToken(),
    },
  });
}

type ProjectSource = { projectId?: string, credentials?: unknown, keyFilename?: string };

/** The configured project: options, inline credentials, the key file, then Google's project env. */
export function configuredProjectId({ projectId, credentials, keyFilename }: ProjectSource): string | undefined {
  if (projectId) {
    return projectId;
  }

  const inline = (credentials as { project_id?: unknown } | undefined)?.project_id;
  if (typeof inline === 'string') {
    return inline;
  }

  if (keyFilename) {
    try {
      const fromFile = JSON.parse(readFileSync(keyFilename, 'utf8')).project_id;
      if (typeof fromFile === 'string') {
        return fromFile;
      }
    } catch {
      // An unreadable key file fails the configured client too
    }
  }

  return process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT || undefined;
}
