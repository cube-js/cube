import { Storage } from '@google-cloud/storage';

export type GoogleStorageClientConfig = {
  credentials: any,
};

/**
 * Whether the provided GCS credentials are usable. Empty string / undefined
 * (no credentials configured, e.g. OIDC / workload identity) and an empty
 * object are treated as absent so the Google SDK falls back to Application
 * Default Credentials (which honors `GOOGLE_APPLICATION_CREDENTIALS`, including
 * workload-identity-federation `external_account` config files).
 */
export function hasGCSCredentials(credentials: any): boolean {
  if (!credentials) {
    return false;
  }
  if (typeof credentials === 'object' && Object.keys(credentials).length === 0) {
    return false;
  }
  return true;
}

/**
 * Returns an array of signed GCS URLs of the unloaded csv files.
 */
export async function extractFilesFromGCS(
  gcsConfig: GoogleStorageClientConfig,
  bucketName: string,
  tableName: string
): Promise<string[]> {
  const storage = new Storage(
    hasGCSCredentials(gcsConfig.credentials)
      ? { credentials: gcsConfig.credentials, projectId: gcsConfig.credentials.project_id }
      : undefined
  );

  const bucket = storage.bucket(bucketName);
  const [files] = await bucket.getFiles({ prefix: `${tableName}/` });

  if (files.length) {
    const csvFiles = await Promise.all(files.map(async (file) => {
      const [url] = await file.getSignedUrl({
        action: 'read',
        expires: new Date(new Date().getTime() + 60 * 60 * 1000)
      });
      return url;
    }));

    return csvFiles;
  } else {
    throw new Error('No CSV files were obtained from the bucket');
  }
}
