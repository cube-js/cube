import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { S3, GetObjectCommand, S3ClientConfig } from '@aws-sdk/client-s3';

export type S3StorageClientConfig = S3ClientConfig;

/**
 * Returns a copy of the S3 client config with unusable static credentials and
 * a blank region removed, so the AWS SDK falls back to its default resolution.
 *
 * When an export bucket is configured for OIDC / workload identity there are no
 * static keys, yet some drivers still pass a `{ accessKeyId: '', secretAccessKey: '' }`
 * object. The SDK would send those empty strings verbatim and the request fails
 * with `AuthorizationHeaderMalformed: ... a non-empty Access Key (AKID) must be
 * provided`. Dropping the empty credentials lets the SDK use its default provider
 * chain (env vars, web identity token file `AWS_WEB_IDENTITY_TOKEN_FILE`, IRSA,
 * etc.). Likewise a blank region would short-circuit region resolution from
 * `AWS_REGION` / `AWS_DEFAULT_REGION`.
 *
 * Credential *providers* (functions, e.g. `fromTemporaryCredentials`) and
 * fully-populated static credentials are left untouched.
 */
export function normalizeS3ClientConfig(clientOptions: S3StorageClientConfig): S3StorageClientConfig {
  const config: S3StorageClientConfig = { ...clientOptions };
  const { credentials } = config;

  if (
    credentials &&
    typeof credentials === 'object' &&
    'accessKeyId' in credentials &&
    (!credentials.accessKeyId || !credentials.secretAccessKey)
  ) {
    delete config.credentials;
  }

  if (typeof config.region === 'string' && config.region.trim() === '') {
    delete config.region;
  }

  return config;
}

/**
 * Returns an array of signed AWS S3 URLs of the unloaded csv files.
 */
export async function extractUnloadedFilesFromS3(
  clientOptions: S3StorageClientConfig,
  bucketName: string,
  prefix: string
): Promise<string[]> {
  const storage = new S3(normalizeS3ClientConfig(clientOptions));
  // It looks that different driver configurations use different formats
  // for the bucket - some expect only names, some - full url-like names.
  // So we unify this.
  bucketName = bucketName.replace(/^[a-zA-Z]+:\/\//, '');

  const list = await storage.listObjectsV2({
    Bucket: bucketName,
    Prefix: prefix,
  });
  if (list) {
    if (!list.Contents) {
      return [];
    } else {
      const csvFiles = await Promise.all(
        list.Contents.map(async (file) => {
          const command = new GetObjectCommand({
            Bucket: bucketName,
            Key: file.Key,
          });
          return getSignedUrl(storage, command, { expiresIn: 3600 });
        })
      );
      return csvFiles;
    }
  }

  throw new Error('Unable to retrieve list of files from S3 storage after unloading.');
}
