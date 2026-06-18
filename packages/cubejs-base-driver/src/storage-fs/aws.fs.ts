import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { S3, GetObjectCommand, S3ClientConfig } from '@aws-sdk/client-s3';

export type S3StorageClientConfig = S3ClientConfig;

/**
 * Returns an array of signed AWS S3 URLs of the unloaded csv files.
 */
export async function extractUnloadedFilesFromS3(
  clientOptions: S3StorageClientConfig,
  bucketName: string,
  prefix: string
): Promise<string[]> {
  const storage = new S3(clientOptions);
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
