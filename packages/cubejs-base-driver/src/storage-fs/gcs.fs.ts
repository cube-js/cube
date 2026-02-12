import { Storage } from '@google-cloud/storage';

export type GoogleStorageClientConfig = {
  credentials: any,
};

/**
 * Returns an array of signed GCS URLs of the unloaded csv files.
 */
export async function extractFilesFromGCS(
  gcsConfig: GoogleStorageClientConfig,
  bucketName: string,
  tableName: string
): Promise<string[]> {
  const storage = new Storage(
    gcsConfig.credentials
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
