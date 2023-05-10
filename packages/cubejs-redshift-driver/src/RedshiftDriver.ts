import { PostgresDriver, PostgresDriverConfiguration } from '@cubejs-backend/postgres-driver';
import { DownloadTableCSVData, UnloadOptions } from '@cubejs-backend/base-driver';
import { getEnv } from '@cubejs-backend/shared';
import crypto from 'crypto';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { S3, GetObjectCommand } from '@aws-sdk/client-s3';

interface RedshiftDriverExportRequiredAWS {
  bucketType: 's3',
  bucketName: string,
  region: string,
}

interface RedshiftDriverExportArnAWS extends RedshiftDriverExportRequiredAWS{
  // ARN used to access S3 unload data from e.g. EC2 instances, instead of explicit key/secret credentials.
  // See https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2.html
  // Resources needing to read these files will need proper read permissions on their role as well.
  unloadArn?: string,
}

interface RedshiftDriverExportKeySecretAWS extends RedshiftDriverExportRequiredAWS{
  keyId?: string,
  secretKey?: string,
}

interface RedshiftDriverExportAWS extends RedshiftDriverExportArnAWS, RedshiftDriverExportKeySecretAWS {
}

export interface RedshiftDriverConfiguration extends PostgresDriverConfiguration {
  exportBucket?: RedshiftDriverExportAWS;
}

export class RedshiftDriver extends PostgresDriver<RedshiftDriverConfiguration> {
  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 4;
  }

  public constructor(options: RedshiftDriverConfiguration = {}) {
    super(options);
  }

  protected getInitialConfiguration(): Partial<RedshiftDriverConfiguration> {
    return {
      // @todo It's not possible to support UNLOAD in readOnly mode, because we need column types (CREATE TABLE?)
      readOnly: false,
      exportBucket: this.getExportBucket(),
    };
  }

  protected getExportBucket(): RedshiftDriverExportAWS | undefined {
    const supportedBucketTypes = ['s3'];

    const requiredExportBucket: Partial<RedshiftDriverExportRequiredAWS> = {
      bucketType: getEnv('dbExportBucketType', {
        supported: supportedBucketTypes
      }),
      bucketName: getEnv('dbExportBucket'),
      region: getEnv('dbExportBucketAwsRegion'),
    };

    const exportBucket: Partial<RedshiftDriverExportAWS> = {
      ...requiredExportBucket,
      keyId: getEnv('dbExportBucketAwsKey'),
      secretKey: getEnv('dbExportBucketAwsSecret'),
      unloadArn: getEnv('dbExportBucketRedshiftArn')
    };

    if (exportBucket.bucketType) {
      if (!supportedBucketTypes.includes(exportBucket.bucketType)) {
        throw new Error(
          `Unsupported EXPORT_BUCKET_TYPE, supported: ${supportedBucketTypes.join(',')}`
        );
      }

      // Make sure the required keys are set
      const emptyRequiredKeys = Object.keys(requiredExportBucket)
        .filter((key: string) => requiredExportBucket[<keyof RedshiftDriverExportRequiredAWS>key] === undefined);
      if (emptyRequiredKeys.length) {
        throw new Error(
          `Unsupported configuration exportBucket, some configuration keys are empty: ${emptyRequiredKeys.join(',')}`
        );
      }
      // If unload ARN is not set, secret and key id must be set for Redshift
      if (!exportBucket.unloadArn) {
        // Make sure the required keys are set
        const emptySecretKeys = Object.keys(exportBucket)
          .filter((key: string) => key !== 'unloadArn')
          .filter((key: string) => exportBucket[<keyof RedshiftDriverExportAWS>key] === undefined);
        if (emptySecretKeys.length) {
          throw new Error(
            `Unsupported configuration exportBucket, some configuration keys are empty: ${emptySecretKeys.join(',')}`
          );
        }
      }

      return <RedshiftDriverExportAWS>exportBucket;
    }

    return undefined;
  }

  public async loadUserDefinedTypes(): Promise<void> {
    // @todo Implement for Redshift, column \"typcategory\" does not exist in pg_type
  }

  public async isUnloadSupported() {
    if (this.config.exportBucket) {
      return true;
    }

    return false;
  }

  public async unload(table: string, options: UnloadOptions): Promise<DownloadTableCSVData> {
    if (!this.config.exportBucket) {
      throw new Error('Unload is not configured');
    }

    const { bucketType, bucketName, region, unloadArn, keyId, secretKey } = this.config.exportBucket;

    const conn = await this.pool.connect();

    try {
      const exportPathName = crypto.randomBytes(10).toString('hex');

      const optionsToExport = {
        REGION: `'${region}'`,
        HEADER: '',
        FORMAT: 'CSV',
        GZIP: '',
        MAXFILESIZE: `${options.maxFileSize}MB`
      };
      const optionsPart = Object.entries(optionsToExport)
        .map(([key, value]) => `${key} ${value}`)
        .join(' ');

      await this.prepareConnection(conn, {
        executionTimeout: this.config.executionTimeout ? this.config.executionTimeout * 1000 : 600000,
      });

      let unloadTotalRows: number | null = null;

      /**
       * @link https://github.com/brianc/node-postgres/blob/pg%408.6.0/packages/pg-protocol/src/messages.ts#L211
       * @link https://github.com/brianc/node-postgres/blob/pg%408.6.0/packages/pg-protocol/src/parser.ts#L357
       *
       * message: 'UNLOAD completed, 0 record(s) unloaded successfully.',
       */
      conn.addListener('notice', (e: any) => {
        if (e.message && e.message.startsWith('UNLOAD completed')) {
          const matches = e.message.match(/\d+/);
          if (matches) {
            unloadTotalRows = parseInt(matches[0], 10);
          } else {
            throw new Error('Unable to detect number of unloaded records');
          }
        }
      });

      const baseQuery = `UNLOAD ('SELECT * FROM ${table}') TO '${bucketType}://${bucketName}/${exportPathName}/'`;
      // Prefer the unloadArn if it is present
      const credentialQuery = unloadArn
        ? `iam_role '${unloadArn}'`
        : `CREDENTIALS 'aws_access_key_id=${keyId};aws_secret_access_key=${secretKey}'`;

      const unloadQuery = `${baseQuery} ${credentialQuery} ${optionsPart}`;

      // Unable to extract number of extracted rows, because it's done in protocol notice
      await conn.query({
        text: unloadQuery,
      });

      if (unloadTotalRows === 0) {
        return {
          csvFile: [],
        };
      }

      const client = new S3({
        credentials: (keyId && secretKey) ? {
          accessKeyId: keyId,
          secretAccessKey: secretKey,
        } : undefined,
        region,
      });
      const list = await client.listObjectsV2({
        Bucket: bucketName,
        Prefix: exportPathName,
      });
      if (list && list.Contents) {
        const csvFile = await Promise.all(
          list.Contents.map(async (file) => {
            const command = new GetObjectCommand({
              Bucket: bucketName,
              Key: file.Key,
            });
            return getSignedUrl(client, command, { expiresIn: 3600 });
          })
        );

        return {
          csvFile,
        };
      }

      throw new Error('Unable to UNLOAD table, there are no files in S3 storage');
    } finally {
      conn.removeAllListeners('notice');

      await conn.release();
    }
  }
}
