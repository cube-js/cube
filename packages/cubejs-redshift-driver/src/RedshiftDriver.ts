import { PostgresDriver, PostgresDriverConfiguration } from '@cubejs-backend/postgres-driver';
import { DownloadTableCSVData, UnloadOptions } from '@cubejs-backend/query-orchestrator';
import { getEnv } from '@cubejs-backend/shared';
import crypto from 'crypto';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { S3, GetObjectCommand } from '@aws-sdk/client-s3';

interface RedshiftDriverExportAWS {
  bucketType: 's3',
  bucketName: string,
  keyId: string,
  secretKey: string,
  region: string,
}

export interface RedshiftDriverConfiguration extends PostgresDriverConfiguration {
  exportBucket?: RedshiftDriverExportAWS;
}

export class RedshiftDriver extends PostgresDriver<RedshiftDriverConfiguration> {
  public constructor(options: RedshiftDriverConfiguration = {}) {
    super(options);
  }

  protected getInitialConfiguration(): Partial<RedshiftDriverConfiguration> {
    return {
      exportBucket: this.getExportBucket(),
    };
  }

  protected getExportBucket(): RedshiftDriverExportAWS|undefined {
    const exportBucket: Partial<RedshiftDriverExportAWS> = {
      bucketType: getEnv('dbExportBucketType', {
        supported: ['s3']
      }),
      bucketName: getEnv('dbExportBucket'),
      keyId: getEnv('dbExportBucketAwsKey'),
      secretKey: getEnv('dbExportBucketAwsSecret'),
      region: getEnv('dbExportBucketAwsRegion'),
    };

    if (exportBucket.bucketType) {
      const supportedBucketTypes = ['s3'];

      if (!supportedBucketTypes.includes(exportBucket.bucketType)) {
        throw new Error(
          `Unsupported EXPORT_BUCKET_TYPE, supported: ${supportedBucketTypes.join(',')}`
        );
      }

      const emptyKeys = Object.keys(exportBucket)
        .filter((key: string) => exportBucket[<keyof RedshiftDriverExportAWS>key] === undefined);
      if (emptyKeys.length) {
        throw new Error(
          `Unsupported configuration exportBucket, some configuration keys are empty: ${emptyKeys.join(',')}`
        );
      }

      return <RedshiftDriverExportAWS>exportBucket;
    }

    return undefined;
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

    const { bucketType, bucketName, keyId, secretKey, region } = this.config.exportBucket;

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
        executionTimeout: 600000,
      });

      let unloadTotalRows: number|null = null;

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

      // Unable to extract number of extracted rows, because it's done in protocol notice
      await conn.query({
        text: (
          `UNLOAD ('SELECT * FROM ${table}') TO '${bucketType}://${bucketName}/${exportPathName}/' ` +
          `CREDENTIALS 'aws_access_key_id=${keyId};aws_secret_access_key=${secretKey}' ${optionsPart}`
        ),
      });
      if (unloadTotalRows === 0) {
        return {
          csvFile: [],
        };
      }

      const client = new S3({
        credentials: {
          accessKeyId: keyId,
          secretAccessKey: secretKey,
        },
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
