/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `RedshiftDriver` and related types declaration.
 */

import { getEnv } from '@cubejs-backend/shared';
import {
  PostgresDriver,
  PostgresDriverConfiguration,
} from '@cubejs-backend/postgres-driver';
import {
  DownloadTableCSVData,
  UnloadOptions,
  TableStructure,
  DriverCapabilities,
} from '@cubejs-backend/base-driver';
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

/**
 * Redshift driver class.
 */
export class RedshiftDriver extends PostgresDriver<RedshiftDriverConfiguration> {
  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 4;
  }

  /**
   * Class constructor.
   */
  public constructor(
    options: RedshiftDriverConfiguration & {
      /**
       * Data source name.
       */
      dataSource?: string,

      /**
       * Max pool size value for the [cube]<-->[db] pool.
       */
      maxPoolSize?: number,

      /**
       * Time to wait for a response from a connection after validation
       * request before determining it as not valid. Default - 10000 ms.
       */
      testConnectionTimeout?: number,
    } = {}
  ) {
    super(options);
  }

  /**
   * @override
   */
  protected getInitialConfiguration(
    dataSource: string,
  ): Partial<RedshiftDriverConfiguration> {
    return {
      readOnly: true,
      exportBucket: this.getExportBucket(dataSource),
    };
  }

  /**
   * Returns driver's capabilities object.
   */
  public capabilities(): DriverCapabilities {
    return { unloadWithoutTempTable: true };
  }

  protected getExportBucket(
    dataSource: string,
  ): RedshiftDriverExportAWS | undefined {
    const supportedBucketTypes = ['s3'];

    const requiredExportBucket: Partial<RedshiftDriverExportRequiredAWS> = {
      bucketType: getEnv('dbExportBucketType', {
        supported: supportedBucketTypes,
        dataSource,
      }),
      bucketName: getEnv('dbExportBucket', { dataSource }),
      region: getEnv('dbExportBucketAwsRegion', { dataSource }),
    };

    const exportBucket: Partial<RedshiftDriverExportAWS> = {
      ...requiredExportBucket,
      keyId: getEnv('dbExportBucketAwsKey', { dataSource }),
      secretKey: getEnv('dbExportBucketAwsSecret', { dataSource }),
      unloadArn: getEnv('redshiftUnloadArn', { dataSource }),
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

  /**
   * Determines whether export bucket feature is configured or not.
   */
  public async isUnloadSupported() {
    if (this.config.exportBucket) {
      return true;
    }
    return false;
  }

  /**
   * Returns to the Cubestore an object with links to unloaded to the
   * export bucket data.
   */
  public async unload(
    table: string,
    options: UnloadOptions,
  ): Promise<DownloadTableCSVData> {
    if (!this.config.exportBucket) {
      throw new Error('Unload is not configured');
    }
    const types = options.query
      ? await this.unloadWithSql(table, options)
      : await this.unloadWithTable(table, options);
    const csvFile = await this.getCsvFiles(table);
    return {
      exportBucketCsvEscapeSymbol:
        this.config.exportBucketCsvEscapeSymbol,
      csvFile,
      types
    };
  }

  /**
   * Unload data from a SQL query to an export bucket.
   */
  private async unloadWithSql(
    table: string,
    options: UnloadOptions,
  ): Promise<TableStructure> {
    if (!this.config.exportBucket) {
      throw new Error('Export bucket is not configured.');
    }
    if (!options.query) {
      throw new Error('Unload query is missed.');
    }
    const types = await this.queryColumnTypes(options.query.sql);
    const {
      bucketType,
      bucketName,
      region,
      unloadArn,
      keyId,
      secretKey,
    } = this.config.exportBucket;
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
    const conn = await this.pool.connect();
    try {
      await this.prepareConnection(conn, {
        executionTimeout: this.config.executionTimeout
          ? this.config.executionTimeout * 1000
          : 600000,
      });
      const baseQuery = `
        UNLOAD ('${options.query.sql}')
        TO '${bucketType}://${bucketName}/${table}/'
      `;
      const credentialQuery = unloadArn
        ? `iam_role '${unloadArn}'`
        : 'CREDENTIALS ' +
          `'aws_access_key_id=${keyId};` +
          `aws_secret_access_key=${secretKey}'`;
      const unloadQuery =
        `${baseQuery} ${credentialQuery} ${optionsPart}`;
      await conn.query({
        text: unloadQuery,
      });
    } finally {
      await conn.release();
    }
    return types;
  }

  /**
   * Unload data from a temp table to an export bucket.
   */
  private async unloadWithTable(
    table: string,
    options: UnloadOptions,
  ): Promise<TableStructure> {
    if (!this.config.exportBucket) {
      throw new Error('Export bucket is not configured.');
    }
    const types = await this.tableColumnTypes(table);
    const columns = types.map(t => t.name).join(', ');
    const {
      bucketType,
      bucketName,
      region,
      unloadArn,
      keyId,
      secretKey,
    } = this.config.exportBucket;
    const conn = await this.pool.connect();
    try {
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
        executionTimeout: this.config.executionTimeout
          ? this.config.executionTimeout * 1000
          : 600000,
      });
      const baseQuery = `
        UNLOAD ('SELECT ${columns} FROM ${table}')
        TO '${bucketType}://${bucketName}/${table}/'
      `;
      const credentialQuery = unloadArn
        ? `iam_role '${unloadArn}'`
        : 'CREDENTIALS ' +
          `'aws_access_key_id=${keyId};` +
          `aws_secret_access_key=${secretKey}'`;
      const unloadQuery =
        `${baseQuery} ${credentialQuery} ${optionsPart}`;
      await conn.query({
        text: unloadQuery,
      });
    } finally {
      await conn.release();
    }
    return types;
  }

  /**
   * Returns an array of signed URLs of the unloaded csv files.
   */
  private async getCsvFiles(table: string): Promise<string[]> {
    if (!this.config.exportBucket) {
      throw new Error('Export bucket is not configured.');
    }
    const {
      bucketName,
      region,
      keyId,
      secretKey,
    } = this.config.exportBucket;
    const client = new S3({
      credentials: (keyId && secretKey) ? {
        accessKeyId: keyId,
        secretAccessKey: secretKey,
      } : undefined,
      region,
    });
    const list = await client.listObjectsV2({
      Bucket: bucketName,
      Prefix: table,
    });
    if (!list || !list.Contents) {
      return [];
    } else {
      const csvFile = await Promise.all(
        list.Contents.map(async (file) => {
          const command = new GetObjectCommand({
            Bucket: bucketName,
            Key: file.Key,
          });
          return getSignedUrl(client, command, { expiresIn: 3600 });
        })
      );
      return csvFile;
    }
  }

  /**
   * Returns an array of queried fields meta info.
   */
  public async queryColumnTypes(sql: string): Promise<TableStructure> {
    const conn = await this.pool.connect();
    const typesSql = `${sql} LIMIT 0`;
    try {
      await this.prepareConnection(conn);
      const result = await conn.query({
        text: typesSql,
      });
      return result.fields.map((field) => ({
        name: field.name,
        type: this.toGenericType(field.format),
      }));
    } finally {
      await conn.release();
    }
  }
}
