/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `RedshiftDriver` and related types declaration.
 */

import { assertDataSource, getEnv } from '@cubejs-backend/shared';
import { PostgresDriver, PostgresDriverConfiguration } from '@cubejs-backend/postgres-driver';
import {
  DatabaseStructure,
  DownloadTableCSVData,
  DriverCapabilities,
  InformationSchemaColumn,
  QueryColumnsResult,
  QuerySchemasResult,
  QueryTablesResult,
  StreamOptions,
  StreamTableDataWithTypes,
  TableColumn,
  TableStructure,
  UnloadOptions
} from '@cubejs-backend/base-driver';
import crypto from 'crypto';

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

const IGNORED_SCHEMAS = ['pg_catalog', 'pg_internal', 'information_schema', 'mysql', 'performance_schema', 'sys', 'INFORMATION_SCHEMA'];

/**
 * Redshift driver class.
 */
export class RedshiftDriver extends PostgresDriver<RedshiftDriverConfiguration> {
  private readonly dbName: string;

  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 5;
  }

  /**
   * Class constructor.
   */
  public constructor(
    config: RedshiftDriverConfiguration & {
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
    super(config);

    const dataSource =
      config.dataSource ||
      assertDataSource('default');

    // We need a DB name for querying external tables.
    // It's not possible to get it later from the pool
    this.dbName = getEnv('dbName', { dataSource });
  }

  protected primaryKeysQuery() {
    return null;
  }

  protected foreignKeysQuery() {
    return null;
  }

  /**
   * @override
   */
  protected override informationSchemaQuery() {
    return `
      SELECT columns.column_name as ${this.quoteIdentifier('column_name')},
             columns.table_name as ${this.quoteIdentifier('table_name')},
             columns.table_schema as ${this.quoteIdentifier('table_schema')},
             columns.data_type as ${this.quoteIdentifier('data_type')}
      FROM information_schema.columns
      WHERE columns.table_schema NOT IN (${IGNORED_SCHEMAS.map(s => `'${s}'`).join(',')})
   `;
  }

  /**
   * In Redshift schemas not owned by the current user are not shown in regular information_schema,
   * so it needs to be queried through the pg_namespace table. Because user might be granted specific
   * permissions on the concrete schema (like CREATE tables in already existing but not owned pre-aggregation schema).
   * @override
   */
  public override async createSchemaIfNotExists(schemaName: string): Promise<void> {
    const schemaExistsQuery = `SELECT nspname FROM pg_namespace where nspname = ${this.param(0)}`;
    const schemas = await this.query(schemaExistsQuery, [schemaName]);
    if (schemas.length === 0) {
      await this.query(`CREATE SCHEMA IF NOT EXISTS ${schemaName}`, []);
    }
  }

  /**
   * In Redshift external tables are not shown in regular Postgres information_schema,
   * so it needs to be queried separately.
   * @override
   */
  public override async tablesSchema(): Promise<DatabaseStructure> {
    const query = this.informationSchemaQuery();
    const data: InformationSchemaColumn[] = await this.query(query, []);
    const tablesSchema = this.informationColumnsSchemaSorter(data)
      .reduce<DatabaseStructure>(this.informationColumnsSchemaReducer, {});

    const allSchemas = await this.getSchemas();
    const externalSchemas = allSchemas.filter(s => !tablesSchema[s.schema_name]).map(s => s.schema_name);

    for (const externalSchema of externalSchemas) {
      tablesSchema[externalSchema] = {};
      const tablesRes = await this.tablesForExternalSchema(externalSchema);
      const tables = tablesRes.map(t => t.table_name);
      for (const tableName of tables) {
        const columnRes = await this.columnsForExternalTable(externalSchema, tableName);
        tablesSchema[externalSchema][tableName] = columnRes.map(def => ({
          name: def.column_name,
          type: def.data_type,
          attributes: []
        }));
      }
    }

    return tablesSchema;
  }

  // eslint-disable-next-line camelcase
  private async tablesForExternalSchema(schemaName: string): Promise<{ table_name: string }[]> {
    return this.query(`SHOW TABLES FROM SCHEMA ${this.dbName}.${schemaName}`, []);
  }

  private async columnsForExternalTable(schemaName: string, tableName: string): Promise<QueryColumnsResult[]> {
    return this.query(`SHOW COLUMNS FROM TABLE ${this.dbName}.${schemaName}.${tableName}`, []);
  }

  /**
   * @override
   */
  protected override getSchemasQuery() {
    return `
      SELECT table_schema as ${this.quoteIdentifier('schema_name')}
      FROM information_schema.tables
      WHERE table_schema NOT IN (${IGNORED_SCHEMAS.map(s => `'${s}'`).join(',')})
      GROUP BY table_schema
    `;
  }

  /**
   * From the Redshift docs:
   * SHOW SCHEMAS FROM DATABASE database_name [LIKE 'filter_pattern'] [LIMIT row_limit ]
   * It returns regular schemas (queryable from information_schema) and external ones.
   * @override
   */
  public override async getSchemas(): Promise<QuerySchemasResult[]> {
    const schemas = await this.query<QuerySchemasResult>(`SHOW SCHEMAS FROM DATABASE ${this.dbName}`, []);

    return schemas
      .filter(s => !IGNORED_SCHEMAS.includes(s.schema_name))
      .map(s => ({ schema_name: s.schema_name }));
  }

  public override async getTablesForSpecificSchemas(schemas: QuerySchemasResult[]): Promise<QueryTablesResult[]> {
    const tables = await super.getTablesForSpecificSchemas(schemas);

    // We might request the external schemas and tables, their descriptions won't be returned
    // by the super.getTablesForSpecificSchemas(). Need to request them separately.
    const missedSchemas = schemas.filter(s => !tables.some(t => t.schema_name === s.schema_name));

    for (const externalSchema of missedSchemas) {
      const tablesRes = await this.tablesForExternalSchema(externalSchema.schema_name);
      tablesRes.forEach(t => {
        tables.push({ schema_name: externalSchema.schema_name, table_name: t.table_name });
      });
    }

    return tables;
  }

  public override async getColumnsForSpecificTables(tables: QueryTablesResult[]): Promise<QueryColumnsResult[]> {
    const columns = await super.getColumnsForSpecificTables(tables);

    // We might request the external tables, their descriptions won't be returned
    // by the super.getColumnsForSpecificTables(). Need to request them separately.
    const missedTables = tables.filter(table => !columns.some(column => column.schema_name === table.schema_name && column.table_name === table.table_name));

    for (const table of missedTables) {
      const columnRes = await this.columnsForExternalTable(table.schema_name, table.table_name);
      columnRes.forEach(c => {
        columns.push({
          schema_name: c.schema_name,
          table_name: c.table_name,
          column_name: c.column_name,
          data_type: c.data_type,
        });
      });
    }

    return columns;
  }

  /**
   * @override
   */
  protected getInitialConfiguration(
    dataSource: string,
  ): Partial<RedshiftDriverConfiguration> {
    return {
      // @todo It's not possible to support UNLOAD in readOnly mode, because we need column types (CREATE TABLE?)
      readOnly: false,
      exportBucket: this.getExportBucket(dataSource),
    };
  }

  protected static checkValuesLimit(values?: unknown[]) {
    // Redshift server is not exactly compatible with PostgreSQL protocol
    // And breaks after 32767 parameter values with `there is no parameter $-32768`
    // This is a bug/misbehaviour on server side, nothing we can do besides generate a more meaningful error
    const length = (values?.length ?? 0);
    if (length >= 32768) {
      throw new Error(`Redshift server does not support more than 32767 parameters, but ${length} passed`);
    }
  }

  public override async createTable(quotedTableName: string, columns: TableColumn[]): Promise<void> {
    if (quotedTableName.length > 127) {
      throw new Error('Redshift can not work with table names longer than 127 symbols. ' +
        `Consider using the 'sqlAlias' attribute in your cube definition for ${quotedTableName}.`);
    }

    // we can not call super.createTable(quotedTableName, columns)
    // because Postgres has 63 length check. So pasting the code from the base driver
    const createTableSql = this.createTableSql(quotedTableName, columns);
    await this.query(createTableSql, []).catch(e => {
      e.message = `Error during create table: ${createTableSql}: ${e.message}`;
      throw e;
    });
  }

  /**
   * AWS Redshift doesn't have any special connection check.
   * And querying even system tables is billed.
   * @override
   */
  public override async testConnection() {
    const conn = await this.pool.connect();
    conn.release();
  }

  public override async stream(
    query: string,
    values: unknown[],
    options: StreamOptions
  ): Promise<StreamTableDataWithTypes> {
    RedshiftDriver.checkValuesLimit(values);

    return super.stream(query, values, options);
  }

  protected override async queryResponse(query: string, values: unknown[]) {
    RedshiftDriver.checkValuesLimit(values);

    return super.queryResponse(query, values);
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

  public override async tableColumnTypes(table: string): Promise<TableStructure> {
    const columns: TableStructure = await super.tableColumnTypes(table);

    if (columns.length) {
      return columns;
    }

    // It's possible that table is external Spectrum table, so we need to query it separately
    const [schema, name] = table.split('.');

    // We might get table from Spectrum schema, so common request via `information_schema.columns`
    // won't return anything. `getColumnsForSpecificTables` is aware of Spectrum tables.
    const columnRes = await this.columnsForExternalTable(schema, name);

    return columnRes.map(c => ({ name: c.column_name, type: this.toGenericType(c.data_type) }));
  }

  public async isUnloadSupported() {
    return !!this.config.exportBucket;
  }

  public async unload(tableName: string, options: UnloadOptions): Promise<DownloadTableCSVData> {
    if (!this.config.exportBucket) {
      throw new Error('Unload is not configured');
    }

    const types = await this.tableColumnTypes(tableName);
    const columns = types.map(t => t.name).join(', ');

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

      const baseQuery = `
        UNLOAD ('SELECT ${columns} FROM ${tableName}')
        TO '${bucketType}://${bucketName}/${exportPathName}/'
      `;

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
          exportBucketCsvEscapeSymbol: this.config.exportBucketCsvEscapeSymbol,
          csvFile: [],
          types
        };
      }

      const csvFile = await this.extractUnloadedFilesFromS3(
        {
          credentials: (keyId && secretKey) ? {
            accessKeyId: keyId,
            secretAccessKey: secretKey,
          } : undefined,
          region,
        },
        bucketName,
        exportPathName,
      );

      if (csvFile.length === 0) {
        throw new Error('Unable to UNLOAD table, there are no files in S3 storage');
      }

      return {
        exportBucketCsvEscapeSymbol: this.config.exportBucketCsvEscapeSymbol,
        csvFile,
        types
      };
    } finally {
      conn.removeAllListeners('notice');
      conn.release();
    }
  }

  public capabilities(): DriverCapabilities {
    return {
      incrementalSchemaLoading: true,
    };
  }
}
