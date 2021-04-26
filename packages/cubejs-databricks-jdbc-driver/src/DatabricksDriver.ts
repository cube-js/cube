/* eslint-disable no-restricted-syntax */
import { JDBCDriver, JDBCDriverConfiguration } from '@cubejs-backend/jdbc-driver';
import { getEnv } from '@cubejs-backend/shared';
import fs from 'fs';
import path from 'path';

import { DatabricksQuery } from './DatabricksQuery';
import { downloadJDBCDriver } from './installer';

export type DatabricksDriverConfiguration = JDBCDriverConfiguration & {
  readOnly?: boolean,
};

async function fileExistsOr(fsPath: string, fn: () => Promise<string>): Promise<string> {
  if (fs.existsSync(fsPath)) {
    return fsPath;
  }

  return fn();
}

type ShowTableRow = { database: string, tableName: string, isTemporary: boolean };
type ShowDatabasesRow = { databaseName: string };

const DatabricksToGenericType: Record<string, string> = {
  'decimal(10,0)': 'bigint',
};

const jdbcDriverResolver: Promise<string>|null = null;

async function resolveJDBCDriver(): Promise<string> {
  if (jdbcDriverResolver) {
    return jdbcDriverResolver;
  }

  return fileExistsOr(
    path.join(process.cwd(), 'SparkJDBC42.jar'),
    async () => fileExistsOr(path.join(__dirname, '..', '..', 'download', 'SparkJDBC42.jar'), async () => {
      const pathOrNull = await downloadJDBCDriver(false);
      if (pathOrNull) {
        return pathOrNull;
      }

      throw new Error('Please download and place SparkJDBC42.jar inside your project directory');
    })
  );
}

export class DatabricksDriver extends JDBCDriver {
  protected readonly config: DatabricksDriverConfiguration;

  public static dialectClass() {
    return DatabricksQuery;
  }

  public constructor(configuration: Partial<DatabricksDriverConfiguration>) {
    const config: DatabricksDriverConfiguration = {
      database: getEnv('dbName', { required: false }),
      dbType: 'databricks',
      url: getEnv('databrickUrl'),
      drivername: 'com.simba.spark.jdbc.Driver',
      customClassPath: undefined,
      properties: {},
      ...configuration
    };

    super(config);

    this.config = config;
  }

  public readOnly() {
    return !!this.config.readOnly;
  }

  protected async getCustomClassPath() {
    return resolveJDBCDriver();
  }

  public async createSchemaIfNotExists(schemaName: string) {
    return this.query(`CREATE SCHEMA IF NOT EXISTS ${schemaName}`, []);
  }

  public quoteIdentifier(identifier: string): string {
    return `\`${identifier}\``;
  }

  public async tableColumnTypes(table: string) {
    const [schema, tableName] = table.split('.');

    const result = [];
    const response: any[] = await this.query(`DESCRIBE ${schema}.${tableName}`, []);

    for (const column of response) {
      // Databricks describe additional info by default after empty line.
      if (column.col_name === '') {
        break;
      }

      result.push({ name: column.col_name, type: this.toGenericType(column.data_type) });
    }

    return result;
  }

  public async getTablesQuery(schemaName: string) {
    const response = await this.query(`SHOW TABLES IN ${this.quoteIdentifier(schemaName)}`, []);

    return response.map((row: any) => ({
      table_name: row.tableName,
    }));
  }

  protected async getTables(): Promise<ShowTableRow[]> {
    if (this.config.database) {
      return <any> this.query(`SHOW TABLES IN ${this.quoteIdentifier(this.config.database)}`, []);
    }

    const databases: ShowDatabasesRow[] = await this.query('SHOW DATABASES', []);

    const allTables: (ShowTableRow[])[] = await Promise.all(
      databases.map(async ({ databaseName }) => this.query(
        `SHOW TABLES IN ${this.quoteIdentifier(databaseName)}`,
        []
      ))
    );

    return allTables.flat();
  }

  public toGenericType(columnType: string): string {
    return DatabricksToGenericType[columnType.toLowerCase()] || super.toGenericType(columnType);
  }

  public async tablesSchema() {
    const tables = await this.getTables();

    const metadata: Record<string, Record<string, object>> = {};

    await Promise.all(tables.map(async ({ database, tableName }) => {
      if (!(database in metadata)) {
        metadata[database] = {};
      }

      const columns = await this.tableColumnTypes(`${database}.${tableName}`);
      metadata[database][tableName] = columns;
    }));

    return metadata;
  }
}
