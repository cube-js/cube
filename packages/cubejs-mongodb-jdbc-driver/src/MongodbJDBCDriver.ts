/* eslint-disable no-restricted-syntax */
import fs from 'fs';
import path from 'path';
import {
  JDBCDriver,
  JDBCDriverConfiguration,
} from '@cubejs-backend/jdbc-driver';
import { getEnv } from '@cubejs-backend/shared';
import { MongodbJDBCQuery } from './MongodbJDBCQuery';
import { downloadJDBCDriver, driverName } from './installer';

const { version } = require('../../package.json');

export type DatabricksDriverConfiguration = JDBCDriverConfiguration &
  {
    readOnly?: boolean,
    // common bucket config
    bucketType?: string,
    exportBucket?: string,
    exportBucketMountDir?: string,
    pollInterval?: number,
    // AWS bucket config
    awsKey?: string,
    awsSecret?: string,
    awsRegion?: string,
    // Azure export bucket
    azureKey?: string,
  };

async function fileExistsOr(
  fsPath: string,
  fn: () => Promise<string>,
): Promise<string> {
  if (fs.existsSync(fsPath)) {
    return fsPath;
  }
  return fn();
}

type ShowTableRow = {
  database: string,
  tableName: string,
  isTemporary: boolean,
};

type ShowDatabasesRow = {
  databaseName: string,
};

const DatabricksToGenericType: Record<string, string> = {
  'decimal(10,0)': 'bigint',
};

async function resolveJDBCDriver(): Promise<string> {
  return fileExistsOr(
    path.join(process.cwd(), driverName),
    async () => fileExistsOr(
      path.join(__dirname, '..', 'download', driverName),
      async () => {
        const pathOrNull = await downloadJDBCDriver();
        if (pathOrNull) {
          return pathOrNull;
        }
        throw new Error(
          'Please download and place mongodb-jdbc-2.0.0-all.jar inside your ' +
          'project directory'
        );
      }
    )
  );
}

/**
 * Databricks driver class.
 */
export class MongodbJDBCDriver extends JDBCDriver {
  protected readonly config: DatabricksDriverConfiguration;

  public static dialectClass() {
    return MongodbJDBCQuery;
  }

  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 2;
  }

  public constructor(
    conf: Partial<DatabricksDriverConfiguration> = {},
  ) {
    const config: DatabricksDriverConfiguration = {
      ...conf,
      
      drivername: 'com.mongodb.jdbc.MongoDriver',
      properties: {
        user: 'admin',
        password: 'admin',
        loglevel: 'INFO',
        database: getEnv('dbName', { required: false }),
      },
      dbType: 'mongodb',
      database: getEnv('dbName', { required: false }),
      url: getEnv('mongodbJDBCUrl'),
    };
    super(config);
    this.config = config;
  }

  public readOnly() {
    return false;
  }

  /**
   * @override
   */
  protected async getCustomClassPath() {
    return resolveJDBCDriver();
  }

  public async createSchemaIfNotExists(schemaName: string) {
    return this.query(`CREATE SCHEMA IF NOT EXISTS ${schemaName}`, []);
  }

  public quoteIdentifier(identifier: string): string {
    return `\`${identifier}\``;
  }

  /**
   * Determines whether export bucket feature is configured or no.
   * @returns {boolean}
   */
  public async isUnloadSupported() {
    return false;
  }
}
