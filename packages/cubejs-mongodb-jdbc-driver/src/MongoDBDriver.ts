import { assertDataSource, getEnv } from '@cubejs-backend/shared';
import { JDBCDriver, JDBCDriverConfiguration } from '@cubejs-backend/jdbc-driver';
import fs from 'fs';
import path from 'path';

import { downloadJDBCDriver } from './installer';
import { MongoDBDriverQuery } from './MongoDBDriverQuery';

export class MongoDBDriver extends JDBCDriver {
  public constructor(
    conf: Partial<JDBCDriverConfiguration> & {
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
       * request before determining it as not valid. Default - 60000 ms.
       */
      testConnectionTimeout?: number,
    } = {}
  ) {
    const dataSource = conf.dataSource || assertDataSource('default');

    const config: JDBCDriverConfiguration = {
      database: getEnv('dbName', { dataSource }),
      dbType: 'mongodb',
      url: getEnv('jdbcUrl', { dataSource }),
      drivername: 'com.mongodb.jdbc.MongoDriver',
      properties: {
        user: getEnv('dbUser', { dataSource }),
        password: getEnv('dbPass', { dataSource }),
      },
      ...conf,
    };

    super(config);
  }

  protected async getCustomClassPath() {
    const customClassPath = path.join(__dirname, '..', 'mongodb-jdbc-2.2.0-all.jar');

    if (!fs.existsSync(customClassPath)) {
      await downloadJDBCDriver(customClassPath);
    }

    return customClassPath;
  }

  public readOnly() {
    return true;
  }

  public async tablesSchema() {
    const data = await this.withConnection(async (connection: any) => {
      const metaData = await connection.getMetaDataAsync();
      const columnsResult = await metaData.getColumnsAsync(null, null, null, null);
      const columns = await columnsResult.toObjArrayAsync();

      return columns.map((column: any) => ({
        column_name: column.COLUMN_NAME,
        table_name: column.TABLE_NAME,
        table_schema: column.TABLE_CAT,
        data_type: column.TYPE_NAME,
      }));
    });

    return data.reduce(this.informationColumnsSchemaReducer, {});
  }

  public static dialectClass() {
    return MongoDBDriverQuery;
  }
}
