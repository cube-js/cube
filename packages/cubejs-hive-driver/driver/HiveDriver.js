const jshs2 = require('jshs2');
const SqlString = require('sqlstring');
const genericPool = require('generic-pool');
const { BaseDriver } = require('@cubejs-backend/base-driver');

const {
  HS2Util, IDLContainer, HiveConnection, Configuration
} = jshs2;
const Connection = require('jshs2/lib/Connection');

const IDLFactory = require('jshs2/lib/common/IDLFactory');

const newIDL = [
  "2.1.1",
  "2.2.3",
  '2.3.4',
];

const oldExtractConfig = IDLFactory.extractConfig;
IDLFactory.extractConfig = (config) => {
  if (newIDL.indexOf(config.HiveVer) !== -1) {
    const thrift = `Thrift_${config.ThriftVer}`;
    const hive = `Hive_${config.HiveVer}`;
    const cdh = config.CDHVer && `CDH_${config.CDHVer}`;

    return {
      thrift,
      hive,
      cdh,
      path: `../../../idl/Hive_${config.HiveVer}`
    };
  }

  return oldExtractConfig(config);
};

const TSaslTransport = require('./TSaslTransport');

class HiveDriver extends BaseDriver {
  /**
   * Returns default concurrency value.
   */
  static getDefaultConcurrency() {
    return 2;
  }

  constructor(config = {}) {
    super();
    this.config = {
      auth: 'PLAIN',
      host: process.env.CUBEJS_DB_HOST,
      port: process.env.CUBEJS_DB_PORT,
      dbName: process.env.CUBEJS_DB_NAME || 'default',
      timeout: 10000,
      username: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
      hiveType: process.env.CUBEJS_DB_HIVE_TYPE === 'CDH' ? HS2Util.HIVE_TYPE.CDH : HS2Util.HIVE_TYPE.HIVE,
      hiveVer: process.env.CUBEJS_DB_HIVE_VER || '2.1.1',
      thriftVer: process.env.CUBEJS_DB_HIVE_THRIFT_VER || '0.9.3',
      cdhVer: process.env.CUBEJS_DB_HIVE_CDH_VER,
      authZid: 'cube.js',
      ...config
    };
    const configuration = new Configuration(this.config);
    this.pool = genericPool.createPool({
      create: async () => {
        const idl = new IDLContainer();
        await idl.initialize(configuration);
        Connection.AUTH_MECHANISMS.PLAIN.transport = TSaslTransport(
          this.config.authZid, this.config.username, this.config.password
        );
        const hiveConnection = new HiveConnection(configuration, idl);
        hiveConnection.cursor = await hiveConnection.connect();
        hiveConnection.cursor.getOperationStatus = function () {
          return new Promise((resolve, reject) => {
            const serviceType = this.Conn.IDL.ServiceType;
            const request = new serviceType.TGetOperationStatusReq({
              operationHandle: this.OperationHandle,
            });

            this.Client.GetOperationStatus(request, (err, res) => {
              if (err) {
                reject(new Error(err));
              } else if (
                res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS ||
                res.operationState === serviceType.TOperationState.ERROR_STATE
              ) {
                // eslint-disable-next-line no-unused-vars
                const [errorMessage, infoMessage, message] = HS2Util.getThriftErrorMessage(
                  res.status, 'ExecuteStatement operation fail'
                );

                reject(new Error(res.errorMessage || message));
              } else {
                resolve(res.operationState);
              }
            });
          });
        };
        return hiveConnection;
      },
      destroy: (connection) => connection.close()
    }, {
      min: 0,
      max: this.config.maxPoolSize || 8,
      evictionRunIntervalMillis: 10000,
      softIdleTimeoutMillis: 30000,
      idleTimeoutMillis: 30000,
      acquireTimeoutMillis: 20000
    });
  }

  async testConnection() {
    // eslint-disable-next-line no-underscore-dangle
    const conn = await this.pool._factory.create();
    try {
      return await this.query('SELECT 1', [], conn);
    } finally {
      // eslint-disable-next-line no-underscore-dangle
      await this.pool._factory.destroy(conn);
    }
  }

  sleep(ms) {
    return new Promise((resolve) => {
      setTimeout(() => resolve(), ms);
    });
  }

  async query(query, values, conn) {
    values = values || [];
    const sql = SqlString.format(query, values);
    const connection = conn || await this.pool.acquire();
    try {
      const execResult = await connection.cursor.execute(sql);
      while (true) {
        const status = await connection.cursor.getOperationStatus();
        if (HS2Util.isFinish(connection.cursor, status)) {
          break;
        }

        await this.sleep(500);
      }

      let allRows = [];
      if (execResult.hasResultSet) {
        const schema = await connection.cursor.getSchema();
        while (true) {
          const results = await connection.cursor.fetchBlock();
          allRows.push(...(results.rows));
          if (!results.hasMoreRows) {
            break;
          }
        }
        allRows = allRows.map(
          row => schema
            .map((column, i) => ({ [column.columnName.replace(/^_u(.+?)\./, '')]: row[i] === 'NULL' ? null : row[i] })) // TODO NULL
            .reduce((a, b) => ({ ...a, ...b }), {})
        );
      }
      return allRows;
    } finally {
      if (!conn) {
        this.pool.release(connection);
      }
    }
  }

  async tablesSchema() {
    const tables = await this.query(`show tables in ${this.config.dbName}`);

    return {
      [this.config.dbName]: (await Promise.all(tables.map(async table => {
        const tableName = table.tab_name || table.tableName;
        const columns = await this.query(`describe ${this.config.dbName}.${tableName}`);
        return {
          [tableName]: columns.map(c => ({ name: c.col_name, type: c.data_type }))
        };
      }))).reduce((a, b) => ({ ...a, ...b }), {})
    };
  }

  async release() {
    await this.pool.drain();
    await this.pool.clear();
  }

  quoteIdentifier(identifier) {
    return `\`${identifier}\``;
  }
}

module.exports = HiveDriver;
