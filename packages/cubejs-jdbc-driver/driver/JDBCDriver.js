const BaseDriver = require('@cubejs-backend/query-orchestrator/driver/BaseDriver');
const SqlString = require('sqlstring');

const applyParams = (query, params) => {
  return SqlString.format(query, params);
};

const { promisify } = require('util');
const genericPool = require('generic-pool');
const DriverManager = require('jdbc/lib/drivermanager');
const Connection = require('jdbc/lib/connection');
const jinst = require('jdbc/lib/jinst');
const mvn = promisify(require('node-java-maven'));

let mvnPromise = null;

const initMvn = (customClassPath) => {
  if (!mvnPromise) {
    mvnPromise = mvn().then((mvnResults) => {
      if (!jinst.isJvmCreated()) {
        jinst.addOption("-Xrs");
        const classPath = mvnResults.classpath.concat(customClassPath || []);
        jinst.setupClasspath(classPath);
      }
    });
  }
  return mvnPromise;
};

const DbTypes = {
  mysql: {
    driverClass: "com.mysql.jdbc.Driver",
    prepareConnectionQueries: [`SET time_zone = '+00:00'`],
    mavenDependency: {
      "groupId": "mysql",
      "artifactId": "mysql-connector-java",
      "version": "8.0.13"
    },
    properties: {
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
    },
    jdbcUrl: () => `jdbc:mysql://${process.env.CUBEJS_DB_HOST}:3306/${process.env.CUBEJS_DB_NAME}`
  },
  athena: {
    driverClass: "com.qubole.jdbc.jdbc41.core.QDriver",
    prepareConnectionQueries: [],
    mavenDependency: {
      "groupId": "com.syncron.amazonaws",
      "artifactId": "simba-athena-jdbc-driver",
      "version": "2.0.2"
    },
    jdbcUrl: () => `jdbc:awsathena://AwsRegion=${process.env.CUBEJS_AWS_REGION}`,
    properties: {
      UID: process.env.CUBEJS_AWS_KEY,
      PWD: process.env.CUBEJS_AWS_SECRET,
      S3OutputLocation: process.env.CUBEJS_AWS_S3_OUTPUT_LOCATION
    }
  }
};

class JDBCDriver extends BaseDriver {
  constructor(config) {
    super();
    config = config || {};

    const dbTypeDescription = JDBCDriver.dbTypeDescription(config.dbType || process.env.CUBEJS_DB_TYPE);
    this.config = {
      dbType: process.env.CUBEJS_DB_TYPE,
      url: process.env.CUBEJS_JDBC_URL || dbTypeDescription && dbTypeDescription.jdbcUrl(),
      drivername: process.env.CUBEJS_JDBC_DRIVER || dbTypeDescription && dbTypeDescription.driverClass,
      properties: dbTypeDescription && dbTypeDescription.properties,
      ...config
    };

    if (!this.config.drivername) {
      throw new Error('drivername is required property');
    }
    if (!this.config.url) {
      throw new Error('url is required property');
    }

    this.pool = genericPool.createPool({
      create: async () => {
        await initMvn(config.customClassPath);
        if (!this.jdbcProps) {
          this.jdbcProps = this.getJdbcProperties();
        }
        const getConnection = promisify(DriverManager.getConnection.bind(DriverManager));
        return new Connection(await getConnection(this.config.url, this.jdbcProps));
      },
      destroy: async (connection) => {
        return promisify(connection.close.bind(connection));
      },
      validate: (connection) => {
        const isValid = promisify(connection.isValid.bind(connection));
        try {
          return isValid(this.testConnectionTimeout() / 1000);
        } catch (e) {
          return false;
        }
      }
    }, {
      min: 0,
      max: 8,
      evictionRunIntervalMillis: 10000,
      softIdleTimeoutMillis: 30000,
      idleTimeoutMillis: 30000,
      testOnBorrow: true,
      acquireTimeoutMillis: 20000
    });
  }

  getJdbcProperties() {
    const java = jinst.getInstance();
    const Properties = java.import('java.util.Properties');
    const properties = new Properties();

    for(let name in this.config.properties) {
      properties.putSync(name, this.config.properties[name]);
    }

    return properties;
  }

  testConnection() {
    return this.query(`SELECT 1`, []);
  }

  prepareConnectionQueries() {
    let dbTypeDescription = JDBCDriver.dbTypeDescription(this.config.dbType);
    return this.config.prepareConnectionQueries ||
      dbTypeDescription && dbTypeDescription.prepareConnectionQueries ||
      [];
  }

  query(query, values) {
    const queryWithParams = applyParams(query, values);
    const cancelObj = {};
    const promise = this.queryPromised(queryWithParams, cancelObj, this.prepareConnectionQueries());
    promise.cancel =
      () => cancelObj.cancel && cancelObj.cancel() || Promise.reject(new Error('Statement is not ready'));
    return promise;
  }

  async queryPromised(query, cancelObj, options) {
    options = options || {};
    try {
      const conn = await this.pool.acquire();
      try {
        const prepareConnectionQueries = options.prepareConnectionQueries || [];
        for (let i = 0; i < prepareConnectionQueries.length; i++) {
          await this.executeStatement(conn, prepareConnectionQueries[i]);
        }
        return await this.executeStatement(conn, query, cancelObj);
      } finally {
        await this.pool.release(conn);
      }
    } catch(ex) {
      if (ex.cause) {
        throw new Error(ex.cause.getMessageSync());
      } else {
        throw ex;
      }
    }
  }

  async executeStatement(conn, query, cancelObj) {
    const createStatementAsync = promisify(conn.createStatement.bind(conn));
    const statement = await createStatementAsync();
    if (cancelObj) {
      cancelObj.cancel = promisify(statement.cancel.bind(statement));
    }
    const setQueryTimeout = promisify(statement.setQueryTimeout.bind(statement));
    await setQueryTimeout(600);
    const executeQueryAsync = promisify(statement.execute.bind(statement));
    const resultSet = await executeQueryAsync(query);
    const toObjArrayAsync =
      resultSet.toObjArray && promisify(resultSet.toObjArray.bind(resultSet)) ||
      (() => Promise.resolve(resultSet));
    return await toObjArrayAsync();
  }

  async release() {
    await this.pool.drain();
    await this.pool.clear();
  }

  static dbTypeDescription(dbType) {
    return DbTypes[dbType];
  }
}

module.exports = JDBCDriver;
