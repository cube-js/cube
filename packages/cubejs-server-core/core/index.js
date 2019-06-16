/* eslint-disable global-require */
const ApiGateway = require('@cubejs-backend/api-gateway');
const crypto = require('crypto');
const fs = require('fs-extra');
const path = require('path');
const CompilerApi = require('./CompilerApi');
const OrchestratorApi = require('./OrchestratorApi');
const FileRepository = require('./FileRepository');
const DevServer = require('./DevServer');

const DriverDependencies = {
  postgres: '@cubejs-backend/postgres-driver',
  mysql: '@cubejs-backend/mysql-driver',
  mssql: '@cubejs-backend/mssql-driver',
  athena: '@cubejs-backend/athena-driver',
  jdbc: '@cubejs-backend/jdbc-driver',
  mongobi: '@cubejs-backend/mongobi-driver',
  bigquery: '@cubejs-backend/bigquery-driver',
  redshift: '@cubejs-backend/postgres-driver',
  clickhouse: '@cubejs-backend/clickhouse-driver',
  hive: '@cubejs-backend/hive-driver'
};

const checkEnvForPlaceholders = () => {
  const placeholderSubstr = '<YOUR_DB_';
  const credentials = [
    'CUBEJS_DB_HOST',
    'CUBEJS_DB_NAME',
    'CUBEJS_DB_USER',
    'CUBEJS_DB_PASS'
  ];
  if (
    credentials.find((credential) => (
      process.env[credential] && process.env[credential].indexOf(placeholderSubstr) === 0
    ))
  ) {
    throw new Error('Your .env file contains placeholders in DB credentials. Please replace them with your DB credentials.');
  }
};

class CubejsServerCore {
  constructor(options) {
    options = options || {};
    options = {
      driverFactory: () => CubejsServerCore.createDriver(options.dbType),
      apiSecret: process.env.CUBEJS_API_SECRET,
      dbType: process.env.CUBEJS_DB_TYPE,
      devServer: process.env.NODE_ENV !== 'production',
      telemetry: process.env.CUBEJS_TELEMETRY !== 'false',
      ...options
    };
    if (
      !options.driverFactory ||
      !options.apiSecret ||
      !options.dbType
    ) {
      throw new Error('driverFactory, apiSecret, dbType are required options');
    }
    this.options = options;
    this.driverFactory = options.driverFactory;
    this.externalDriverFactory = options.externalDriverFactory;
    this.apiSecret = options.apiSecret;
    this.schemaPath = options.schemaPath || 'schema';
    this.dbType = options.dbType;
    this.logger = options.logger || ((msg, params) => {
      const { error, ...restParams } = params;
      if (process.env.NODE_ENV !== 'production') {
        console.log(`${msg}: ${JSON.stringify(restParams)}${error ? `\n${error}` : ''}`);
      } else {
        console.log(JSON.stringify({ message: msg, ...params }));
      }
    });
    this.repository = new FileRepository(this.schemaPath);
    this.repositoryFactory = options.repositoryFactory || (() => this.repository);
    this.contextToDbType = typeof options.dbType === 'function' ? options.dbType : () => options.dbType;
    this.contextToExternalDbType = typeof options.externalDbType === 'function' ?
      options.externalDbType :
      () => options.externalDbType;
    this.appIdToCompilerApi = {};
    this.appIdToOrchestratorApi = {};
    this.contextToAppId = options.contextToAppId || (() => process.env.CUBEJS_APP || 'STANDALONE');
    this.orchestratorOptions =
      typeof options.orchestratorOptions === 'function' ?
        options.orchestratorOptions :
        () => options.orchestratorOptions;

    const Analytics = require('analytics-node');
    const client = new Analytics('dSR8JiNYIGKyQHKid9OaLYugXLao18hA', { flushInterval: 100 });
    const { machineIdSync } = require('node-machine-id');
    const { promisify } = require('util');
    const anonymousId = machineIdSync();
    this.anonymousId = anonymousId;
    this.event = async (name, props) => {
      if (!options.telemetry) {
        return;
      }
      try {
        if (!this.projectFingerprint) {
          try {
            this.projectFingerprint =
              crypto.createHash('md5').update(JSON.stringify(await fs.readJson('package.json'))).digest('hex');
            const coreServerJson = await fs.readJson(path.join(__dirname, '..', 'package.json'));
            this.coreServerVersion = coreServerJson.version;
          } catch (e) {
            // console.error(e);
          }
        }
        await promisify(client.track.bind(client))({
          event: name,
          anonymousId,
          properties: {
            projectFingerprint: this.projectFingerprint,
            coreServerVersion: this.coreServerVersion,
            ...props
          }
        });
        await promisify(client.flush.bind(client))();
      } catch (e) {
        // console.error(e);
      }
    };

    if (this.options.devServer) {
      this.devServer = new DevServer(this);
      const oldLogger = this.logger;
      this.logger = ((msg, params) => {
        if (
          msg === 'Load Request' ||
          msg === 'Load Request Success' ||
          msg === 'Orchestrator error' ||
          msg === 'Internal Server Error' ||
          msg === 'User Error' ||
          msg === 'Compiling schema'
        ) {
          this.event(msg, { error: params.error });
        }
        oldLogger(msg, params);
      });
      let causeErrorPromise;
      process.on('uncaughtException', async (e) => {
        console.error(e.stack || e);
        if (e.message && e.message.indexOf('Redis connection to') !== -1) {
          console.log('🛑 Cube.js Server requires locally running Redis instance to connect to');
          if (process.platform.indexOf('win') === 0) {
            console.log('💾 To install Redis on Windows please use https://github.com/MicrosoftArchive/redis/releases');
          } else if (process.platform.indexOf('darwin') === 0) {
            console.log('💾 To install Redis on Mac please use https://redis.io/topics/quickstart or `$ brew install redis`');
          } else {
            console.log('💾 To install Redis please use https://redis.io/topics/quickstart');
          }
        }
        if (!causeErrorPromise) {
          causeErrorPromise = this.event('Dev Server Fatal Error', {
            error: (e.stack || e.message || e).toString()
          });
        }
        await causeErrorPromise;
        process.exit(1);
      });
    } else {
      this.event('Server Start');
    }
  }

  static create(options) {
    return new CubejsServerCore(options);
  }

  async initApp(app) {
    checkEnvForPlaceholders();
    const apiGateway = new ApiGateway(
      this.apiSecret,
      this.getCompilerApi.bind(this),
      this.getOrchestratorApi.bind(this),
      this.logger, {
        basePath: this.options.basePath,
        checkAuthMiddleware: this.options.checkAuthMiddleware
      }
    );
    apiGateway.initApp(app);
    if (this.options.devServer) {
      this.devServer.initDevEnv(app);
    }
  }

  getCompilerApi(context) {
    const appId = this.contextToAppId(context);
    if (!this.appIdToCompilerApi[appId]) {
      this.appIdToCompilerApi[appId] = this.createCompilerApi(
        this.repositoryFactory(context), {
          dbType: this.contextToDbType(context),
          externalDbType: this.contextToExternalDbType(context),
          schemaVersion: this.options.schemaVersion && (() => this.options.schemaVersion(context))
        }
      );
    }
    return this.appIdToCompilerApi[appId];
  }

  getOrchestratorApi(context) {
    const appId = this.contextToAppId(context);
    if (!this.appIdToOrchestratorApi[appId]) {
      let driverPromise;
      let externalPreAggregationsDriverPromise;
      this.appIdToOrchestratorApi[appId] = this.createOrchestratorApi({
        getDriver: () => {
          if (!driverPromise) {
            const driver = this.driverFactory(context);
            driverPromise = driver.testConnection().then(() => driver).catch(e => {
              driverPromise = null;
              throw e;
            });
          }
          return driverPromise;
        },
        getExternalDriverFactory: () => {
          if (!externalPreAggregationsDriverPromise) {
            const driver = this.externalDriverFactory(context);
            externalPreAggregationsDriverPromise = driver.testConnection().then(() => driver).catch(e => {
              externalPreAggregationsDriverPromise = null;
              throw e;
            });
          }
          return externalPreAggregationsDriverPromise;
        },
        redisPrefix: appId,
        orchestratorOptions: this.orchestratorOptions(context)
      });
    }
    return this.appIdToOrchestratorApi[appId];
  }

  createCompilerApi(repository, options) {
    options = options || {};
    return new CompilerApi(repository, options.dbType || this.dbType, {
      schemaVersion: options.schemaVersion || this.options.schemaVersion,
      devServer: this.options.devServer,
      logger: this.logger,
      externalDbType: options.externalDbType
    });
  }

  createOrchestratorApi(options) {
    options = options || {};
    return new OrchestratorApi(options.getDriver || this.getDriver.bind(this), this.logger, {
      redisPrefix: options.redisPrefix || process.env.CUBEJS_APP,
      externalDriverFactory: options.getExternalDriverFactory,
      ...(options.orchestratorOptions || this.options.orchestratorOptions)
    });
  }

  async getDriver() {
    if (!this.driver) {
      const driver = this.driverFactory();
      await driver.testConnection(); // TODO mutex
      this.driver = driver;
    }
    return this.driver;
  }

  static createDriver(dbType) {
    checkEnvForPlaceholders();
    // eslint-disable-next-line global-require,import/no-dynamic-require
    return new (require(CubejsServerCore.driverDependencies(dbType || process.env.CUBEJS_DB_TYPE)))();
  }

  static driverDependencies(dbType) {
    if (!DriverDependencies[dbType]) {
      throw new Error(`Unsupported db type: ${dbType}`);
    }
    return DriverDependencies[dbType];
  }
}

module.exports = CubejsServerCore;
