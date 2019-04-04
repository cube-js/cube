/* eslint-disable global-require */
const ApiGateway = require('@cubejs-backend/api-gateway');
const CompilerApi = require('./CompilerApi');
const OrchestratorApi = require('./OrchestratorApi');
const FileRepository = require('./FileRepository');
const DevServer = require('./DevServer');

const DriverDependencies = {
  postgres: '@cubejs-backend/postgres-driver',
  mysql: '@cubejs-backend/mysql-driver',
  athena: '@cubejs-backend/athena-driver',
  jdbc: '@cubejs-backend/jdbc-driver',
  mongobi: '@cubejs-backend/mongobi-driver',
  bigquery: '@cubejs-backend/bigquery-driver'
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
    this.apiSecret = options.apiSecret;
    this.schemaPath = options.schemaPath || 'schema';
    this.dbType = options.dbType;
    this.logger = options.logger || ((msg, params) => { console.log(`${msg}: ${JSON.stringify(params)}`); });
    this.repository = new FileRepository(this.schemaPath);

    if (this.options.devServer) {
      this.devServer = new DevServer(this);
      const Analytics = require('analytics-node');
      const client = new Analytics('dSR8JiNYIGKyQHKid9OaLYugXLao18hA', { flushInterval: 100 });
      const { machineIdSync } = require('node-machine-id');
      const { promisify } = require('util');
      const anonymousId = machineIdSync();
      this.anonymousId = anonymousId;
      this.event = async (name, props) => {
        try {
          await promisify(client.track.bind(client))({
            event: name,
            anonymousId,
            properties: props
          });
          await promisify(client.flush.bind(client))();
        } catch (e) {
          // console.error(e);
        }
      };
      if (!options.logger) {
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
          console.log(`${msg}: ${JSON.stringify(params)}`);
        });
      }
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
    }
  }

  static create(options) {
    return new CubejsServerCore(options);
  }

  async initApp(app) {
    checkEnvForPlaceholders();
    this.compilerApi = this.createCompilerApi(this.repository);
    this.orchestratorApi = this.createOrchestratorApi();
    const apiGateway = new ApiGateway(
      this.apiSecret,
      this.compilerApi,
      this.orchestratorApi,
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

  createCompilerApi(repository) {
    return new CompilerApi(repository, this.dbType, {
      schemaVersion: this.options.schemaVersion,
      devServer: this.options.devServer,
      logger: this.logger
    });
  }

  createOrchestratorApi() {
    return new OrchestratorApi(() => this.getDriver(), this.logger, {
      redisPrefix: process.env.CUBEJS_APP,
      ...this.options.orchestratorOptions
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
    return DriverDependencies[dbType] || DriverDependencies.jdbc;
  }
}

module.exports = CubejsServerCore;
