/* eslint-disable global-require */
const ApiGateway = require('@cubejs-backend/api-gateway');
const crypto = require('crypto');
const fs = require('fs-extra');
const path = require('path');
const LRUCache = require('lru-cache');
const SqlString = require('sqlstring');
const R = require('ramda');
const CompilerApi = require('./CompilerApi');
const OrchestratorApi = require('./OrchestratorApi');
const RefreshScheduler = require('./RefreshScheduler');
const FileRepository = require('./FileRepository');
const DevServer = require('./DevServer');
const track = require('./track');
const agentCollect = require('./agentCollect');
const { version } = require('../package.json');

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
  hive: '@cubejs-backend/hive-driver',
  snowflake: '@cubejs-backend/snowflake-driver',
  prestodb: '@cubejs-backend/prestodb-driver',
  oracle: '@cubejs-backend/oracle-driver',
  sqlite: '@cubejs-backend/sqlite-driver',
  awselasticsearch: '@cubejs-backend/elasticsearch-driver',
  elasticsearch: '@cubejs-backend/elasticsearch-driver',
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

const devLogger = (level) => (type, { error, warning, ...message }) => {
  const colors = {
    red: '31', // ERROR
    green: '32', // INFO
    yellow: '33', // WARNING
  };

  const withColor = (str, color = colors.green) => `\u001b[${color}m${str}\u001b[0m`;
  const format = ({
    requestId, duration, allSqlLines, query, values, showRestParams, ...json
  }) => {
    const restParams = JSON.stringify(json, null, 2);
    const durationStr = duration ? `(${duration}ms)` : '';
    const prefix = `${requestId} ${durationStr}`;
    if (query && values) {
      const queryMaxLines = 50;
      query = query.replace(/\$(\d+)/g, '?');
      let formatted = SqlString.format(query, values).split('\n');
      if (formatted.length > queryMaxLines && !allSqlLines) {
        formatted = R.take(queryMaxLines / 2, formatted)
          .concat(['.....', '.....', '.....'])
          .concat(R.takeLast(queryMaxLines / 2, formatted));
      }
      return `${prefix}\n--\n  ${formatted.join('\n')}\n--${showRestParams ? `\n${restParams}` : ''}`;
    } else if (query) {
      return `${prefix}\n--\n${JSON.stringify(query, null, 2)}\n--${showRestParams ? `\n${restParams}` : ''}`;
    }
    return `${prefix}${showRestParams ? `\n${restParams}` : ''}`;
  };

  const logWarning = () => console.log(
    `${withColor(type, colors.yellow)}: ${format({ ...message, allSqlLines: true, showRestParams: true })} \n${withColor(warning, colors.yellow)}`
  );
  const logError = () => console.log(`${withColor(type, colors.red)}: ${format({ ...message, allSqlLines: true, showRestParams: true })} \n${error}`);
  const logDetails = () => console.log(`${withColor(type)}: ${format(message)}`);

  if (error) {
    logError();
    return;
  }

  // eslint-disable-next-line default-case
  switch ((level || 'info').toLowerCase()) {
    case "trace": {
      if (!error && !warning) {
        logDetails();
        break;
      }
    }
    // eslint-disable-next-line no-fallthrough
    case "info": {
      if (!error && !warning && [
        'Executing SQL',
        'Executing Load Pre Aggregation SQL',
        'Load Request Success',
        'Performing query',
        'Performing query completed',
      ].includes(type)) {
        logDetails();
        break;
      }
    }
    // eslint-disable-next-line no-fallthrough
    case "warn": {
      if (!error && warning) {
        logWarning();
        break;
      }
    }
    // eslint-disable-next-line no-fallthrough
    case "error": {
      if (error) {
        logError();
        break;
      }
    }
  }
};

const prodLogger = (level) => (msg, params) => {
  const { error, warning } = params;

  const logMessage = () => console.log(JSON.stringify({ message: msg, ...params }));
  // eslint-disable-next-line default-case
  switch ((level || 'warn').toLowerCase()) {
    case "trace": {
      if (!error && !warning) {
        logMessage();
        break;
      }
    }
    // eslint-disable-next-line no-fallthrough
    case "info":
      if ([
        'REST API Request',
      ].includes(msg)) {
        logMessage();
        break;
      }
    // eslint-disable-next-line no-fallthrough
    case "warn": {
      if (!error && warning) {
        logMessage();
        break;
      }
    }
    // eslint-disable-next-line no-fallthrough
    case "error": {
      if (error) {
        logMessage();
        break;
      }
    }
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
    this.logger = options.logger ||
      (process.env.NODE_ENV !== 'production' ?
        devLogger(process.env.CUBEJS_LOG_LEVEL) :
        prodLogger(process.env.CUBEJS_LOG_LEVEL)
      );
    this.repository = new FileRepository(this.schemaPath);
    this.repositoryFactory = options.repositoryFactory || (() => this.repository);
    this.contextToDbType = typeof options.dbType === 'function' ? options.dbType : () => options.dbType;
    this.contextToExternalDbType = typeof options.externalDbType === 'function' ?
      options.externalDbType :
      () => options.externalDbType;
    this.preAggregationsSchema =
      typeof options.preAggregationsSchema === 'function' ? options.preAggregationsSchema : () => options.preAggregationsSchema;
    this.compilerCache = new LRUCache({
      max: options.compilerCacheSize || 250,
      maxAge: options.maxCompilerCacheKeepAlive,
      updateAgeOnGet: options.updateCompilerCacheKeepAlive
    });
    this.dataSourceIdToOrchestratorApi = {};
    this.contextToAppId = options.contextToAppId || (() => process.env.CUBEJS_APP || 'STANDALONE');
    this.contextToDataSourceId = options.contextToDataSourceId || this.defaultContextToDataSourceId.bind(this);
    this.orchestratorOptions =
      typeof options.orchestratorOptions === 'function' ?
        options.orchestratorOptions :
        () => options.orchestratorOptions;

    // proactively free up old cache values occassionally
    if (options.maxCompilerCacheKeepAlive) {
      setInterval(() => this.compilerCache.prune(), options.maxCompilerCacheKeepAlive);
    }

    if (options.scheduledRefreshTimer) {
      setInterval(
        () => this.runScheduledRefresh(),
        typeof options.scheduledRefreshTimer === 'number' ? (options.scheduledRefreshTimer * 1000) : 5000
      );
    }

    const { machineIdSync } = require('node-machine-id');
    let anonymousId = 'unknown';
    try {
      anonymousId = machineIdSync();
    } catch (e) {
      // console.error(e);
    }
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
        await track({
          event: name,
          anonymousId,
          projectFingerprint: this.projectFingerprint,
          coreServerVersion: this.coreServerVersion,
          ...props
        });
      } catch (e) {
        // console.error(e);
      }
    };

    this.initAgent();

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
          msg === 'Compiling schema' ||
          msg === 'Recompiling schema' ||
          msg === 'Slow Query Warning'
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
      const oldLogger = this.logger;
      let loadRequestCount = 0;
      this.logger = ((msg, params) => {
        if (msg === 'Load Request Success') {
          loadRequestCount++;
        }
        oldLogger(msg, params);
      });
      setInterval(() => {
        this.event('Load Request Success Aggregated', { loadRequestSuccessCount: loadRequestCount });
        loadRequestCount = 0;
      }, 60000);
      this.event('Server Start');
    }
  }

  initAgent() {
    if (process.env.CUBEJS_AGENT_ENDPOINT_URL) {
      const oldLogger = this.logger;
      this.preAgentLogger = oldLogger;
      this.logger = (msg, params) => {
        oldLogger(msg, params);
        agentCollect(
          {
            msg,
            ...params
          },
          process.env.CUBEJS_AGENT_ENDPOINT_URL,
          oldLogger
        );
      };
    }
  }

  async flushAgent() {
    if (process.env.CUBEJS_AGENT_ENDPOINT_URL) {
      await agentCollect(
        { msg: 'Flush Agent' },
        process.env.CUBEJS_AGENT_ENDPOINT_URL,
        this.preAgentLogger
      );
    }
  }

  static create(options) {
    return new CubejsServerCore(options);
  }

  async initApp(app) {
    checkEnvForPlaceholders();
    const apiGateway = this.apiGateway();
    apiGateway.initApp(app);
    if (this.options.devServer) {
      this.devServer.initDevEnv(app);
    } else {
      app.get('/', (req, res) => {
        res.status(200)
          .send(`<html><body>Cube.js server is running in production mode. <a href="https://cube.dev/docs/deployment#production-mode">Learn more about production mode</a>.</body></html>`);
      });
    }
  }

  initSubscriptionServer(sendMessage) {
    checkEnvForPlaceholders();
    const apiGateway = this.apiGateway();
    return apiGateway.initSubscriptionServer(sendMessage);
  }

  apiGateway() {
    if (!this.apiGatewayInstance) {
      this.apiGatewayInstance = new ApiGateway(
        this.apiSecret,
        this.getCompilerApi.bind(this),
        this.getOrchestratorApi.bind(this),
        this.logger, {
          basePath: this.options.basePath,
          checkAuthMiddleware: this.options.checkAuthMiddleware,
          checkAuth: this.options.checkAuth,
          queryTransformer: this.options.queryTransformer,
          extendContext: this.options.extendContext,
          refreshScheduler: () => new RefreshScheduler(this)
        }
      );
    }
    return this.apiGatewayInstance;
  }

  getCompilerApi(context) {
    const appId = this.contextToAppId(context);
    let compilerApi = this.compilerCache.get(appId);
    const currentSchemaVersion = this.options.schemaVersion && (() => this.options.schemaVersion(context));
    if (!compilerApi) {
      compilerApi = this.createCompilerApi(
        this.repositoryFactory(context), {
          dbType: (dataSourceContext) => this.contextToDbType({ ...context, ...dataSourceContext }),
          externalDbType: this.contextToExternalDbType(context),
          schemaVersion: currentSchemaVersion,
          preAggregationsSchema: this.preAggregationsSchema(context),
          context
        }
      );
      this.compilerCache.set(appId, compilerApi);
    }

    compilerApi.schemaVersion = currentSchemaVersion;
    return compilerApi;
  }

  defaultContextToDataSourceId(context) {
    return `${this.contextToAppId(context)}_${context.dataSource}`;
  }

  getOrchestratorApi(context) {
    const dataSourceId = this.contextToDataSourceId(context);
    if (!this.dataSourceIdToOrchestratorApi[dataSourceId]) {
      let driverPromise;
      let externalPreAggregationsDriverPromise;
      this.dataSourceIdToOrchestratorApi[dataSourceId] = this.createOrchestratorApi({
        getDriver: async () => {
          if (!driverPromise) {
            const driver = await this.driverFactory(context);
            driverPromise = driver.testConnection().then(() => driver).catch(e => {
              driverPromise = null;
              throw e;
            });
          }
          return driverPromise;
        },
        getExternalDriverFactory: this.externalDriverFactory && (async () => {
          if (!externalPreAggregationsDriverPromise) {
            const driver = await this.externalDriverFactory(context);
            externalPreAggregationsDriverPromise = driver.testConnection().then(() => driver).catch(e => {
              externalPreAggregationsDriverPromise = null;
              throw e;
            });
          }
          return externalPreAggregationsDriverPromise;
        }),
        redisPrefix: dataSourceId,
        orchestratorOptions: this.orchestratorOptions(context)
      });
    }
    return this.dataSourceIdToOrchestratorApi[dataSourceId];
  }

  createCompilerApi(repository, options) {
    options = options || {};
    return new CompilerApi(repository, options.dbType || this.dbType, {
      schemaVersion: options.schemaVersion || this.options.schemaVersion,
      devServer: this.options.devServer,
      logger: this.logger,
      externalDbType: options.externalDbType,
      preAggregationsSchema: options.preAggregationsSchema,
      allowUngroupedWithoutPrimaryKey: this.options.allowUngroupedWithoutPrimaryKey,
      compileContext: options.context
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

  async runScheduledRefresh(context, queryingOptions) {
    const scheduler = new RefreshScheduler(this);
    return scheduler.runScheduledRefresh(context, queryingOptions);
  }

  async getDriver() {
    if (!this.driver) {
      const driver = this.driverFactory({});
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

  testConnections() {
    const tests = [];
    Object.keys(this.dataSourceIdToOrchestratorApi).forEach(dataSourceId => {
      const orchestratorApi = this.dataSourceIdToOrchestratorApi[dataSourceId];
      tests.push(orchestratorApi.testConnection());
    });
    return Promise.all(tests);
  }

  async releaseConnections() {
    const releases = [];
    Object.keys(this.dataSourceIdToOrchestratorApi).forEach(dataSourceId => {
      const orchestratorApi = this.dataSourceIdToOrchestratorApi[dataSourceId];
      releases.push(orchestratorApi.release());
    });
    await Promise.all(releases);
    this.dataSourceIdToOrchestratorApi = {};
  }

  static version() {
    return version;
  }
}

module.exports = CubejsServerCore;
