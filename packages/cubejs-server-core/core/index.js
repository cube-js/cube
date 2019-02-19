const ApiGateway = require('@cubejs-backend/api-gateway');
const CompilerApi = require('./CompilerApi');
const OrchestratorApi = require('./OrchestratorApi');
const FileRepository = require('./FileRepository');

const DriverDependencies = {
  postgres: '@cubejs-backend/postgres-driver',
  mysql: '@cubejs-backend/mysql-driver',
  athena: '@cubejs-backend/athena-driver',
  jdbc: '@cubejs-backend/jdbc-driver',
};

const devServerIndexJs = (localUrl, cubejsToken) => `import React from "react";
import ReactDOM from "react-dom";

import cubejs from "@cubejs-client/core";
import { QueryRenderer } from "@cubejs-client/react";
import { Chart, Axis, Tooltip, Geom, Coord, Legend } from "bizcharts";

const API_URL = "${localUrl}"; // change to your actual endpoint

const renderChart = resultSet => (
  <Chart height={400} data={resultSet.chartPivot()} forceFit>
    <Coord type="theta" radius={0.75} />
    <Axis name="Orders.count" />
    <Legend position="right" name="category" />
    <Tooltip showTitle={false} />
    <Geom type="intervalStack" position="Orders.count" color="x" />
  </Chart>
);

const query = {
  measures: ["Orders.count"],
  dimensions: ["Orders.status"]
};

const cubejsApi = cubejs(
  "${cubejsToken}",
  { apiUrl: API_URL + "/cubejs-api/v1" }
);

const App = () => (
  <div style={{ textAlign: 'center', fontFamily: 'sans-serif' }}>
    <h1>Order by status example</h1>
    <QueryRenderer
      query={query}
      cubejsApi={cubejsApi}
      render={({ resultSet, error }) =>
        (resultSet && renderChart(resultSet)) ||
        (error && error.toString()) || <span>Loading...</span>
      }
    />
  </div>
);

const rootElement = document.getElementById("root");
ReactDOM.render(<App />, rootElement);
`;

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
    this.logger = options.logger || ((msg, params) => { console.log(`${msg}: ${JSON.stringify(params)}`)});
    this.repository = new FileRepository(this.schemaPath);

    if (process.env.NODE_ENV !== 'production') {
      const Analytics = require('analytics-node');
      const client = new Analytics('dSR8JiNYIGKyQHKid9OaLYugXLao18hA', { flushInterval: 100 });
      const { machineIdSync } = require('node-machine-id');
      const { promisify } = require('util');
      const anonymousId = machineIdSync();
      this.event = async (name, props) => {
        try {
          await promisify(client.track.bind(client))({
            event: name,
            anonymousId: anonymousId,
            properties: props
          });
          await promisify(client.flush.bind(client))()
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
            msg === 'User Error'
          ) {
            this.event(msg, { error: params.error });
          }
          console.log(`${msg}: ${JSON.stringify(params)}`)
        });
      }
      let causeErrorPromise;
      process.on('uncaughtException', async (e) => {
        console.error(e.stack || e);
        if (e.message && e.message.indexOf('Redis connection to') !== -1) {
          console.log('🛑 Cube.js Server requires locally running Redis instance to connect to');
        }
        if (!causeErrorPromise) {
          causeErrorPromise = this.event('Dev Server Fatal Error', {
            error: (e.stack || e.message || e).toString()
          });
        }
        await causeErrorPromise;
        process.exit(1);
      })
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
      this.logger
    );
    apiGateway.initApp(app);
    this.initDevEnv(app);
  }

  initDevEnv(app) {
    if (process.env.NODE_ENV !== 'production') {
      const port = process.env.PORT || 4000; // TODO
      const localUrl = process.env.CUBEJS_API_URL || `http://localhost:${port}`;
      const jwt = require('jsonwebtoken');
      let cubejsToken = jwt.sign({}, this.apiSecret, { expiresIn: '1d' });
      console.log(`🔒 Your temporary cube.js token: ${cubejsToken}`);
      console.log(`🦅 Dev environment available at ${localUrl}`);
      this.event('Dev Server Start');
      app.get('/', (req, res) => {
        this.event('Dev Server Env Open');
        const { getParameters } = require('codesandbox-import-utils/lib/api/define');

        const parameters = getParameters({
          files: {
            'index.js': {
              content: devServerIndexJs(localUrl, cubejsToken),
            },
            'package.json': {
              content: {
                dependencies: {
                  '@cubejs-client/core': 'latest',
                  '@cubejs-client/react': 'latest',
                  'bizcharts': 'latest',
                  'react': 'latest',
                  'react-dom': 'latest'
                }
              },
            },
          },
          template: 'create-react-app'
        });

        res.redirect(`https://codesandbox.io/api/v1/sandboxes/define?parameters=${parameters}`);
      })
    }
  }

  createCompilerApi(repository) {
    return new CompilerApi(repository, this.dbType);
  }

  createOrchestratorApi() {
    return new OrchestratorApi(() => this.getDriver(), this.logger, this.options.orchestratorOptions);
  }

  async getDriver() {
    if (!this.driver) {
      const driver = this.driverFactory();
      await driver.testConnection(); //TODO mutex
      this.driver = driver;
    }
    return this.driver;
  }

  static createDriver(dbType) {
    checkEnvForPlaceholders();
    return new (require(CubejsServerCore.driverDependencies(dbType || process.env.CUBEJS_DB_TYPE)))();
  }

  static driverDependencies(dbType) {
    return DriverDependencies[dbType] || DriverDependencies.jdbc
  }
}

module.exports = CubejsServerCore;