require('dotenv').config();
const CubejsServerCore = require('@cubejs-backend/server-core');

const DriverDependencies = {
  postgres: '@cubejs-backend/postgres-driver',
  jdbc: '@cubejs-backend/jdbc-driver',
};

const devServerIndexJs = (localUrl, cubejsToken) => `import React from "react";
import ReactDOM from "react-dom";

import cubejs from "@cubejs-client/core";
import { QueryRenderer } from "@cubejs-client/react";
import { Chart, Axis, Tooltip, Geom, Coord, Legend } from "bizcharts";

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
  { apiUrl: "${localUrl}/cubejs-api/v1" }
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
}

class CubejsServer {
  constructor(config) {
    config = config || {};
    config = {
      driverFactory: () => CubejsServer.createDriver(config.dbType),
      apiSecret: process.env.CUBEJS_API_SECRET,
      dbType: process.env.CUBEJS_DB_TYPE,
      ...config
    };
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
          console.error(e);
        }
      };
      if (!config.logger) {
        config.logger = ((msg, params) => {
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
    this.core = CubejsServerCore.create(config);
  }

  async listen() {
    try {
      checkEnvForPlaceholders();
      const express = require('express');
      const app = express();
      const bodyParser = require('body-parser');
      app.use(require('cors')());
      app.use(bodyParser.json({ limit: '50mb' }));

      await this.core.initApp(app);
      const port = process.env.PORT || 4000;

      if (process.env.NODE_ENV !== 'production') {
        const localUrl = `http://localhost:${port}`;
        const jwt = require('jsonwebtoken');
        let cubejsToken = jwt.sign({}, this.core.apiSecret, { expiresIn: '1d' });
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
                    '@cubejs-client/core': '0.2.2',
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

      return new Promise((resolve, reject) => {
        app.listen(port, (err) => {
          if (err) {
            reject(err);
            return;
          }
          resolve({ app, port });
        });
      })
    } catch (e) {
      await this.event('Dev Server Fatal Error', {
        error: (e.stack || e.message || e).toString()
      });
      throw e;
    }
  }

  static createDriver(dbType) {
    return new (require(CubejsServer.driverDependencies(dbType || process.env.CUBEJS_DB_TYPE)))();
  }

  static driverDependencies(dbType) {
    return DriverDependencies[dbType] || DriverDependencies.jdbc
  }
}

module.exports = CubejsServer;
