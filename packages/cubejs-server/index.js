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

class CubejsServer {
  constructor(config) {
    config = config || {};
    config = {
      driverFactory: () => new (require(CubejsServer.driverDependencies(config.dbType || process.env.CUBEJS_DB_TYPE)))(),
      apiSecret: process.env.CUBEJS_API_SECRET,
      dbType: process.env.CUBEJS_DB_TYPE,
      ...config
    };
    if (
      process.env.CUBEJS_DB_HOST.indexOf('<YOUR_DB_') === 0 ||
      process.env.CUBEJS_DB_NAME.indexOf('<YOUR_DB_') === 0 ||
      process.env.CUBEJS_DB_USER.indexOf('<YOUR_DB_') === 0 ||
      process.env.CUBEJS_DB_PASS.indexOf('<YOUR_DB_') === 0
    ) {
      throw new Error('Your .env file contains placeholders in DB credentials. Please replace them with your DB credentials.');
    }
    this.core = CubejsServerCore.create(config);
  }

  async listen() {
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
      app.get('/', (req, res) => {
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
  }

  static driverDependencies(dbType) {
    return DriverDependencies[dbType] || DriverDependencies.jdbc
  }
}

module.exports = CubejsServer;