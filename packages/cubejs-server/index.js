require('dotenv').config();
const CubejsServerCore = require('@cubejs-backend/server-core');

const DriverDependencies = {
  postgres: '@cubejs-backend/postgres-driver',
  jdbc: '@cubejs-backend/jdbc-driver',
};

class CubejsServer {
  constructor(config) {
    config = config || {};
    this.core = CubejsServerCore.create({
      driverFactory: () => new (require(CubejsServer.driverDependencies(config.dbType || process.env.CUBEJS_DB_TYPE)))(),
      apiSecret: process.env.CUBEJS_API_SECRET,
      dbType: process.env.CUBEJS_DB_TYPE,
      ...config
    });
  }

  async listen() {
    const express = require('express');
    const app = express();
    const bodyParser = require('body-parser');
    app.use(require('cors')());
    app.use(bodyParser.json({ limit: '50mb' }));

    await this.core.initApp(app);

    return new Promise((resolve, reject) => {
      const port = process.env.PORT || 4000;
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