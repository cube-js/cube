require('dotenv').config();
const CubejsServerCore = require('@cubejs-backend/server-core');

class CubejsServer {
  constructor(config) {
    config = config || {};
    this.core = CubejsServerCore.create(config);
  }

  async listen() {
    try {
      const express = require('express');
      const app = express();
      const bodyParser = require('body-parser');
      app.use(require('cors')());
      app.use(bodyParser.json({ limit: '50mb' }));

      await this.core.initApp(app);
      const port = process.env.PORT || 4000;

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
      this.core.event && (await this.core.event('Dev Server Fatal Error', {
        error: (e.stack || e.message || e).toString()
      }));
      throw e;
    }
  }

  static createDriver(dbType) {
    return CubejsServerCore.createDriver(dbType);
  }

  static driverDependencies(dbType) {
    return CubejsServerCore.driverDependencies(dbType);
  }

  static apiSecret() {
    return process.env.CUBEJS_API_SECRET;
  }
}

module.exports = CubejsServer;
