/* eslint-disable global-require */
require('dotenv').config();
const CubejsServerCore = require('@cubejs-backend/server-core');

class CubejsServer {
  constructor(config) {
    config = config || {};
    this.core = CubejsServerCore.create(config);
    this.redirector = null;
    this.server = null;
  }

  async listen(options = {}) {
    try {
      if (this.server) {
        throw new Error("CubeServer is already listening");
      }

      const http = require("http");
      const https = require("https");
      const util = require("util");
      const express = require("express");
      const app = express();
      const bodyParser = require("body-parser");
      app.use(require("cors")());
      app.use(bodyParser.json({ limit: "50mb" }));

      await this.core.initApp(app);

      return new Promise((resolve, reject) => {
        const PORT = process.env.PORT || 4000;
        const TLS_PORT = process.env.TLS_PORT || 4433;

        if (process.env.CUBEJS_ENABLE_TLS === "true") {
          this.redirector = http.createServer((req, res) => {
            res.writeHead(301, {
              Location: `https://${req.headers.host}:${TLS_PORT}${req.url}`
            });
            res.end();
          });
          this.redirector.listen(PORT);
          this.server = Object.keys(options).length ? https.createServer(options, app) : https.createServer(app);
          this.server.listen(TLS_PORT, err => {
            if (err) {
              this.server = null;
              this.redirector = null;
              reject(err);
              return;
            }
            this.redirector.close = util.promisify(this.redirector.close);
            this.server.close = util.promisify(this.server.close);
            resolve({
              app, port: PORT, tlsPort: TLS_PORT, server: this.server
            });
          });
        } else {
          this.server = Object.keys(options).length ? http.createServer(options, app) : http.createServer(app);
          this.server.listen(PORT, err => {
            if (err) {
              this.server = null;
              this.redirector = null;
              reject(err);
              return;
            }
            resolve({ app, port: PORT, server: this.server });
          });
        }
      });
    } catch (e) {
      if (this.core.event) {
        await this.core.event("Dev Server Fatal Error", {
          error: (e.stack || e.message || e).toString()
        });
      }
      throw e;
    }
  }

  async close() {
    try {
      if (!this.server) {
        throw new Error("CubeServer is not started.");
      }
      await this.server.close();
      this.server = null;
      if (this.redirector) {
        await this.redirector.close();
        this.redirector = null;
      }
    } catch (e) {
      if (this.core.event) {
        await this.core.event("Dev Server Fatal Error", {
          error: (e.stack || e.message || e).toString()
        });
      }
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
