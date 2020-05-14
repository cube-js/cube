/* eslint-disable global-require */
require('dotenv').config();
const CubejsServerCore = require('@cubejs-backend/server-core');
const WebSocketServer = require('./WebSocketServer');
const { version } = require('./package.json');

class CubejsServer {
  constructor(config) {
    config = config || {};
    config.webSockets = config.webSockets || (process.env.CUBEJS_WEB_SOCKETS === 'true');
    this.core = CubejsServerCore.create(config);
    this.webSockets = config.webSockets;
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

        const enableTls = process.env.CUBEJS_ENABLE_TLS === "true";
        if (enableTls) {
          this.redirector = http.createServer((req, res) => {
            res.writeHead(301, {
              Location: `https://${req.headers.host.split(':')[0]}:${TLS_PORT}${req.url}`
            });
            res.end();
          });
          this.redirector.listen(PORT);
        }

        const httpOrHttps = enableTls ? https : http;
        this.server = Object.keys(options).length ?
          httpOrHttps.createServer(options, app) : httpOrHttps.createServer(app);

        if (this.webSockets) {
          this.socketServer = new WebSocketServer(this.core, this.core.options);
          this.socketServer.initServer(this.server);
        }

        this.server.listen(enableTls ? TLS_PORT : PORT, err => {
          if (err) {
            this.server = null;
            this.redirector = null;
            reject(err);
            return;
          }
          if (this.redirector) {
            this.redirector.close = util.promisify(this.redirector.close);
          }
          this.server.close = util.promisify(this.server.close);
          resolve({
            app,
            port: PORT,
            tlsPort: process.env.CUBEJS_ENABLE_TLS === "true" ? TLS_PORT : undefined,
            server: this.server,
            version
          });
        });
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

  testConnections() {
    return this.core.testConnections();
  }

  runScheduledRefresh(context, queryingOptions) {
    return this.core.runScheduledRefresh(context, queryingOptions);
  }

  async close() {
    try {
      if (this.socketServer) {
        await this.socketServer.close();
      }
      if (!this.server) {
        throw new Error("CubeServer is not started.");
      }
      await this.server.close();
      this.server = null;
      if (this.redirector) {
        await this.redirector.close();
        this.redirector = null;
      }
      await this.core.releaseConnections();
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

  static version() {
    return version;
  }
}

module.exports = CubejsServer;
