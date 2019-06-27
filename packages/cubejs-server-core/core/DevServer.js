/* eslint-disable global-require */
// Playground version: 0.7.2
const fs = require('fs-extra');
const path = require('path');
const spawn = require('cross-spawn');

const executeCommand = (command, args, options) => {
  const child = spawn(command, args, { stdio: 'inherit', ...options });
  return new Promise((resolve, reject) => {
    child.on('close', code => {
      if (code !== 0) {
        reject(new Error(`${command} ${args.join(' ')} failed with exit code ${code}. Please check your console.`));
        return;
      }
      resolve();
    });
  });
};

class DevServer {
  constructor(cubejsServer) {
    this.cubejsServer = cubejsServer;
  }

  initDevEnv(app) {
    const port = process.env.PORT || 4000; // TODO
    const apiUrl = process.env.CUBEJS_API_URL || `http://localhost:${port}`;
    const jwt = require('jsonwebtoken');
    const cubejsToken = jwt.sign({}, this.cubejsServer.apiSecret, { expiresIn: '1d' });
    if (process.env.NODE_ENV !== 'production') {
      console.log(`🔓 Authentication checks are disabled in developer mode. Please use NODE_ENV=production to enable it.`);
    } else {
      console.log(`🔒 Your temporary cube.js token: ${cubejsToken}`);
    }
    console.log(`🦅 Dev environment available at ${apiUrl}`);
    this.cubejsServer.event('Dev Server Start');
    const serveStatic = require('serve-static');

    const catchErrors = (handler) => async (req, res, next) => {
      try {
        await handler(req, res, next);
      } catch (e) {
        this.cubejsServer.event('Dev Server Error', { error: (e.stack || e).toString() });
        res.status(500).json({ error: (e.stack || e).toString() });
      }
    };

    app.get('/playground/context', catchErrors((req, res) => {
      this.cubejsServer.event('Dev Server Env Open');
      res.json({
        cubejsToken: jwt.sign({}, this.cubejsServer.apiSecret, { expiresIn: '1d' }),
        apiUrl: process.env.CUBEJS_API_URL,
        anonymousId: this.cubejsServer.anonymousId,
        coreServerVersion: this.cubejsServer.coreServerVersion
      });
    }));

    app.get('/playground/db-schema', catchErrors(async (req, res) => {
      this.cubejsServer.event('Dev Server DB Schema Load');
      const driver = await this.cubejsServer.getDriver();
      const tablesSchema = await driver.tablesSchema();
      this.cubejsServer.event('Dev Server DB Schema Load Success');
      if (Object.keys(tablesSchema || {}).length === 0) {
        this.cubejsServer.event('Dev Server DB Schema Load Empty');
      }
      res.json({ tablesSchema });
    }));

    app.get('/playground/files', catchErrors(async (req, res) => {
      this.cubejsServer.event('Dev Server Files Load');
      const files = await this.cubejsServer.repository.dataSchemaFiles();
      res.json({ files });
    }));

    app.post('/playground/generate-schema', catchErrors(async (req, res) => {
      this.cubejsServer.event('Dev Server Generate Schema');
      const driver = await this.cubejsServer.getDriver();
      const tablesSchema = await driver.tablesSchema();

      const ScaffoldingTemplate = require('@cubejs-backend/schema-compiler/scaffolding/ScaffoldingTemplate');
      const scaffoldingTemplate = new ScaffoldingTemplate(tablesSchema, driver);
      const files = scaffoldingTemplate.generateFilesByTableNames(req.body.tables);
      await Promise.all(files.map(file => fs.writeFile(path.join('schema', file.fileName), file.content)));
      res.json({ files });
    }));

    const dashboardAppPath = this.cubejsServer.options.dashboardAppPath || 'dashboard-app';

    app.get('/playground/ensure-dashboard-app', catchErrors(async (req, res) => {
      this.cubejsServer.event('Dev Server Ensure Dashboard App');
      if (!await fs.pathExists(dashboardAppPath) || this.createReactAppInit) {
        if (!this.createReactAppInit) {
          this.cubejsServer.event('Dev Server Create Dashboard App');
          this.createReactAppInit = executeCommand('npx', ['create-react-app', dashboardAppPath]).catch(e => {
            if (e.toString().indexOf('ENOENT') !== -1) {
              throw new Error(`npx is not installed. Please update your npm: \`$ npm install -g npm\`.`);
            }
            throw e;
          });
        }
        await this.createReactAppInit;
        this.cubejsServer.event('Dev Server Create Dashboard App Success');
        this.createReactAppInit = null;
      }

      res.json();
    }));

    app.get('/playground/dashboard-app-files', catchErrors(async (req, res) => {
      const sourcePath = await path.join(dashboardAppPath, 'src');

      if (this.createReactAppInit) {
        await this.createReactAppInit;
      }

      if (!(await fs.pathExists(sourcePath))) {
        res.status(404).json({
          error: await fs.pathExists(dashboardAppPath) ?
            `Dashboard app corrupted. Please remove '${dashboardAppPath}' directory and recreate it` :
            `Dashboard app not found in '${dashboardAppPath}' directory`
        });
        return;
      }

      if (!(await fs.pathExists(sourcePath))) {
        res.status(404).json({ error: 'Dashboard app not found' });
        return;
      }

      const files = await fs.readdir(sourcePath);
      const fileContents = (await Promise.all(files
        .map(async file => {
          const fileName = path.join(sourcePath, file);
          const stats = await fs.lstat(fileName);
          if (!stats.isDirectory()) {
            const content = await fs.readFile(fileName, "utf-8");
            return [{
              fileName: fileName.replace(dashboardAppPath, '').replace(/\\/g, '/'),
              content
            }];
          }
          return [];
        }))).reduce((a, b) => a.concat(b), []);

      res.json({ fileContents });
    }));

    app.post('/playground/dashboard-app-files', catchErrors(async (req, res) => {
      this.cubejsServer.event('Dev Server App File Write');
      const { files } = req.body;
      await Promise.all(
        files.map(file => fs.writeFile(
          path.join(...[dashboardAppPath].concat(file.fileName.split('/'))),
          file.content
        ))
      );
      res.json({ files });
    }));

    app.post('/playground/ensure-dependencies', catchErrors(async (req, res) => {
      this.cubejsServer.event('Dev Server App Ensure Dependencies');
      const { dependencies } = req.body;
      const cmd = async () => {
        const packageJson = await fs.readJson(path.join(dashboardAppPath, 'package.json'));
        const toInstall = Object.keys(dependencies).filter(dependency => !packageJson.dependencies[dependency]);
        if (toInstall.length) {
          this.cubejsServer.event('Dev Server Dashboard Npm Install');
          await executeCommand(
            'npm',
            ['install', '--save'].concat(toInstall),
            { cwd: path.resolve(dashboardAppPath) }
          );
          await executeCommand(
            'npm',
            ['install'],
            { cwd: path.resolve(dashboardAppPath) }
          );
          this.cubejsServer.event('Dev Server Dashboard Npm Install Success');
        }
        return toInstall;
      };
      if (this.curNpmInstall) {
        this.curNpmInstall = this.curNpmInstall.then(cmd);
      } else {
        this.curNpmInstall = cmd();
      }
      const { curNpmInstall } = this;
      const toInstall = await this.curNpmInstall;
      if (curNpmInstall === this.curNpmInstall) {
        this.curNpmInstall = null;
      }
      res.json({ toInstall });
    }));

    const dashboardAppPort = this.cubejsServer.options.dashboardAppPort || 3050;

    app.get('/playground/start-dashboard-app', catchErrors(async (req, res) => {
      this.cubejsServer.event('Dev Server Start Dashboard App');
      if (!this.dashboardAppProcess) {
        this.dashboardAppProcess = spawn('npm', ['run', 'start'], {
          cwd: dashboardAppPath,
          env: {
            ...process.env,
            PORT: dashboardAppPort
          }
        });
        this.dashboardAppProcess.dashboardUrlPromise = new Promise((resolve) => {
          this.dashboardAppProcess.stdout.on('data', (data) => {
            console.log(data.toString());
            if (data.toString().match(/Compiled/)) {
              resolve(dashboardAppPort);
            }
          });
        });

        this.dashboardAppProcess.on('close', exitCode => {
          if (exitCode !== 0) {
            console.log(`Dashboard react-app failed with exit code ${exitCode}`);
            this.cubejsServer.event('Dev Server Dashboard App Failed', { exitCode });
          }
          this.dashboardAppProcess = null;
        });
      }

      await this.dashboardAppProcess.dashboardUrlPromise;
      res.json({ dashboardPort: dashboardAppPort });
    }));

    app.get('/playground/dashboard-app-status', catchErrors(async (req, res) => {
      this.cubejsServer.event('Dev Server Dashboard App Status');
      const dashboardPort = this.dashboardAppProcess && await this.dashboardAppProcess.dashboardUrlPromise;
      res.json({
        running: !!dashboardPort,
        dashboardPort
      });
    }));

    app.use(serveStatic(path.join(__dirname, '../playground'), {
      lastModified: false,
      etag: false,
      setHeaders: (res, url) => {
        if (url.indexOf('/index.html') !== -1) {
          res.setHeader('Cache-Control', 'no-cache');
        }
      }
    }));
  }
}

module.exports = DevServer;
