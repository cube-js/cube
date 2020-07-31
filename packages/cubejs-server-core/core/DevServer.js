/* eslint-disable global-require */
// Playground version: 0.19.31
const fs = require('fs-extra');
const path = require('path');
const spawn = require('cross-spawn');
const AppContainer = require('../dev/AppContainer');
const DependencyTree = require('../dev/DependencyTree');
const PackageFetcher = require('../dev/PackageFetcher');

const repo = {
  owner: 'cube-js',
  name: 'cubejs-playground-templates'
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
        console.error((e.stack || e).toString());
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
        coreServerVersion: this.cubejsServer.coreServerVersion,
        projectFingerprint: this.cubejsServer.projectFingerprint
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
      res.json({
        files: files.map(f => ({
          ...f,
          absPath: path.resolve(path.join(this.cubejsServer.repository.localPath(), f.fileName))
        }))
      });
    }));

    app.post('/playground/generate-schema', catchErrors(async (req, res) => {
      this.cubejsServer.event('Dev Server Generate Schema');
      if (!req.body) {
        throw new Error(`Your express app config is missing body-parser middleware. Typical config can look like: \`app.use(bodyParser.json({ limit: '50mb' }));\``);
      }
      const driver = await this.cubejsServer.getDriver();
      const tablesSchema = req.body.tablesSchema || (await driver.tablesSchema());

      const ScaffoldingTemplate = require('@cubejs-backend/schema-compiler/scaffolding/ScaffoldingTemplate');
      const scaffoldingTemplate = new ScaffoldingTemplate(tablesSchema, driver);
      const files = scaffoldingTemplate.generateFilesByTableNames(req.body.tables);

      const schemaPath = this.cubejsServer.options.schemaPath || 'schema';

      await Promise.all(files.map(file => fs.writeFile(path.join(schemaPath, file.fileName), file.content)));
      res.json({ files });
    }));

    const dashboardAppPath = this.cubejsServer.options.dashboardAppPath || 'dashboard-app';

    let lastApplyTemplatePackagesError = null;

    app.get('/playground/dashboard-app-create-status', catchErrors(async (req, res) => {
      const sourcePath = path.join(dashboardAppPath, 'src');
      
      if (lastApplyTemplatePackagesError) {
        const toThrow = lastApplyTemplatePackagesError;
        lastApplyTemplatePackagesError = null;
        throw toThrow;
      }

      if (this.applyTemplatePackagesPromise) {
        if (req.query.instant) {
          res.status(404).json({ error: 'Dashboard app creating' });
          return;
        }
        await this.applyTemplatePackagesPromise;
      }

      if (!(fs.pathExistsSync(sourcePath))) {
        res.status(404).json({
          error: fs.pathExistsSync(dashboardAppPath) ?
            `Dashboard app corrupted. Please remove '${path.resolve(dashboardAppPath)}' directory and recreate it` :
            `Dashboard app not found in '${path.resolve(dashboardAppPath)}' directory`
        });
        return;
      }

      if (!(await fs.pathExists(sourcePath))) {
        res.status(404).json({ error: 'Dashboard app not found' });
        return;
      }

      res.json({
        status: 'created',
        installedTemplates: AppContainer.getPackageVersions(dashboardAppPath)
      });
    }));

    const dashboardAppPort = this.cubejsServer.options.dashboardAppPort || 3000;

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
        dashboardPort,
        dashboardAppPath: path.resolve(dashboardAppPath)
      });
    }));

    app.post('/playground/apply-template-packages', catchErrors(async (req, res) => {
      this.cubejsServer.event('Dev Server Download Template Packages');
      
      const fetcher = new PackageFetcher(repo);

      this.cubejsServer.event('Dev Server App File Write');
      const { toApply, templateConfig } = req.body;
      
      const applyTemplates = async () => {
        const manifestJson = await fetcher.manifestJSON();
        const response = await fetcher.downloadPackages();
        
        let templatePackages = [];
        if (typeof toApply === 'string') {
          const template = manifestJson.templates.find(({ name }) => name === toApply);
          templatePackages = template.templatePackages;
        } else {
          templatePackages = toApply;
        }
        
        const dt = new DependencyTree(manifestJson, templatePackages);
        
        const appContainer = new AppContainer(
          dt.getRootNode(),
          {
            appPath: dashboardAppPath,
            packagesPath: response.packagesPath
          },
          templateConfig
        );
        
        this.cubejsServer.event('Dev Server Create Dashboard App');
        await appContainer.applyTemplates();
        this.cubejsServer.event('Dev Server Create Dashboard App Success');

        this.cubejsServer.event('Dev Server Dashboard Npm Install');
        
        await appContainer.ensureDependencies();
        this.cubejsServer.event('Dev Server Dashboard Npm Install Success');
        
        fetcher.cleanup();
      };
      
      if (this.applyTemplatePackagesPromise) {
        this.applyTemplatePackagesPromise = this.applyTemplatePackagesPromise.then(applyTemplates);
      } else {
        this.applyTemplatePackagesPromise = applyTemplates();
      }
      const promise = this.applyTemplatePackagesPromise;
      
      promise.then(() => {
        if (promise === this.applyTemplatePackagesPromise) {
          this.applyTemplatePackagesPromise = null;
        }
      }, (err) => {
        lastApplyTemplatePackagesError = err;
        if (promise === this.applyTemplatePackagesPromise) {
          this.applyTemplatePackagesPromise = null;
        }
      });
      res.json(true); // TODO
    }));
    
    app.get('/playground/manifest', catchErrors(async (_, res) => {
      const fetcher = new PackageFetcher(repo);
      res.json(await fetcher.manifestJSON());
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
