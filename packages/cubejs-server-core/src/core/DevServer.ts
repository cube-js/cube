/* eslint-disable global-require */
// Playground version: 0.19.31
import type { ChildProcess } from 'child_process';
import spawn from 'cross-spawn';
import path from 'path';
import fs from 'fs-extra';
import crypto from 'crypto';
import type { Application as ExpressApplication } from 'express';

import { CubejsServerCore, ServerCoreInitializedOptions } from './server';
import AppContainer from '../dev/AppContainer';
import DependencyTree from '../dev/DependencyTree';
import PackageFetcher from '../dev/PackageFetcher';
import DevPackageFetcher from '../dev/DevPackageFetcher';

const repo = {
  owner: 'cube-js',
  name: 'cubejs-playground-templates'
};

export class DevServer {
  protected applyTemplatePackagesPromise: Promise<any>|null = null;

  protected dashboardAppProcess: ChildProcess & { dashboardUrlPromise?: Promise<any> }|null = null;

  public constructor(
    protected readonly cubejsServer: CubejsServerCore,
  ) {
  }

  public initDevEnv(app: ExpressApplication, options: ServerCoreInitializedOptions) {
    const jwt = require('jsonwebtoken');
    const port = process.env.PORT || 4000; // TODO
    const apiUrl = process.env.CUBEJS_API_URL || `http://localhost:${port}`;

    // todo: empty/default `apiSecret` in dev mode to allow the DB connection wizard
    const cubejsToken = jwt.sign({}, options.apiSecret || 'secret', { expiresIn: '1d' });

    if (process.env.NODE_ENV !== 'production') {
      console.log('🔓 Authentication checks are disabled in developer mode. Please use NODE_ENV=production to enable it.');
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
        cubejsToken,
        apiUrl: process.env.CUBEJS_API_URL,
        basePath: options.basePath,
        anonymousId: this.cubejsServer.anonymousId,
        coreServerVersion: this.cubejsServer.coreServerVersion,
        projectFingerprint: this.cubejsServer.projectFingerprint,
        shouldStartConnectionWizardFlow: !this.cubejsServer.configFileExists()
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
        throw new Error('Your express app config is missing body-parser middleware. Typical config can look like: `app.use(bodyParser.json({ limit: \'50mb\' }));`');
      }

      if (!req.body.tables) {
        throw new Error('You have to select at least one table');
      }

      const driver = await this.cubejsServer.getDriver();
      const tablesSchema = req.body.tablesSchema || (await driver.tablesSchema());

      const ScaffoldingTemplate = require('@cubejs-backend/schema-compiler/scaffolding/ScaffoldingTemplate');
      const scaffoldingTemplate = new ScaffoldingTemplate(tablesSchema, driver);
      const files = scaffoldingTemplate.generateFilesByTableNames(req.body.tables);

      const schemaPath = options.schemaPath || 'schema';

      await Promise.all(files.map(file => fs.writeFile(path.join(schemaPath, file.fileName), file.content)));
      res.json({ files });
    }));

    let lastApplyTemplatePackagesError = null;

    app.get('/playground/dashboard-app-create-status', catchErrors(async (req, res) => {
      const sourcePath = path.join(options.dashboardAppPath, 'src');

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

      // docker-compose share a volume for /dashboard-app and directory will be empty
      if (!fs.pathExistsSync(options.dashboardAppPath) || fs.readdirSync(options.dashboardAppPath).length === 0) {
        res.status(404).json({
          error: `Dashboard app not found in '${path.resolve(options.dashboardAppPath)}' directory`
        });

        return;
      }

      if (!fs.pathExistsSync(sourcePath)) {
        res.status(404).json({
          error: `Dashboard app corrupted. Please remove '${path.resolve(options.dashboardAppPath)}' directory and recreate it`
        });

        return;
      }

      res.json({
        status: 'created',
        installedTemplates: AppContainer.getPackageVersions(options.dashboardAppPath)
      });
    }));

    app.get('/playground/start-dashboard-app', catchErrors(async (req, res) => {
      this.cubejsServer.event('Dev Server Start Dashboard App');

      if (!this.dashboardAppProcess) {
        this.dashboardAppProcess = spawn('npm', ['run', 'start'], {
          cwd: options.dashboardAppPath,
          env: <any>{
            ...process.env,
            PORT: options.dashboardAppPort
          }
        });

        this.dashboardAppProcess.dashboardUrlPromise = new Promise((resolve) => {
          this.dashboardAppProcess.stdout.on('data', (data) => {
            console.log(data.toString());
            if (data.toString().match(/Compiled/)) {
              resolve(options.dashboardAppPort);
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
      res.json({ dashboardPort: options.dashboardAppPort });
    }));

    app.get('/playground/dashboard-app-status', catchErrors(async (req, res) => {
      this.cubejsServer.event('Dev Server Dashboard App Status');
      const dashboardPort = this.dashboardAppProcess && await this.dashboardAppProcess.dashboardUrlPromise;
      res.json({
        running: !!dashboardPort,
        dashboardPort,
        dashboardAppPath: path.resolve(options.dashboardAppPath)
      });
    }));

    app.post('/playground/apply-template-packages', catchErrors(async (req, res) => {
      this.cubejsServer.event('Dev Server Download Template Packages');

      const fetcher = process.env.TEST_TEMPLATES ? new DevPackageFetcher(repo) : new PackageFetcher(repo);

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
            appPath: options.dashboardAppPath,
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
        console.log('err', err);
        lastApplyTemplatePackagesError = err;
        if (promise === this.applyTemplatePackagesPromise) {
          this.applyTemplatePackagesPromise = null;
        }
      });
      res.json(true); // TODO
    }));

    app.get('/playground/manifest', catchErrors(async (_, res) => {
      const fetcher = process.env.TEST_TEMPLATES ? new DevPackageFetcher(repo) : new PackageFetcher(repo);
      res.json(await fetcher.manifestJSON());
    }));

    app.use(serveStatic(path.join(__dirname, '../../../playground'), {
      lastModified: false,
      etag: false,
      setHeaders: (res, url) => {
        if (url.indexOf('/index.html') !== -1) {
          res.setHeader('Cache-Control', 'no-cache');
        }
      }
    }));

    app.get('/playground/test-connection', catchErrors(async (_, res) => {
      const orchestratorApi = this.cubejsServer.getOrchestratorApi({
        authInfo: null,
        requestId: ''
      });

      try {
        orchestratorApi.addDataSeenSource('default');
        await orchestratorApi.testConnection();
        this.cubejsServer.event('test_database_connection_success');
      } catch (error) {
        this.cubejsServer.event('test_database_connection_error');
        return res.status(400).json({
          error: error.toString()
        });
      }

      return res.json('ok');
    }));

    app.get('/restart', catchErrors(async (_, res) => {
      process.kill(process.pid, 'SIGUSR1');

      return res.json('Restarting...');
    }));

    app.post('/playground/env', catchErrors(async (req, res) => {
      let { variables = {} } = req.body || {};

      if (!variables.CUBEJS_API_SECRET) {
        variables.CUBEJS_API_SECRET = crypto.randomBytes(64).toString('hex');
      }
      variables = Object.entries(variables).map(([key, value]) => ([key, value].join('=')));

      if (fs.existsSync('./.env')) {
        fs.removeSync('./.env');
      }

      if (!fs.existsSync('./schema')) {
        fs.mkdirSync('./schema');
      }

      fs.writeFileSync('.env', variables.join('\n'));

      res.status(200).json('ok');
    }));
  }
}
