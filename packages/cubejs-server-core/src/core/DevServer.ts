/* eslint-disable global-require,no-restricted-syntax */
import dotenv from '@cubejs-backend/dotenv';
import { CubePreAggregationConverter, CubeSchemaConverter } from '@cubejs-backend/schema-compiler';
import spawn from 'cross-spawn';
import path from 'path';
import fs from 'fs-extra';
import { getRequestIdFromRequest } from '@cubejs-backend/api-gateway';
import { LivePreviewWatcher } from '@cubejs-backend/cloud';
import { AppContainer, DependencyTree, PackageFetcher, DevPackageFetcher } from '@cubejs-backend/templates';
import jwt from 'jsonwebtoken';
import isDocker from 'is-docker';
import type { Application as ExpressApplication, Request, Response } from 'express';
import type { ChildProcess } from 'child_process';
import { executeCommand, getEnv, packageExists } from '@cubejs-backend/shared';
import crypto from 'crypto';

import type { BaseDriver } from '@cubejs-backend/query-orchestrator';

import { CubejsServerCore } from './server';
import { ExternalDbTypeFn, ServerCoreInitializedOptions } from './types';
import DriverDependencies from './DriverDependencies';

const repo = {
  owner: 'cube-js',
  name: 'cubejs-playground-templates'
};

type DevServerOptions = {
  externalDbTypeFn: ExternalDbTypeFn;
  isReadyForQueryProcessing: () => boolean;
  dockerVersion?: string;
};

export class DevServer {
  protected applyTemplatePackagesPromise: Promise<any> | null = null;

  protected dashboardAppProcess: ChildProcess & { dashboardUrlPromise?: Promise<any> } | null = null;

  protected livePreviewWatcher = new LivePreviewWatcher();

  public constructor(
    protected readonly cubejsServer: CubejsServerCore,
    protected readonly options: DevServerOptions
  ) {
  }

  public initDevEnv(app: ExpressApplication, options: ServerCoreInitializedOptions) {
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

    if (
      (
        this.options.externalDbTypeFn({
          authInfo: null,
          securityContext: null,
          requestId: '',
        }) || ''
      ).toLowerCase() !== 'cubestore'
    ) {
      console.log('⚠️  Your pre-aggregations will be on an external database. It is recommended to use Cube Store for optimal performance');
    }

    this.cubejsServer.event('Dev Server Start');
    const serveStatic = require('serve-static');

    const catchErrors = (handler) => async (req, res, next) => {
      try {
        await handler(req, res, next);
      } catch (e) {
        console.error(((e as Error).stack || e).toString());
        this.cubejsServer.event('Dev Server Error', { error: ((e as Error).stack || e).toString() });
        res.status(500).json({ error: ((e as Error).stack || e).toString() });
      }
    };

    app.get('/playground/context', catchErrors((req, res) => {
      this.cubejsServer.event('Dev Server Env Open');

      res.json({
        cubejsToken,
        basePath: options.basePath,
        anonymousId: this.cubejsServer.anonymousId,
        coreServerVersion: this.cubejsServer.coreServerVersion,
        dockerVersion: this.options.dockerVersion || null,
        projectFingerprint: this.cubejsServer.projectFingerprint,
        dbType: options.dbType || null,
        shouldStartConnectionWizardFlow: !this.options.isReadyForQueryProcessing(),
        livePreview: options.livePreview,
        isDocker: isDocker(),
        telemetry: options.telemetry,
        identifier: this.getIdentifier(options.apiSecret),
        previewFeatures: getEnv('previewFeatures'),
      });
    }));

    app.get('/playground/db-schema', catchErrors(async (req, res) => {
      this.cubejsServer.event('Dev Server DB Schema Load');
      const driver = await this.cubejsServer.getDriver({
        dataSource: req.body.dataSource || 'default',
        authInfo: null,
        securityContext: null,
        requestId: getRequestIdFromRequest(req),
      });

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

      const dataSource = req.body.dataSource || 'default';

      const driver = await this.cubejsServer.getDriver({
        dataSource,
        authInfo: null,
        securityContext: null,
        requestId: getRequestIdFromRequest(req),
      });
      const tablesSchema = req.body.tablesSchema || (await driver.tablesSchema());

      const ScaffoldingTemplate = require('@cubejs-backend/schema-compiler/scaffolding/ScaffoldingTemplate');
      const scaffoldingTemplate = new ScaffoldingTemplate(tablesSchema, driver);
      const files = scaffoldingTemplate.generateFilesByTableNames(req.body.tables, { dataSource });

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
        const { dashboardAppPort = 3000 } = options;
        this.dashboardAppProcess = spawn('npm', [
          'run',
          'start',
          '--',
          '--port',
          dashboardAppPort.toString(),
          ...(isDocker() ? ['--host', '0.0.0.0'] : [])
        ], {
          cwd: options.dashboardAppPath,
          env: <any>{
            ...process.env,
            PORT: dashboardAppPort
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

    let driverPromise: Promise<void> | null = null;
    let driverError: Error | null = null;

    app.get('/playground/driver', catchErrors(async (req: Request, res: Response) => {
      const { driver } = req.query;

      if (!driver || !DriverDependencies[driver]) {
        return res.status(400).json('Wrong driver');
      }

      if (packageExists(DriverDependencies[driver])) {
        return res.json({ status: 'installed' });
      } else if (driverPromise) {
        return res.json({ status: 'installing' });
      } else if (driverError) {
        return res.status(500).json({
          status: 'error',
          error: driverError.toString()
        });
      }

      return res.json({ status: null });
    }));

    app.post('/playground/driver', catchErrors((req, res) => {
      const { driver } = req.body;

      if (!DriverDependencies[driver]) {
        return res.status(400).json(`'${driver}' driver dependency not found`);
      }

      async function installDriver() {
        driverError = null;

        try {
          await executeCommand(
            'npm',
            ['install', DriverDependencies[driver], '--save-dev'],
            { cwd: path.resolve('.') }
          );
        } catch (error) {
          driverError = error as Error;
        } finally {
          driverPromise = null;
        }
      }

      if (!driverPromise) {
        driverPromise = installDriver();
      }

      return res.json({
        dependency: DriverDependencies[driver]
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

        let templatePackages: string[];
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

    app.get('/playground/live-preview/start/:token', catchErrors(async (req: Request, res: Response) => {
      this.livePreviewWatcher.setAuth(req.params.token);
      this.livePreviewWatcher.startWatch();

      res.setHeader('Content-Type', 'text/html');
      res.write('<html><body><script>window.close();</script></body></html>');
      res.end();
    }));

    app.get('/playground/live-preview/stop', catchErrors(async (req, res) => {
      this.livePreviewWatcher.stopWatch();
      res.json({ active: false });
    }));

    app.get('/playground/live-preview/status', catchErrors(async (req, res) => {
      const statusObj = await this.livePreviewWatcher.getStatus();
      res.json(statusObj);
    }));

    app.post('/playground/live-preview/token', catchErrors(async (req, res) => {
      const token = await this.livePreviewWatcher.createTokenWithPayload(req.body);
      res.json({ token });
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

    app.post('/playground/test-connection', catchErrors(async (req, res) => {
      const { variables = {} } = req.body || {};

      let driver: BaseDriver | null = null;

      try {
        if (!variables.CUBEJS_DB_TYPE) {
          throw new Error('CUBEJS_DB_TYPE is required');
        }

        // Backup env variables for restoring
        const originalProcessEnv = process.env;
        process.env = {
          ...process.env,
        };

        for (const [envName, value] of Object.entries(variables)) {
          process.env[envName] = <string>value;
        }

        driver = CubejsServerCore.createDriver(variables.CUBEJS_DB_TYPE);

        // Restore original process.env
        process.env = originalProcessEnv;

        await driver.testConnection();

        this.cubejsServer.event('test_database_connection_success');

        return res.json('ok');
      } catch (error) {
        this.cubejsServer.event('test_database_connection_error');

        return res.status(400).json({
          error: error.toString()
        });
      } finally {
        if (driver && (<any>driver).release) {
          await (<any>driver).release();
        }
      }
    }));

    app.post('/playground/env', catchErrors(async (req, res) => {
      let { variables = {} } = req.body || {};

      if (!variables.CUBEJS_API_SECRET) {
        variables.CUBEJS_API_SECRET = options.apiSecret;
      }

      variables.CUBEJS_EXTERNAL_DEFAULT = 'true';
      variables.CUBEJS_SCHEDULED_REFRESH_DEFAULT = 'true';
      variables.CUBEJS_DEV_MODE = 'true';
      variables = Object.entries(variables).map(([key, value]) => ([key, value].join('=')));

      const repositoryPath = path.join(process.cwd(), options.schemaPath);

      if (!fs.existsSync(repositoryPath)) {
        fs.mkdirSync(repositoryPath);
      }

      fs.writeFileSync(path.join(process.cwd(), '.env'), variables.join('\n'));

      dotenv.config({ override: true });

      await this.cubejsServer.resetInstanceState();

      res.status(200).json(req.body.variables || {});
    }));

    app.post('/playground/token', catchErrors(async (req, res) => {
      const { payload = {} } = req.body;
      const jwtOptions = typeof payload.exp != null ? {} : { expiresIn: '1d' };

      const token = jwt.sign(payload, options.apiSecret, jwtOptions);

      res.json({ token });
    }));

    app.post('/playground/schema/pre-aggregation', catchErrors(async (req: Request, res: Response) => {
      const { cubeName, preAggregationName, code } = req.body;

      const schemaConverter = new CubeSchemaConverter(this.cubejsServer.repository, [
        new CubePreAggregationConverter({
          cubeName,
          preAggregationName,
          code
        })
      ]);

      try {
        await schemaConverter.generate();
      } catch (error) {
        res.status(400).json({ error: (error as Error).message || error });
      }

      schemaConverter.getSourceFiles().forEach(({ cubeName: currentCubeName, fileName, source }) => {
        if (currentCubeName === cubeName) {
          this.cubejsServer.repository.writeDataSchemaFile(fileName, source);
        }
      });

      res.json('ok');
    }));
  }

  protected getIdentifier(apiSecret: string): string {
    return crypto.createHash('md5')
      .update(apiSecret)
      .digest('hex')
      .replace(/[^\d]/g, '')
      .slice(0, 10);
  }
}
