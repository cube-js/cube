import path from 'path';
import fs from 'fs';
import fsAsync from 'fs/promises';
import vm from 'vm';
import color from '@oclif/color';
import dotenv from '@cubejs-backend/dotenv';
import { parse as semverParse, SemVer, compare as semverCompare } from 'semver';
import {
  displayCLIWarning,
  getEnv,
  isDockerImage, isNativeSupported,
  packageExists,
  PackageManifest,
  resolveBuiltInPackageVersion,
} from '@cubejs-backend/shared';
import { SystemOptions } from '@cubejs-backend/server-core';
import { isFallbackBuild, pythonLoadConfig } from '@cubejs-backend/native';

import {
  getMajorityVersion,
  isCubeNotServerPackage,
  isDevPackage,
  isSimilarPackageRelease, parseNpmLock,
  parseYarnLock, ProjectLock,
} from './utils';
import { CreateOptions, CubejsServer } from '../server';
import type { TypescriptCompiler as TypescriptCompilerType } from './typescript-compiler';

function safetyParseSemver(version: string | null) {
  if (version) {
    return semverParse(version);
  }

  return null;
}

export class ServerContainer {
  protected isCubeConfigEmpty: boolean = true;

  public constructor(
    protected readonly configuration: { debug: boolean }
  ) {
  }

  protected getTypeScriptCompiler(): TypescriptCompilerType {
    if (packageExists('typescript', isDockerImage())) {
      // eslint-disable-next-line global-require
      const { TypescriptCompiler } = require('./typescript-compiler');

      return new TypescriptCompiler();
    }

    throw new Error(
      'Typescript dependency not found. Please run this command from project directory.'
    );
  }

  protected compareBuiltInAndUserVersions(builtInVersion: SemVer, userVersion: SemVer) {
    const compareResult = semverCompare(builtInVersion, userVersion);

    if (this.configuration.debug) {
      console.log('[runProjectDockerDiagnostics] compare', {
        builtIn: builtInVersion.raw,
        user: userVersion.raw,
        compare: compareResult,
      });
    }

    if (compareResult === -1) {
      console.log(
        `${color.yellow('warning')} You are using old Docker image (${getMajorityVersion(builtInVersion, true)}) `
        + `with new packages (${getMajorityVersion(userVersion, true)})`
      );
    }

    if (compareResult === 1) {
      console.log(
        `${color.yellow('warning')} You are using old Cube.js packages (${getMajorityVersion(userVersion, true)}) `
        + `with new Docker image (${getMajorityVersion(builtInVersion, true)})`
      );
    }
  }

  protected async parseLock() {
    const hasNpm = fs.existsSync(path.join(process.cwd(), 'package-lock.json'));
    const hasYarn = fs.existsSync(path.join(process.cwd(), 'yarn.lock'));

    if (this.configuration.debug) {
      console.log('[parseLock] do', {
        hasNpm,
        hasYarn
      });
    }

    if (hasNpm && hasYarn) {
      console.log(
        `${color.yellow('warning')} You are using two different lock files, both for npm/yarn. Please use only one.`
      );

      return null;
    }

    if (hasNpm) {
      return parseNpmLock();
    }

    if (hasYarn) {
      return parseYarnLock();
    }

    // @todo Error or notice?
    return null;
  }

  protected async runProjectDockerDiagnostics(manifest: PackageManifest, lock: ProjectLock) {
    if (this.configuration.debug) {
      console.log('[runProjectDockerDiagnostics] do');
    }

    const builtInCoreVersion = safetyParseSemver(
      await resolveBuiltInPackageVersion(
        '@cubejs-backend/server',
      )
    );
    if (!builtInCoreVersion) {
      return;
    }

    const userCoreVersion = safetyParseSemver(
      lock.resolveVersion('@cubejs-backend/server'),
    );
    if (userCoreVersion) {
      this.compareBuiltInAndUserVersions(
        builtInCoreVersion,
        userCoreVersion
      );

      return;
    }

    if (manifest.devDependencies) {
      /**
       * It's needed to detect case when user didnt install @cubejs-backend/server, but
       * install @cubejs-backend/postgres-driver and it doesn't fit to built-in server
       */
      const depsToCompareVersions = Object.keys(manifest.devDependencies).filter(
        isCubeNotServerPackage
      );
      // eslint-disable-next-line no-restricted-syntax
      for (const pkgName of depsToCompareVersions) {
        const pkgVersion = safetyParseSemver(
          lock.resolveVersion(pkgName)
        );
        if (pkgVersion) {
          this.compareBuiltInAndUserVersions(builtInCoreVersion, pkgVersion);

          return;
        }
      }
    }
  }

  public async runProjectDiagnostics() {
    if (!fs.existsSync(path.join(process.cwd(), 'package.json'))) {
      if (this.configuration.debug) {
        console.log('[runProjectDiagnostics] Unable to find package.json, configuration diagnostics skipped');
      }

      return;
    }

    // eslint-disable-next-line global-require,import/no-dynamic-require
    const manifest = require(path.join(process.cwd(), 'package.json'));
    if (!manifest) {
      return;
    }

    const deepsToIgnore = [
      '@cubejs-backend/databricks-jdbc-driver',
    ];

    if (manifest.dependencies) {
      // eslint-disable-next-line no-restricted-syntax
      for (const [pkgName] of Object.entries(manifest.dependencies)) {
        if (isDevPackage(pkgName) && !deepsToIgnore.includes(pkgName)) {
          displayCLIWarning(`"${pkgName}" package must be installed in devDependencies`);
        }
      }
    }

    const lock = await this.parseLock();
    if (!lock) {
      return;
    }

    if (manifest.devDependencies) {
      const coreVersion = safetyParseSemver(
        lock.resolveVersion('@cubejs-backend/server'),
      );
      if (coreVersion) {
        const depsToCompareVersions = Object.keys(manifest.devDependencies).filter(
          isCubeNotServerPackage
        );

        // eslint-disable-next-line no-restricted-syntax
        for (const pkgName of depsToCompareVersions) {
          const pkgVersion = safetyParseSemver(
            lock.resolveVersion(pkgName)
          );
          if (pkgVersion && !isSimilarPackageRelease(pkgVersion, coreVersion)) {
            console.log(
              `${color.yellow('error')} "${pkgName}" (${getMajorityVersion(pkgVersion)}) `
              + `is using another release then @cubejs-backend/server (${getMajorityVersion(coreVersion)}).`
            );
          }
        }
      }
    }

    if (isDockerImage()) {
      await this.runProjectDockerDiagnostics(manifest, lock);
    } else if (this.configuration.debug) {
      console.log('[runProjectDockerDiagnostics] skipped');
    }
  }

  public async runServerInstance(
    configuration: CreateOptions,
    embedded: boolean = false,
    isCubeConfigEmpty: boolean
  ) {
    if (embedded) {
      process.env.CUBEJS_SCHEDULED_REFRESH_TIMER = 'false';
      configuration.scheduledRefreshTimer = false;
    }

    const server = this.createServer(configuration, { isCubeConfigEmpty });

    if (!embedded) {
      try {
        const { version, port } = await server.listen();

        console.log(`🚀 Cube API server (${version}) is listening on ${port}`);
      } catch (e: any) {
        console.error('Fatal error during server start: ');
        console.error(e.stack || e);

        process.exit(1);
      }
    }

    return server;
  }

  protected createServer(config: CreateOptions, systemOptions?: SystemOptions): CubejsServer {
    return new CubejsServer(config, systemOptions);
  }

  public async lookupConfiguration(override: boolean = false): Promise<CreateOptions> {
    dotenv.config({
      override,
      multiline: 'line-breaks'
    });

    const devMode = getEnv('devMode');
    if (devMode) {
      process.env.NODE_ENV = 'development';
    }

    if (fs.existsSync(path.join(process.cwd(), 'cube.py'))) {
      const supported = isNativeSupported();
      if (supported !== true) {
        throw new Error(
          `Native extension is required to load Python configuration. ${supported.reason}. Read more: ` +
          'https://github.com/cube-js/cube/blob/master/packages/cubejs-backend-native/README.md#supported-architectures-and-platforms'
        );
      }

      if (isFallbackBuild()) {
        throw new Error(
          'Unable to load Python configuration because you are using the fallback build of native extension. Read more: ' +
          'https://github.com/cube-js/cube/blob/master/packages/cubejs-backend-native/README.md#supported-architectures-and-platforms'
        );
      }

      return this.loadConfigurationFromPythonFile();
    }

    if (fs.existsSync(path.join(process.cwd(), 'cube.ts'))) {
      return this.loadConfigurationFromMemory(
        this.getTypeScriptCompiler().compileConfiguration()
      );
    }

    if (fs.existsSync(path.join(process.cwd(), 'cube.js'))) {
      return this.loadConfigurationFromFile();
    }

    displayCLIWarning(
      'There is no cube.js file. Continue with environment variables'
    );

    return {};
  }

  protected async loadConfigurationFromMemory(content: string): Promise<CreateOptions> {
    if (this.configuration.debug) {
      console.log('Loaded configuration from memory', content);
    }

    const exports: Record<string, any> = {};

    const script = new vm.Script(content);
    script.runInNewContext(
      {
        require,
        console,
        // Workaround for ES5 exports
        exports
      },
      {
        filename: 'cube.js',
        displayErrors: true,
      }
    );

    if (exports.default) {
      return exports.default;
    }

    throw new Error(
      'Configure file must export configuration as default.'
    );
  }

  protected async loadConfigurationFromPythonFile(): Promise<CreateOptions> {
    const content = await fsAsync.readFile(
      path.join(process.cwd(), 'cube.py'),
      'utf-8'
    );

    if (this.configuration.debug) {
      console.log('Loaded python configuration file', content);
    }

    return this.loadConfigurationFromPythonMemory(content);
  }

  protected async loadConfigurationFromPythonMemory(content: string): Promise<CreateOptions> {
    const config = await pythonLoadConfig(content, {
      fileName: 'cube.py',
    });

    return config as any;
  }

  protected async loadConfigurationFromFile(): Promise<CreateOptions> {
    const file = await import(
      path.join(process.cwd(), 'cube.js')
    );

    if (this.configuration.debug) {
      console.log('Loaded js configuration file', file);
    }

    if (file.default) {
      return file.default;
    }

    throw new Error(
      'Configuration file must export the configuration as default.'
    );
  }

  /**
   * @param embedded Cube.js will start without https/ws/graceful shutdown + without timers
   */
  public async start(embedded: boolean = false) {
    const makeInstance = async (override: boolean) => {
      const userConfig = await this.lookupConfiguration(override);

      const configuration = {
        // By default graceful shutdown is disabled, but this value is needed for reboot
        gracefulShutdown: getEnv('gracefulShutdown') || (process.env.NODE_ENV === 'production' ? 30 : 2),
        ...userConfig,
      };

      const server = await this.runServerInstance(
        configuration,
        embedded,
        Object.keys(userConfig).length === 0
      );

      return {
        configuration,
        gracefulEnabled: !!(getEnv('gracefulShutdown') || userConfig.gracefulShutdown),
        server
      };
    };

    let instance = await makeInstance(false);

    if (!embedded) {
      let shutdownHandler: Promise<0 | 1> | null = null;
      let killSignalCount = 0;

      const signalToShutdown: NodeJS.Signals[] = [
        // Signal Terminate - graceful shutdown in Unix systems
        'SIGTERM',
        // Ctrl+C
        'SIGINT'
      ];

      // eslint-disable-next-line no-restricted-syntax
      for (const bindSignal of signalToShutdown) {
        // eslint-disable-next-line no-loop-func
        process.on(bindSignal, async (signal) => {
          killSignalCount++;

          if (killSignalCount === 3) {
            console.log('Received killing signal 3 times, exiting immediately');

            // 130 is the default exit code when killed by a signal.
            process.exit(130);
          }

          if (instance.gracefulEnabled) {
            if (shutdownHandler) {
              return;
            }

            console.log(`Received ${signal} signal, shutting down in ${instance.configuration.gracefulShutdown}s`);

            try {
              shutdownHandler = instance.server.shutdown(signal, true);

              process.exit(
                await shutdownHandler,
              );
            } catch (e) {
              console.log(e);

              process.exit(1);
            }
          } else {
            console.log(`Received ${signal} signal, terminating with process exit`);
            process.exit(0);
          }
        });
      }

      let restartHandler: Promise<0 | 1> | null = null;

      process.addListener('SIGUSR1', async (signal) => {
        console.log(`Received ${signal} signal, reloading in ${instance.configuration.gracefulShutdown}s`);

        if (restartHandler) {
          console.log('Unable to restart server while it\'s already restarting');

          return;
        }

        try {
          restartHandler = instance.server.shutdown(signal, true);

          await restartHandler;
        } finally {
          restartHandler = null;
        }

        instance = await makeInstance(true);
      });
    }
  }
}
