import path from 'path';
import fs from 'fs';
import color from '@oclif/color';
import dotenv from 'dotenv';
import { parse as semverParse, SemVer, compare as semverCompare } from 'semver';
import {
  getEnv,
  isDockerImage,
  packageExists,
  PackageManifest,
  resolveBuiltInPackageVersion,
} from '@cubejs-backend/shared';

import {
  getMajorityVersion,
  isCubeNotServerPackage,
  isDevPackage,
  isSimilarPackageRelease, parseNpmLock,
  parseYarnLock, ProjectLock,
} from './utils';
import { CreateOptions, CubejsServer } from '../server';
import type { TypescriptCompiler as TypescriptCompilerType } from './typescript-compiler';

function safetyParseSemver(version: string|null) {
  if (version) {
    return semverParse(version);
  }

  return null;
}

export class ServerContainer {
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

    if (manifest.dependencies) {
      // eslint-disable-next-line no-restricted-syntax
      for (const [pkgName] of Object.entries(manifest.dependencies)) {
        if (isDevPackage(pkgName)) {
          throw new Error(
            `"${pkgName}" package must be installed in devDependencies`
          );
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

  public async runServerInstance(configuration: CreateOptions) {
    const server = new CubejsServer(configuration);

    try {
      const { version, port } = await server.listen();

      console.log(`🚀 Cube.js server (${version}) is listening on ${port}`);
    } catch (e) {
      console.error('Fatal error during server start: ');
      console.error(e.stack || e);

      process.exit(1);
    }

    return server;
  }

  public async lookupConfiguration(): Promise<CreateOptions> {
    const { error } = dotenv.config();
    if (error) {
      throw new Error(error.message);
    }

    const devMode = getEnv('devMode');
    if (devMode) {
      process.env.NODE_ENV = 'development';
    }

    if (fs.existsSync(path.join(process.cwd(), 'cube.ts'))) {
      this.getTypeScriptCompiler().compileConfiguration();
    }

    if (fs.existsSync(path.join(process.cwd(), 'cube.js'))) {
      return this.loadConfiguration();
    }

    console.log(
      `${color.yellow('warning')} There is no cube.js file. Continue with environment variables`
    );

    return {};
  }

  protected async loadConfiguration(): Promise<CreateOptions> {
    const file = await import(
      path.join(process.cwd(), 'cube.js')
    );

    if (this.configuration.debug) {
      console.log('Loaded configuration file', file);
    }

    if (file.default) {
      return file.default;
    }

    throw new Error(
      'Configure file must export configuration as default.'
    );
  }

  public async start() {
    const makeInstance = async () => {
      const configuration = await this.lookupConfiguration();
      return this.runServerInstance({
        gracefulShutdown: getEnv('gracefulShutdown') || process.env.NODE_ENV === 'production' ? 30 : undefined,
        ...configuration,
      });
    };

    let server = await makeInstance();

    // eslint-disable-next-line no-restricted-syntax
    for (const bindSignal of ['SIGTERM', 'SIGINT']) {
      // eslint-disable-next-line no-loop-func
      process.on(bindSignal, async (signal) => {
        process.exit(
          await server.shutdown(signal)
        );
      });
    }

    process.addListener('SIGUSR1', async (signal) => {
      console.log(`Received ${signal} signal, reloading`);

      await server.shutdown(signal, true);

      server = await makeInstance();
    });
  }
}
