import { CreateOptions } from '@cubejs-backend/server-core';
import path from 'path';
import fs from 'fs';
import color from '@oclif/color';
import { parse as semverParse, SemVer } from 'semver';

import { CubejsServer } from '../server';
import { packageExists } from './utils';

import type { TypescriptCompiler as TypescriptCompilerType } from './typescript-compiler';
import { requireFromPackage } from '../shared';

const devPackages = [
  'typescript',
];

function isCubePackage(pkgName: string): boolean {
  return pkgName.toLowerCase().startsWith('@cubejs-backend/');
}

function isDevPackage(pkgName: string): boolean {
  return isCubePackage(pkgName) || devPackages.includes(pkgName.toLowerCase());
}

function isDockerImage(): boolean {
  return Boolean(process.env.CUBEJS_DOCKER_IMAGE_TAG);
}

function isSimilarPackageRelease(pkg: SemVer, core: SemVer): boolean {
  if (pkg.major === 0 && core.major === 0) {
    return pkg.minor === pkg.minor;
  }

  return pkg.major === core.major;
}

function getMajorityVersion(pkg: SemVer): string {
  if (pkg.major === 0) {
    return `^${pkg.major}.${pkg.minor}`;
  }

  return `^${pkg.major}`;
}

type PackageManifest = {
  version: string,
  dependencies: Record<string, string>,
  devDependencies: Record<string, string>
}

export class ServerContainer {
  public constructor(
    protected readonly configuration: { debug: boolean }
  ) {
  }

  protected getTypeScriptCompiler(): TypescriptCompilerType {
    if (packageExists('typescript')) {
      // eslint-disable-next-line global-require
      const { TypescriptCompiler } = require('./typescript-compiler');

      return new TypescriptCompiler();
    }

    throw new Error(
      'Typescript dependency not found. Please run this command from project directory.'
    );
  }

  protected async resolveBuiltInPackageVersion(pkgName: string) {
    const resolvedManifest = await requireFromPackage(
      path.join(pkgName, 'package.json'),
      false
    );
    if (resolvedManifest) {
      return semverParse(resolvedManifest.version);
    }

    return null;
  }

  protected async resolveUserPackageVersion(pkgName: string) {
    const resolvedManifest = await requireFromPackage(
      path.join(pkgName, 'package.json'),
      false
    );
    if (resolvedManifest) {
      return semverParse(resolvedManifest.version);
    }

    return null;
  }

  protected async runProjectDiagnosticsForDocker(manifest: PackageManifest) {
  }

  public async runProjectDiagnostics() {
    if (!fs.existsSync(path.join(process.cwd(), 'package.json'))) {
      if (this.configuration.debug) {
        console.log('Unable to find package.json, configuration diagnostics skipped');
      }

      return;
    }

    // eslint-disable-next-line global-require,import/no-dynamic-require
    const manifest: PackageManifest = require(path.join(process.cwd(), 'package.json'));
    if (manifest) {
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

      if (manifest.devDependencies) {
        // eslint-disable-next-line no-restricted-syntax
        for (const pkgName of Object.keys(manifest.devDependencies)) {
          if (!isDevPackage(pkgName)) {
            console.log(
              `${color.yellow('warning')} "${pkgName}" will not be installed in Cube Cloud (please move it to dependencies)`
            );
          }
        }

        const coreVersion = await this.resolveUserPackageVersion(
          '@cubejs-backend/server',
        );
        if (coreVersion) {
          const depsToCompareVersions = Object.keys(manifest.devDependencies).filter(
            (v) => v !== '@cubejs-backend/server' && isCubePackage(v)
          );
          // eslint-disable-next-line no-restricted-syntax
          for (const pkgName of depsToCompareVersions) {
            const pkgVersion = await this.resolveUserPackageVersion(
              pkgName,
            );
            if (pkgVersion && !isSimilarPackageRelease(pkgVersion, coreVersion)) {
              console.log(
                `${color.yellow('error')} "${pkgName}" (${getMajorityVersion(coreVersion)}) `
                + `is using another release then @cubejs-backend/server (${getMajorityVersion(pkgVersion)}).`
              );
            }
          }
        }
      }

      if (isDockerImage()) {
        await this.runProjectDiagnosticsForDocker(manifest);
      }
    }
  }

  public runServerInstance(configuration: CreateOptions) {
    const server = new CubejsServer(configuration);

    server.listen().then(({ version, port }) => {
      console.log(`🚀 Cube.js server (${version}) is listening on ${port}`);
    }).catch(e => {
      console.error('Fatal error during server start: ');
      console.error(e.stack || e);
    });
  }

  // eslint-disable-next-line consistent-return
  public async lookupConfiguration(): Promise<CreateOptions> {
    if (fs.existsSync(path.join(process.cwd(), 'cube.ts'))) {
      this.getTypeScriptCompiler().compileConfiguration();
    }

    if (fs.existsSync(path.join(process.cwd(), 'cube.js'))) {
      return this.loadConfiguration();
    }

    throw new Error('Unable find configuration file: "cube.js".');
  }

  // eslint-disable-next-line consistent-return
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
}
