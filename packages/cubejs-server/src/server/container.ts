import { CreateOptions } from '@cubejs-backend/server-core';
import path from 'path';
import fs from 'fs';
import color from '@oclif/color';

import { CubejsServer } from '../server';
import { packageExists } from './utils';

import type { TypescriptCompiler as TypescriptCompilerType } from './typescript-compiler';

const devPackages = [
  'typescript',
];

function isCubePackage(pkgName: string): boolean {
  return pkgName.toLowerCase().startsWith('@cubejs-backend/');
}

function isDevPackage(pkgName: string): boolean {
  return isCubePackage(pkgName) || devPackages.includes(pkgName.toLowerCase());
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

  public runProjectDiagnostics() {
    if (!fs.existsSync(path.join(process.cwd(), 'package.json'))) {
      if (this.configuration.debug) {
        console.log('Unable to find package.json, configuration diagnostics skipped');
      }

      return;
    }

    // eslint-disable-next-line global-require,import/no-dynamic-require
    const manifiest = require(path.join(process.cwd(), 'package.json'));
    if (manifiest) {
      if (manifiest.dependencies) {
        // eslint-disable-next-line no-restricted-syntax
        for (const [pkgName] of Object.entries(manifiest.dependencies)) {
          if (isDevPackage(pkgName)) {
            throw new Error(
              `"${pkgName}" package must be installed in devDependencies`
            );
          }
        }
      }

      if (manifiest.devDependencies) {
        // eslint-disable-next-line no-restricted-syntax
        for (const [pkgName] of Object.entries(manifiest.devDependencies)) {
          if (!isDevPackage(pkgName)) {
            console.log(
              `${color.yellow('warning')} "${pkgName}" will not be installed in Cube Cloud (please move it to dependencies)`
            );
          }
        }
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
