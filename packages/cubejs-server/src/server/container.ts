import { CreateOptions } from '@cubejs-backend/server-core';
import path from 'path';
import fs from 'fs';

import { CubejsServer } from '../server';
import { packageExists } from './utils';

import type { TypescriptCompiler as TypescriptCompilerType } from './typescript-compiler';

export class ServerContainer {
  constructor(
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
