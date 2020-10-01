import path from 'path';
import fs from 'fs';
import * as ts from 'typescript';
import { FormatDiagnosticsHost, ModuleKind, ModuleResolutionKind } from 'typescript';

import CubejsServer, { CreateOptions } from '@cubejs-backend/server';
import { CommandInterface } from './command.interface';

class DiagnosticHost implements FormatDiagnosticsHost {
  public getNewLine(): string {
    return ts.sys.newLine;
  }

  public getCurrentDirectory(): string {
    return ts.sys.getCurrentDirectory();
  }

  public getCanonicalFileName(fileName: string): string {
    return ts.sys.useCaseSensitiveFileNames ? fileName : fileName.toLowerCase();
  }
}

export class ServerCommand implements CommandInterface {
  public getName() {
    return 'server';
  }

  public getDescription() {
    return 'Start server';
  }

  protected compileConfiguration = () => {
    const options: ts.CompilerOptions = {
      target: ts.ScriptTarget.ES2017,
      module: ModuleKind.CommonJS,
      jsx: ts.JsxEmit.None,
      lib: [
        'lib.es2017.full.d.ts',
      ],
      rootDir: '/cube',
      esModuleInterop: true,
      moduleResolution: ModuleResolutionKind.NodeJs,
    };

    const files = [
      path.join(process.cwd(), 'cube.ts')
    ];

    const host = ts.createCompilerHost(options);
    const program = ts.createProgram(files, options, host);

    const diagnostics = ts.getPreEmitDiagnostics(program);
    if (diagnostics.length) {
      const DiagnosticHostInstance = new DiagnosticHost();

      ts.sys.write(ts.formatDiagnosticsWithColorAndContext(diagnostics, DiagnosticHostInstance));
      ts.sys.exit(ts.ExitStatus.DiagnosticsPresent_OutputsSkipped);
    }

    const emitResult = program.emit();

    if (emitResult.emitSkipped) {
      console.log('Unable to compile configuration file.');
      process.exit(1);
    }
  }

  protected runServerInstance(configuration: CreateOptions) {
    const server = new CubejsServer(configuration);

    server.listen().then(({ version, port }) => {
      console.log(`🚀 Cube.js server (${version}) is listening on ${port}`);
    }).catch(e => {
      console.error('Fatal error during server start: ');
      console.error(e.stack || e);
    });
  }

  // eslint-disable-next-line consistent-return
  protected async lookupConfiguration(): Promise<CreateOptions> {
    if (fs.existsSync(path.join(process.cwd(), 'cube.ts'))) {
      this.compileConfiguration();
    }

    if (fs.existsSync(path.join(process.cwd(), 'cube.js'))) {
      return this.loadConfiguration();
    }

    console.log('Unable find configuration file: "cube.js".');
    process.exit(1);
  }

  // eslint-disable-next-line consistent-return
  protected async loadConfiguration(): Promise<CreateOptions> {
    const file = await import(
      path.join(process.cwd(), 'cube.js')
    );

    console.log(file);

    if (file.default) {
      return file.default;
    }

    console.log('Configure file must export configuration as default.');
    process.exit(1);
  }

  public async execute() {
    process.env.NODE_ENV = 'production';

    const configuration = await this.lookupConfiguration();
    this.runServerInstance(configuration);
  }
}
