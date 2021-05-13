// eslint-disable-next-line import/no-extraneous-dependencies
import ts from 'typescript';
import path from 'path';
import { DiagnosticHost } from './diagnostic-host';

export class TypescriptCompiler {
  protected readonly options: ts.CompilerOptions = {
    target: ts.ScriptTarget.ES2017,
    module: ts.ModuleKind.CommonJS,
    jsx: ts.JsxEmit.None,
    lib: [
      'lib.es2017.full.d.ts',
    ],
    rootDir: '/cube',
    esModuleInterop: true,
    moduleResolution: ts.ModuleResolutionKind.NodeJs,
  };

  public compileConfiguration = (): string => {
    const files = [
      path.join(process.cwd(), 'cube.ts')
    ];

    // In-memory emit to protect override of cube.js when we compile cube.ts
    const createdFiles: Record<string, string> = {};

    const host = ts.createCompilerHost(this.options);
    host.writeFile = (fileName: string, contents: string) => {
      createdFiles[path.basename(fileName)] = contents;
    };

    const program = ts.createProgram(files, this.options, host);

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

    return createdFiles['cube.js'];
  };
}
