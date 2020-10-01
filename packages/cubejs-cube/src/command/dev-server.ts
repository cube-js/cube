import path from 'path';
import fs from 'fs';
import * as ts from 'typescript';
import { ModuleKind, FormatDiagnosticsHost } from 'typescript';

import CubejsServer, { CreateOptions } from '@cubejs-backend/server';
import { CommandInterface } from './command.interface';
import { ServerCommand } from './server';

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

export class DevServerCommand extends ServerCommand {
  public getName() {
    return 'dev-server';
  }

  public getDescription() {
    return 'Start server in development-mode';
  }

  public async execute() {
    process.env.NODE_ENV = 'development';

    const configuration = this.lookupConfiguration();
    this.runServerInstance(configuration);
  }
}
