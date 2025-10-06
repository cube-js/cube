import { codeFrameColumns, SourceLocation } from '@babel/code-frame';

import { UserError } from './UserError';
import { CompileError } from './CompileError';

export type ErrorLikeObject = {
  message: string;
};

export type PossibleError = Error | UserError | string | ErrorLikeObject;

export interface CompilerErrorInterface {
  message: string;
  plainMessage?: string
  fileName?: string;
  lineNumber?: string;
  position?: number;
}

export interface SyntaxErrorInterface {
  message: string;
  plainMessage?: string
  loc?: SourceLocation | null | undefined,
  fileName?: string;
}

interface File {
  fileName: string,
  content: string,
}

export interface ErrorReporterOptions {
  logger: (msg: string) => void
}

const NO_FILE_SPECIFIED = '_No-file-specified';

export class ErrorReporter {
  protected warnings: SyntaxErrorInterface[] = [];

  protected errors: CompilerErrorInterface[] = [];

  protected file: File | null = null;

  public constructor(
    protected readonly parent: ErrorReporter | null = null,
    protected readonly context: any[] = [],
    protected readonly options: ErrorReporterOptions = {
      logger: (msg) => console.log(msg),
    },
  ) {
  }

  public exitFile() {
    this.file = null;
  }

  public inFile(file: File) {
    this.file = file;
  }

  public warning(e: SyntaxErrorInterface, fileName?: string) {
    const targetFileName = fileName || e.fileName || this.file?.fileName;

    if (this.file && e.loc) {
      const codeFrame = codeFrameColumns(this.file.content, e.loc, {
        message: e.message,
        highlightCode: true,
      });
      const plainMessage = `Warning: ${e.message}`;
      const message = `${plainMessage}\n${codeFrame}`;

      if (this.rootReporter().warnings.find(m => (m.message || m) === message)) {
        return;
      }

      this.rootReporter().warnings.push({
        message,
        plainMessage,
        loc: e.loc,
        fileName: targetFileName,
      });

      if (targetFileName) {
        this.options.logger(`${targetFileName}:\n${message}`);
      } else {
        this.options.logger(message);
      }
    } else {
      if (this.rootReporter().warnings.find(m => (m.message || m) === e.message)) {
        return;
      }

      this.rootReporter().warnings.push({
        ...e,
        fileName: targetFileName,
      });

      if (targetFileName) {
        this.options.logger(`${targetFileName}:\n${e.message}`);
      } else {
        this.options.logger(e.message);
      }
    }
  }

  public syntaxError(e: SyntaxErrorInterface, fileName?: string) {
    const targetFileName = fileName || e.fileName || this.file?.fileName;

    if (this.file && e.loc) {
      const codeFrame = codeFrameColumns(this.file.content, e.loc, {
        message: e.message,
        highlightCode: true,
      });
      const plainMessage = `Syntax Error: ${e.message}`;
      const message = `${plainMessage}\n${codeFrame}`;

      if (this.rootReporter().errors.find(m => (m.message || m) === message)) {
        return;
      }

      this.rootReporter().errors.push({
        message,
        plainMessage,
        fileName: targetFileName,
      });
    } else {
      if (this.rootReporter().errors.find(m => (m.message || m) === e.message)) {
        return;
      }

      this.rootReporter().errors.push({
        ...e,
        fileName: targetFileName,
      });
    }
  }

  public error(e: any, fileName?: any, lineNumber?: any, position?: any) {
    const targetFileName = fileName || this.file?.fileName;
    const message = `${this.context.length ? `${this.context.join(' -> ')}: ` : ''}${e.message ? e.message : (e.stack || e)}`;
    if (this.rootReporter().errors.find(m => (m.message || m) === message)) {
      return;
    }

    this.rootReporter().errors.push({
      message,
      fileName: targetFileName,
      lineNumber,
      position
    });
  }

  public inContext(context: string) {
    return new ErrorReporter(this, this.context.concat(context));
  }

  private groupErrors(): Map<string, CompilerErrorInterface[]> {
    const { errors } = this.rootReporter();

    const errorsByFile = new Map<string, CompilerErrorInterface[]>();

    for (const error of errors) {
      const key = error.fileName || NO_FILE_SPECIFIED;
      if (!errorsByFile.has(key)) {
        errorsByFile.set(key, []);
      }
      errorsByFile.get(key)!.push(error);
    }

    return errorsByFile;
  }

  public throwIfAny() {
    const { errors } = this.rootReporter();

    if (errors.length === 0) {
      return;
    }

    const errorsByFile = this.groupErrors();

    // Build formatted report
    const messageParts: string[] = [];
    const plainMessageParts: string[] = [];

    const sortedFiles = Array.from(errorsByFile.keys()).sort();

    for (const fileName of sortedFiles) {
      const fileErrors = errorsByFile.get(fileName)!;
      const reportFileName = fileName === NO_FILE_SPECIFIED ? '' : `${fileName} `;

      messageParts.push(`${reportFileName}Errors:`);

      const plainMessagesForFile: string[] = [];
      for (const error of fileErrors) {
        messageParts.push(error.message);
        if (error.plainMessage) {
          plainMessagesForFile.push(error.plainMessage);
        }
      }

      if (plainMessagesForFile.length > 0) {
        plainMessageParts.push(`${reportFileName}Errors:`);
        plainMessageParts.push(...plainMessagesForFile);
        plainMessageParts.push('');
      }

      // Add blank line between file groups
      messageParts.push('');
    }

    throw new CompileError(
      messageParts.join('\n'),
      plainMessageParts.join('\n')
    );
  }

  public getErrors() {
    return this.rootReporter().errors;
  }

  public addErrors(errors: PossibleError[], fileName?: string) {
    errors.forEach((e: any) => { this.error(e, fileName); });
  }

  public getWarnings() {
    return this.rootReporter().warnings;
  }

  public addWarnings(warnings: SyntaxErrorInterface[]) {
    warnings.forEach(w => { this.warning(w); });
  }

  protected rootReporter(): ErrorReporter {
    return this.parent ? this.parent.rootReporter() : this;
  }
}
