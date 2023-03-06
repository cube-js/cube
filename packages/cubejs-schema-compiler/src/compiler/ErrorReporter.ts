import { codeFrameColumns, SourceLocation } from '@babel/code-frame';

import { UserError } from './UserError';
import { CompileError } from './CompileError';

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
  loc: SourceLocation | null,
}

interface File {
  fileName: string,
  content: string,
}

interface ErrorReporterOptions {
  logger: (msg: string) => void
}

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

  public warning(e: SyntaxErrorInterface) {
    if (this.file && e.loc) {
      const codeFrame = codeFrameColumns(this.file.content, e.loc, {
        message: e.message,
        highlightCode: true,
      });
      const plainMessage = `Warning: ${e.message} in ${this.file.fileName}`;
      const message = `${plainMessage}\n${codeFrame}`;

      if (this.rootReporter().warnings.find(m => (m.message || m) === message)) {
        return;
      }

      this.rootReporter().warnings.push({
        message,
        plainMessage,
        loc: e.loc,
      });

      this.options.logger(message);
    } else {
      if (this.rootReporter().warnings.find(m => (m.message || m) === e.message)) {
        return;
      }

      this.rootReporter().warnings.push(e);

      this.options.logger(e.message);
    }
  }

  public syntaxError(e: SyntaxErrorInterface) {
    if (this.file && e.loc) {
      const codeFrame = codeFrameColumns(this.file.content, e.loc, {
        message: e.message,
        highlightCode: true,
      });
      const plainMessage = `Syntax Error: ${e.message} in ${this.file.fileName}`;
      const message = `${plainMessage}\n${codeFrame}`;

      if (this.rootReporter().errors.find(m => (m.message || m) === message)) {
        return;
      }

      this.rootReporter().errors.push({
        message,
        plainMessage,
      });
    } else {
      if (this.rootReporter().errors.find(m => (m.message || m) === e.message)) {
        return;
      }

      this.rootReporter().errors.push(e);
    }
  }

  public error(e: any, fileName?: any, lineNumber?: any, position?: any) {
    const message = `${this.context.length ? `${this.context.join(' -> ')}: ` : ''}${e instanceof UserError ? e.message : (e.stack || e)}`;
    if (this.rootReporter().errors.find(m => (m.message || m) === message)) {
      return;
    }

    if (fileName) {
      this.rootReporter().errors.push({
        message, fileName, lineNumber, position
      });
    } else {
      this.rootReporter().errors.push({
        message,
      });
    }
  }

  public inContext(context: string) {
    return new ErrorReporter(this, this.context.concat(context));
  }

  public throwIfAny() {
    if (this.rootReporter().errors.length) {
      throw new CompileError(
        this.rootReporter().errors.map((e) => e.message).join('\n'),
        this.rootReporter().errors.map((e) => e.plainMessage).join('\n')
      );
    }
  }

  protected rootReporter(): ErrorReporter {
    return this.parent ? this.parent.rootReporter() : this;
  }
}
