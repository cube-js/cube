import { FormatDiagnosticsHost } from "typescript";

export class DiagnosticHost implements FormatDiagnosticsHost {
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
