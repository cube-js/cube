import path from 'path';
import ts from 'typescript';

// The build skips declaration files (skipLibCheck), so the published types are checked here.
describe('index.d.ts', () => {
  it('has no type errors', () => {
    const file = path.resolve(__dirname, '../../index.d.ts');
    const program = ts.createProgram([file], { strict: true, noEmit: true, types: [] });
    const errors = ts.getPreEmitDiagnostics(program, program.getSourceFile(file));

    expect(errors.map((e) => ts.flattenDiagnosticMessageText(e.messageText, '\n'))).toEqual([]);
  });
});
