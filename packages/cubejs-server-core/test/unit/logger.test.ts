import { devLogger } from '../../src/core/logger';

describe('devLogger', () => {
  let output: string[];
  let consoleLog: jest.SpyInstance;

  beforeEach(() => {
    output = [];
    consoleLog = jest.spyOn(console, 'log').mockImplementation((...args: unknown[]) => {
      output.push(args.join(' '));
    });
  });

  afterEach(() => {
    consoleLog.mockRestore();
  });

  const logSql = (query: string, values: unknown[]): string => {
    devLogger('info')('Executing SQL', { query, values });

    expect(output).toHaveLength(1);
    return output[0];
  };

  it('prints `?` placeholders as-is with the values next to the SQL', () => {
    const line = logSql('SELECT * FROM t WHERE a = ? AND b = ?', ["O'Brien", 5]);

    expect(line).toContain('SELECT * FROM t WHERE a = ? AND b = ?');
    expect(line).toContain('values: [ "O\'Brien", 5 ]');
  });

  it('does not fail on a reused Postgres placeholder', () => {
    const line = logSql('SELECT * FROM t WHERE a >= $1 AND b >= $1 AND c < $2', ['2024-01-01', '2024-02-01']);

    expect(line).toContain('a >= $1 AND b >= $1 AND c < $2');
    expect(line).toContain("values: [ '2024-01-01', '2024-02-01' ]");
  });

  it('does not fail on MSSQL `@_N` placeholders', () => {
    const line = logSql('SELECT * FROM t WHERE a = @_1 AND b = @_2', ['x', 'y']);

    expect(line).toContain('a = @_1 AND b = @_2');
    expect(line).toContain("values: [ 'x', 'y' ]");
  });

  it('does not fail on values that cannot be rendered as SQL', () => {
    const line = logSql('SELECT ?', [{ key: 1 }, BigInt(10)]);

    expect(line).toContain('SELECT ?');
    expect(line).toContain('values: [ { key: 1 }, 10n ]');
  });

  it('leaves a `?` inside a string literal alone', () => {
    const line = logSql("SELECT * FROM t WHERE a = ? AND s = 'what?'", ['x']);

    expect(line).toContain("a = ? AND s = 'what?'");
    expect(line).toContain("values: [ 'x' ]");
  });
});
