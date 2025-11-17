import { ErrorReporter } from '../../src/compiler/ErrorReporter';
import { CompileError } from '../../src/compiler/CompileError';

describe('ErrorReporter', () => {
  it('should group and format errors and warnings from different files', () => {
    const logs: string[] = [];
    const reporter = new ErrorReporter(null, [], {
      logger: (msg) => logs.push(msg)
    });

    // Test inFile and exitFile
    reporter.inFile({
      fileName: 'schema/users.js',
      content: 'cube(\'Users\', {\n  sql: `SELECT * FROM users`,\n  measures: {\n    count: {\n      type: \'count\'\n    }\n  }\n});'
    });

    // Test syntaxError with location
    reporter.syntaxError({
      message: 'Invalid measure definition',
      loc: {
        start: { line: 4, column: 4 },
        end: { line: 4, column: 9 }
      }
    });

    // Test warning with location
    reporter.warning({
      message: 'Deprecated syntax',
      loc: {
        start: { line: 2, column: 2 },
        end: { line: 2, column: 5 }
      }
    });

    reporter.exitFile();

    // Test error without file context but with explicit fileName
    reporter.error(
      new Error('Connection failed'),
      'config/database.js',
      10,
      5
    );

    // Test inFile for another file
    reporter.inFile({
      fileName: 'schema/orders.js',
      content: 'cube(\'Orders\', {\n  sql: `SELECT * FROM orders`\n});'
    });

    // Test syntaxError without location but with file context
    reporter.syntaxError({
      message: 'Missing required field'
    });

    // Test warning without location
    reporter.warning({
      message: 'Consider adding indexes'
    });

    // Test error with explicit fileName (overrides current file)
    reporter.error(
      { message: 'Validation error' },
      'schema/products.js'
    );

    reporter.exitFile();

    // Test error without any file context
    reporter.error(new Error('Generic error'));

    // Test syntaxError with explicit fileName
    reporter.syntaxError(
      {
        message: 'Parse error'
      },
      'schema/custom.js'
    );

    // Test warning with explicit fileName
    reporter.warning(
      {
        message: 'Performance warning'
      },
      'schema/analytics.js'
    );

    // Note: warnings with same message are deduplicated
    expect(reporter.getErrors().length).toBe(6);
    expect(reporter.getWarnings().length).toBeGreaterThanOrEqual(3);

    // Test throwIfAny - should format errors grouped by file
    expect(() => reporter.throwIfAny()).toThrow(CompileError);

    try {
      reporter.throwIfAny();
    } catch (e: any) {
      // Snapshot the error message to verify formatting
      expect(e.message).toMatchSnapshot('grouped-errors-message');
      expect(e.plainMessage).toMatchSnapshot('grouped-errors-plain-message');
    }

    // Snapshot the collected logs
    expect(logs).toMatchSnapshot('warning-logs');
  });

  it('should handle inContext correctly', () => {
    const reporter = new ErrorReporter(null, [], {
      logger: () => { /* empty */ }
    });

    const contextReporter = reporter.inContext('Processing Users cube');
    contextReporter.error(new Error('Test error'));

    expect(reporter.getErrors()).toMatchSnapshot();
  });

  it('should deduplicate identical errors and warnings', () => {
    const reporter = new ErrorReporter(null, [], {
      logger: () => { /* empty */ }
    });

    reporter.inFile({
      fileName: 'test.js',
      content: 'test content'
    });

    // Add same syntax error twice
    reporter.syntaxError({
      message: 'Duplicate error',
      loc: {
        start: { line: 1, column: 1 },
        end: { line: 1, column: 4 }
      }
    });

    reporter.syntaxError({
      message: 'Duplicate error',
      loc: {
        start: { line: 1, column: 1 },
        end: { line: 1, column: 4 }
      }
    });

    // Add same warning twice
    reporter.warning({
      message: 'Duplicate warning'
    });

    reporter.warning({
      message: 'Duplicate warning'
    });

    expect({
      errors: reporter.getErrors(),
      warnings: reporter.getWarnings()
    }).toMatchSnapshot();
  });

  it('should handle addErrors and addWarnings', () => {
    const reporter = new ErrorReporter(null, [], {
      logger: () => { /* empty */ }
    });

    // Test addErrors with fileName
    reporter.addErrors([
      new Error('Error 1'),
      'Error 2',
      { message: 'Error 3' }
    ], 'batch.js');

    // Test addWarnings
    reporter.addWarnings([
      { message: 'Warning 1' },
      { message: 'Warning 2' }
    ]);

    expect({
      errors: reporter.getErrors(),
      warnings: reporter.getWarnings()
    }).toMatchSnapshot();
  });

  it('should not throw if no errors', () => {
    const reporter = new ErrorReporter(null, [], {
      logger: () => { /* empty */ }
    });

    reporter.warning({ message: 'Just a warning' });

    expect(() => reporter.throwIfAny()).not.toThrow();
  });

  it('should handle errors without fileName at the end', () => {
    const reporter = new ErrorReporter(null, [], {
      logger: () => { /* empty */ }
    });

    reporter.error({ message: 'Error in file A' }, 'fileA.js');
    reporter.error({ message: 'Error in file B' }, 'fileB.js');
    reporter.error({ message: 'Generic error without file' });

    try {
      reporter.throwIfAny();
    } catch (e: any) {
      expect(e.message).toMatchSnapshot();
    }
  });
});
