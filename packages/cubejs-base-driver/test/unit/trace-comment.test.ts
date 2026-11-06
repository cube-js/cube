import { addTraceComment, buildTraceComment } from '../../src';

describe('buildTraceComment', () => {
  test('wraps the id in a block comment', () => {
    expect(buildTraceComment('abc-123')).toBe('/* trace_id: abc-123 */');
  });

  test('keeps every request id shape Cube produces or accepts', () => {
    // W3C traceparent, passed through verbatim by the REST API.
    expect(buildTraceComment('00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'))
      .toBe('/* trace_id: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01 */');
    expect(buildTraceComment('datasources-f47ac10b')).toBe('/* trace_id: datasources-f47ac10b */');
  });

  // The export carries the raw id, so dropping any character the gateway accepts
  // would break the join.
  test('keeps every character the API gateway accepts', () => {
    expect(buildTraceComment('aGVsbG8+/w==')).toBe('/* trace_id: aGVsbG8+/w== */');
    expect(buildTraceComment('user@example.com~1')).toBe('/* trace_id: user@example.com~1 */');
  });

  // The Query History export drops the -span-N suffix when deriving trace_id,
  // so the comment must carry the stem for the join to match.
  test('drops the span suffix', () => {
    expect(buildTraceComment('f47ac10b-58cc-4372-a567-0e02b2c3d479-span-1'))
      .toBe('/* trace_id: f47ac10b-58cc-4372-a567-0e02b2c3d479 */');
  });

  test('drops a uuid span suffix', () => {
    expect(buildTraceComment('conn1-msg2-span-abc-def'))
      .toBe('/* trace_id: conn1-msg2 */');
  });

  test('drops the span suffix of a scheduler id', () => {
    expect(buildTraceComment('scheduler-abc-span-2'))
      .toBe('/* trace_id: scheduler-abc */');
  });

  // The WebSocket gateway validates the client's base id, then appends
  // `-span-<uuid>`, so the full id may legitimately exceed the length cap.
  test('checks the length of the stem, not of the full id', () => {
    const span = '-span-f47ac10b-58cc-4372-a567-0e02b2c3d479';

    expect(buildTraceComment(`${'a'.repeat(100)}${span}`)).toBe(`/* trace_id: ${'a'.repeat(100)} */`);
    expect(buildTraceComment(`${'a'.repeat(128)}${span}`)).toBe(`/* trace_id: ${'a'.repeat(128)} */`);
    expect(buildTraceComment(`${'a'.repeat(129)}${span}`)).toBeNull();
  });

  test('emits nothing for an id the API gateway would reject', () => {
    expect(buildTraceComment('*/ DROP TABLE users; /*')).toBeNull();
    expect(buildTraceComment('abc\n-- evil')).toBeNull();
    expect(buildTraceComment('abc*def')).toBeNull();
    expect(buildTraceComment('CLI REQUEST')).toBeNull();
    expect(buildTraceComment('трейс-id')).toBeNull();
    expect(buildTraceComment('a'.repeat(129))).toBeNull();
  });

  test('emits nothing without an id', () => {
    expect(buildTraceComment(undefined)).toBeNull();
    expect(buildTraceComment(null)).toBeNull();
    expect(buildTraceComment('')).toBeNull();
    expect(buildTraceComment('-span-1')).toBeNull();
  });
});

describe('addTraceComment', () => {
  const sql = 'SELECT 1';

  test('appends the comment', () => {
    expect(addTraceComment(sql, 'abc-123')).toBe('SELECT 1\n/* trace_id: abc-123 */');
  });

  test('appends the trace id of a canonical request id', () => {
    expect(addTraceComment(sql, 'abc-123-span-4')).toBe('SELECT 1\n/* trace_id: abc-123 */');
  });

  test('keeps the comment inside a trailing semicolon', () => {
    expect(addTraceComment('SELECT 1;', 'abc-123')).toBe('SELECT 1\n/* trace_id: abc-123 */;');
    expect(addTraceComment('SELECT 1;  ', 'abc-123')).toBe('SELECT 1\n/* trace_id: abc-123 */;');
  });

  test('never leaves an empty statement after the comment', () => {
    // Cube does not emit repeated separators, but the function has to stay total:
    // a leftover `;` after the comment is a second, empty statement that some
    // engines reject.
    for (const trailing of [';;', '; ;', ';;;', ';\n;  ']) {
      expect(addTraceComment(`SELECT 1${trailing}`, 'abc-123'))
        .toBe('SELECT 1\n/* trace_id: abc-123 */;');
    }
  });

  test('never invents a semicolon the query did not have', () => {
    expect(addTraceComment('SELECT 1 ', 'abc-123')).toBe('SELECT 1 \n/* trace_id: abc-123 */');
    expect(addTraceComment('SELECT 1\n', 'abc-123')).toBe('SELECT 1\n\n/* trace_id: abc-123 */');
  });

  test('stays linear when a long run defeats the anchor', () => {
    // Guards against going back to `;[\s;]*$`, which backtracks quadratically
    // when the run never reaches the anchor — ~1.8s at 60k on uncontrolled input.
    const runaway = `SELECT 1${';'.repeat(60_000)}x`;
    const started = Date.now();

    expect(addTraceComment(runaway, 'abc-123')).toBe(`${runaway}\n/* trace_id: abc-123 */`);
    expect(Date.now() - started).toBeLessThan(1000);
  });

  test('stays linear when the scan walks the whole run', () => {
    // The complement of the case above: here the run reaches the end of the string.
    const semicolons = `SELECT 1${';'.repeat(60_000)}`;
    const spaces = `SELECT 1${' '.repeat(60_000)}`;
    const started = Date.now();

    expect(addTraceComment(semicolons, 'abc-123')).toBe('SELECT 1\n/* trace_id: abc-123 */;');
    expect(addTraceComment(spaces, 'abc-123')).toBe(`${spaces}\n/* trace_id: abc-123 */`);
    expect(Date.now() - started).toBeLessThan(1000);
  });

  test('leaves the query untouched when there is no usable id', () => {
    expect(addTraceComment(sql, undefined)).toBe(sql);
    expect(addTraceComment(sql, '')).toBe(sql);
    expect(addTraceComment(sql, '*/')).toBe(sql);
  });

  test('leaves the query untouched for a hostile request id', () => {
    expect(addTraceComment(sql, '*/ DROP TABLE users; /*')).toBe(sql);
  });

  test('keeps a multi-line query intact', () => {
    const multiline = 'SELECT a,\n  b\nFROM t';
    expect(addTraceComment(multiline, 'abc')).toBe(`${multiline}\n/* trace_id: abc */`);
  });
});
