import { addTraceComment, buildTraceComment, sanitizeTraceId } from '../../src';

describe('sanitizeTraceId', () => {
  test('keeps every request id shape Cube produces or accepts', () => {
    // Canonical REST/SQL API id.
    expect(sanitizeTraceId('f47ac10b-58cc-4372-a567-0e02b2c3d479-span-1'))
      .toBe('f47ac10b-58cc-4372-a567-0e02b2c3d479-span-1');
    // W3C traceparent, passed through verbatim by the REST API.
    expect(sanitizeTraceId('00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'))
      .toBe('00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01');
    // WebSocket subscriptions use a uuid span suffix on a non-uuid base.
    expect(sanitizeTraceId('conn1-msg2-span-f47ac10b-58cc-4372-a567-0e02b2c3d479'))
      .toBe('conn1-msg2-span-f47ac10b-58cc-4372-a567-0e02b2c3d479');
    expect(sanitizeTraceId('scheduler-f47ac10b-58cc-4372-a567-0e02b2c3d479'))
      .toBe('scheduler-f47ac10b-58cc-4372-a567-0e02b2c3d479');
    expect(sanitizeTraceId('datasources-f47ac10b')).toBe('datasources-f47ac10b');
  });

  test('strips characters that would break out of the comment', () => {
    expect(sanitizeTraceId('abc*/ DROP TABLE users; /*')).toBe('abcDROPTABLEusers');
    expect(sanitizeTraceId('abc\n-- evil')).toBe('abc--evil');
    expect(sanitizeTraceId('abc\r\ndef')).toBe('abcdef');
    expect(sanitizeTraceId('abc\' OR \'1\'=\'1')).toBe('abcOR11');
    expect(sanitizeTraceId('abc def')).toBe('abcdef');
    expect(sanitizeTraceId('трейс-id')).toBe('-id');
  });

  test('strips a leading plus, which reads as an optimizer hint on Hive and Spark', () => {
    expect(sanitizeTraceId('+MAPJOIN(a)')).toBe('MAPJOINa');
    expect(sanitizeTraceId('++abc')).toBe('abc');
  });

  test('returns empty for ids with nothing usable left', () => {
    expect(sanitizeTraceId(undefined)).toBe('');
    expect(sanitizeTraceId(null)).toBe('');
    expect(sanitizeTraceId('')).toBe('');
    expect(sanitizeTraceId('*/')).toBe('');
    expect(sanitizeTraceId('   ')).toBe('');
  });

  test('drops the space in the CLI request id', () => {
    expect(sanitizeTraceId('CLI REQUEST')).toBe('CLIREQUEST');
  });
});

describe('buildTraceComment', () => {
  test('wraps a sanitized id in a block comment', () => {
    expect(buildTraceComment('abc-123')).toBe('/* trace_id: abc-123 */');
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

  test('caps the length of the emitted id', () => {
    expect(buildTraceComment('a'.repeat(500)))
      .toBe(`/* trace_id: ${'a'.repeat(128)} */`);
  });

  // Capping the raw id first would cut this one mid-`-span-` and emit the
  // partial marker, which no longer matches the export's trace_id.
  test('caps after stripping the span, never mid-suffix', () => {
    const traceId = 'a'.repeat(125);

    expect(buildTraceComment(`${traceId}-span-1`)).toBe(`/* trace_id: ${traceId} */`);
  });

  test('returns null when nothing usable remains', () => {
    expect(buildTraceComment(undefined)).toBeNull();
    expect(buildTraceComment('*/')).toBeNull();
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

  test('leaves the query untouched when there is no usable id', () => {
    expect(addTraceComment(sql, undefined)).toBe(sql);
    expect(addTraceComment(sql, '')).toBe(sql);
    expect(addTraceComment(sql, '*/')).toBe(sql);
  });

  test('cannot be escaped by a hostile request id', () => {
    const tagged = addTraceComment(sql, '*/ DROP TABLE users; /*');
    // Exactly one comment open and one close: the payload cannot terminate it.
    expect(tagged.match(/\/\*/g)).toHaveLength(1);
    expect(tagged.match(/\*\//g)).toHaveLength(1);
    expect(tagged).toBe('SELECT 1\n/* trace_id: DROPTABLEusers */');
  });

  test('keeps a multi-line query intact', () => {
    const multiline = 'SELECT a,\n  b\nFROM t';
    expect(addTraceComment(multiline, 'abc')).toBe(`${multiline}\n/* trace_id: abc */`);
  });
});
