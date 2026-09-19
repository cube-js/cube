import { MAX_OBJECT_SHAPE_COLUMNS, buildObjectShape } from '../src/object-shape';

const columns = (count: number) => Array.from({ length: count }, (_, i) => `c${i}`);

const fill = (names: ReadonlyArray<string>, values: ReadonlyArray<unknown>) => {
  const shape = buildObjectShape(names);
  const row: Record<string, unknown> = shape === null ? {} : { ...shape };

  for (let i = 0; i < names.length; i++) {
    row[names[i]] = values[i];
  }

  return row;
};

describe('buildObjectShape', () => {
  it('holds every column, with no values of its own', () => {
    const shape = buildObjectShape(['s', 'n']);

    expect(shape).toEqual({ s: null, n: null });
    expect(Object.keys(shape as object)).toEqual(['s', 'n']);
    expect(Object.getPrototypeOf(shape)).toBe(Object.prototype);
  });

  it('declines a result set without columns', () => {
    expect(buildObjectShape([])).toBeNull();
  });

  it('declines past the width V8 keeps in fast mode', () => {
    expect(MAX_OBJECT_SHAPE_COLUMNS).toEqual(127);
    expect(buildObjectShape(columns(127))).not.toBeNull();
    expect(buildObjectShape(columns(128))).toBeNull();
  });

  it('carries any column name a query can alias into', () => {
    // Names reach the shape as JSON string literals, so they arrive from the wire safely.
    const names = [
      'a"; x = 1; //',
      'a`b',
      // eslint-disable-next-line no-template-curly-in-string
      'a${b}',
      'a\nb',
      'a\\b',
      'a b',
      'a\u2028b',
      'constructor',
      'toString',
      'hasOwnProperty',
      ' ',
    ];
    const row = fill(names, names.map((_, i) => `v${i}`));

    expect(Object.keys(row)).toEqual(names);
    names.forEach((name, i) => {
      expect(Object.getOwnPropertyDescriptor(row, name)?.value).toEqual(`v${i}`);
    });
  });

  it('keeps a __proto__ column an own property of the row', () => {
    const row = fill(['__proto__', 'n'], ['pwned', '1']);

    expect(Object.keys(row)).toEqual(['__proto__', 'n']);
    expect(Object.getOwnPropertyDescriptor(row, '__proto__')?.value).toEqual('pwned');
    expect(Object.getPrototypeOf(row)).toBe(Object.prototype);
    expect(({} as any).pwned).toBeUndefined();
    expect(Object.getPrototypeOf({})).toBe(Object.prototype);
  });

  it('reproduces what JSON.parse does to duplicate and integer-like names', () => {
    expect(JSON.stringify(fill(['x', 'y', 'x'], ['1', 'a', '2'])))
      .toEqual(JSON.stringify(JSON.parse('{"x":"1","y":"a","x":"2"}')));
    expect(JSON.stringify(fill(['b', '0'], ['b', 'zero'])))
      .toEqual(JSON.stringify(JSON.parse('{"b":"b","0":"zero"}')));
  });

  it('builds the same row with and without a shape', () => {
    const values = (count: number) => columns(count).map((_, i) => `v${i}`);

    expect(fill(columns(128), values(128)))
      .toEqual({ ...fill(columns(127), values(127)), c127: 'v127' });
  });
});
