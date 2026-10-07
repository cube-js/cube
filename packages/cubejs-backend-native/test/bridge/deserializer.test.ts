import fs from 'fs';
import path from 'path';

import {
  bridgeHarnessAvailable,
  createRustBoxProbe,
  deserializeJson,
  deserializeLoop,
  deserializeSqlTemplates,
  deserializeTyped,
} from './helpers';

const describeBridge = bridgeHarnessAvailable ? describe : describe.skip;

// Shared with benchmarks/deserializer.bench.ts: PostgresQuery#sqlTemplates()
// output, i.e. exactly what Tesseract deserializes on every query.
function loadSqlTemplates(): Record<string, Record<string, string>> {
  const file = path.join(__dirname, '..', '..', 'benchmarks', 'fixtures', 'sql-templates.json');
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

describeBridge('bridge: NativeSerdeDeserializer', () => {
  describe('primitives', () => {
    it.each([
      ['null', null, null],
      ['undefined', undefined, null],
      ['true', true, true],
      ['false', false, false],
      ['empty string', '', ''],
      ['ascii string', 'hello', 'hello'],
      ['unicode string', 'привет 世界', 'привет 世界'],
      ['emoji string', '🙂👍🏽', '🙂👍🏽'],
      ['string with quotes and newlines', 'a "b"\n\'c\'\t\\', 'a "b"\n\'c\'\t\\'],
      ['zero', 0, 0],
      ['negative zero', -0, 0],
      ['positive integer', 42, 42],
      ['negative integer', -7, -7],
      ['max safe integer', Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER],
      ['fraction', 1.5, 1.5],
      ['negative fraction', -0.25, -0.25],
      // serde_json has no representation for non-finite floats.
      ['NaN', NaN, null],
      ['Infinity', Infinity, null],
      ['-Infinity', -Infinity, null],
    ])('%s', (_name, input, expected) => {
      expect(deserializeJson(input)).toEqual(expected);
    });

    it('keeps a long string intact', () => {
      const long = `${'x'.repeat(100_000)}й`;
      expect(deserializeJson(long)).toBe(long);
    });
  });

  describe('containers', () => {
    it('empty array and object', () => {
      expect(deserializeJson([])).toEqual([]);
      expect(deserializeJson({})).toEqual({});
    });

    it('mixed array keeps order and element types', () => {
      expect(deserializeJson([1, 'two', true, null, 2.5, [3], { a: 4 }]))
        .toEqual([1, 'two', true, null, 2.5, [3], { a: 4 }]);
    });

    it('undefined inside an array becomes null', () => {
      expect(deserializeJson([1, undefined, 2])).toEqual([1, null, 2]);
    });

    it('nested objects', () => {
      const input = {
        a: { b: { c: { d: 'deep' } } },
        list: [{ x: 1 }, { y: [2, 3] }],
        flag: false,
      };
      expect(deserializeJson(input)).toEqual(input);
    });

    it('numeric-like, unicode and empty keys', () => {
      const input = { 1: 'one', '01': 'zero-one', ключ: 'значение', '': 'empty', 'a.b': 'dot' };
      expect(deserializeJson(input)).toEqual(input);
    });

    it('object with an explicit undefined value', () => {
      expect(deserializeJson({ a: undefined, b: 1 })).toEqual({ a: null, b: 1 });
    });

    it('reads values through getters', () => {
      const input = {
        get computed() {
          return 'from-getter';
        },
      };
      expect(deserializeJson(input)).toEqual({ computed: 'from-getter' });
    });

    it('rethrows the exception of a throwing getter', () => {
      const input = {
        ok: 1,
        nested: {
          get computed() {
            throw new Error('boom from getter');
          },
        },
      };
      expect(() => deserializeJson(input)).toThrow('boom from getter');
    });

    it('rethrows the exception of a throwing array element getter', () => {
      const input = [1];
      Object.defineProperty(input, 0, {
        get() {
          throw new Error('boom from element');
        },
      });
      expect(() => deserializeJson(input)).toThrow('boom from element');
    });

    it('wide object', () => {
      const input: Record<string, string> = {};

      for (let i = 0; i < 1000; i += 1) {
        input[`key_${i}`] = `value_${i}`;
      }
      expect(deserializeJson(input)).toEqual(input);
    });
  });

  describe('unsupported values', () => {
    it('rejects a function', () => {
      expect(() => deserializeJson(() => 1)).toThrow('deserializer is not implemented');
    });

    it('rejects a Rust box', () => {
      expect(() => deserializeJson(createRustBoxProbe(1, 'box')))
        .toThrow('deserializer is not implemented');
    });

    it('names the offending field', () => {
      expect(() => deserializeJson({ ok: 1, bad: () => 1 }))
        .toThrow('field `bad`: deserializer is not implemented');
    });

    it('names every level of a nested offending field', () => {
      expect(() => deserializeJson({ outer: { inner: () => 1 } }))
        .toThrow('field `outer`: field `inner`: deserializer is not implemented');
    });
  });

  describe('typed struct', () => {
    const base = {
      name: 'probe',
      label: 'lbl',
      ratio: 0.125,
      weight: 2.5,
      ids: [1, -2, 3],
      tags: { b: '2', a: '1' },
    };

    it('deserializes every field', () => {
      expect(deserializeTyped(base)).toEqual(base);
    });

    it('treats null and undefined optionals as None', () => {
      expect(deserializeTyped({ ...base, label: null })).toEqual({ ...base, label: null });
      expect(deserializeTyped({ ...base, label: undefined })).toEqual({ ...base, label: null });
    });

    it('accepts integral numbers for float fields', () => {
      expect(deserializeTyped({ ...base, ratio: 3, weight: 4 }))
        .toEqual({ ...base, ratio: 3, weight: 4 });
    });

    it('rejects a string for a float field', () => {
      expect(() => deserializeTyped({ ...base, ratio: '1' }))
        .toThrow('field `ratio`: JS Number expected for f64 field');
      expect(() => deserializeTyped({ ...base, weight: '1' }))
        .toThrow('field `weight`: JS Number expected for f32 field');
    });

    it('rejects a fractional number for an integer field', () => {
      expect(() => deserializeTyped({ ...base, ids: [1.5] })).toThrow('field `ids`');
    });

    it('rejects a missing required field', () => {
      const { name: _name, ...rest } = base;
      expect(() => deserializeTyped(rest)).toThrow('missing field `name`');
    });

    it('ignores unknown fields', () => {
      expect(deserializeTyped({ ...base, extra: { nested: [1, 2] } })).toEqual(base);
    });
  });

  describe('sql templates', () => {
    it('round-trips the PostgresQuery templates', () => {
      const templates = loadSqlTemplates();
      expect(Object.keys(templates).length).toBeGreaterThan(5);
      expect(deserializeSqlTemplates(templates)).toEqual(templates);
    });

    it('rejects a non-string template', () => {
      expect(() => deserializeSqlTemplates({ functions: { SUM: 1 } }))
        .toThrow('field `functions`: field `SUM`');
    });

    it('loop helper deserializes the same object repeatedly', () => {
      const templates = loadSqlTemplates();
      const groups = Object.keys(templates).length;
      expect(deserializeLoop('sqlTemplates', templates, 3)).toBe(groups * 3);
      expect(deserializeLoop('json', templates, 2)).toBe(groups * 2);
    });
  });
});
