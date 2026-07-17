import { granularityConfigHash } from '../../src/compiler/GlobalGranularitiesConfig';
import type { GlobalGranularitiesConfig } from '../../src/compiler/GlobalGranularitiesConfig';

const config = (
  enabledBuiltIns: string[],
  customGranularities: Record<string, any> = {},
): GlobalGranularitiesConfig => ({ enabledBuiltIns, customGranularities });

describe('granularityConfigHash', () => {
  it('is stable for identical configs', () => {
    const a = config(['year', 'month'], { fy: { interval: '1 year', origin: '2024-02-01' } });
    const b = config(['year', 'month'], { fy: { interval: '1 year', origin: '2024-02-01' } });
    expect(granularityConfigHash(a)).toBe(granularityConfigHash(b));
  });

  it('is sensitive to built-in order (order is part of the wire contract)', () => {
    expect(granularityConfigHash(config(['year', 'month'])))
      .not.toBe(granularityConfigHash(config(['month', 'year'])));
  });

  it('is sensitive to custom emission order', () => {
    const a = config(['year'], { a: { interval: '1 week' }, b: { interval: '2 week' } });
    const b = config(['year'], { b: { interval: '2 week' }, a: { interval: '1 week' } });
    expect(granularityConfigHash(a)).not.toBe(granularityConfigHash(b));
  });

  it('is sensitive to every known definition field', () => {
    const base = { interval: '1 year', title: 'FY', format: '%Y', offset: undefined, origin: '2024-02-01' };
    const baseHash = granularityConfigHash(config(['year'], { fy: base }));
    for (const [field, value] of [
      ['interval', '2 year'], ['title', 'Fiscal'], ['format', '%y'], ['origin', '2025-02-01'],
    ] as const) {
      expect(granularityConfigHash(config(['year'], { fy: { ...base, [field]: value } })))
        .not.toBe(baseHash);
    }
  });

  it('ignores unknown and non-serializable definition props', () => {
    const plain = config(['year'], { fy: { interval: '1 year' } });
    const dirty = config(['year'], {
      fy: {
        interval: '1 year',
        sql: () => 'now()',
        somethingElse: { nested: true },
        title: (() => 'not a string') as any,
      },
    });
    expect(granularityConfigHash(dirty)).toBe(granularityConfigHash(plain));
  });

  it('distinguishes an absent field from a present one', () => {
    expect(granularityConfigHash(config(['year'], { fy: { interval: '1 year' } })))
      .not.toBe(granularityConfigHash(config(['year'], { fy: { interval: '1 year', title: 'FY' } })));
  });

  // Regression (F6): for a name shadowing a built-in, only title/format affect output — the SQL
  // layer fixes a built-in's interval at `1 <name>`. So changing interval/offset/origin on a
  // built-in override must NOT change the hash (else it churns the cache/compilerId for identical
  // output), while changing title/format must.
  describe('built-in override hashes only the fields that affect output', () => {
    const withYear = (def: any) => config(['year'], { year: def });

    it('ignores interval/offset/origin overrides on a built-in name', () => {
      const baseHash = granularityConfigHash(withYear({ title: 'Year' }));
      expect(granularityConfigHash(withYear({ title: 'Year', interval: '2 years' }))).toBe(baseHash);
      expect(granularityConfigHash(withYear({ title: 'Year', offset: '1 day' }))).toBe(baseHash);
      expect(granularityConfigHash(withYear({ title: 'Year', origin: '2024-02-01' }))).toBe(baseHash);
    });

    it('still reflects title/format overrides on a built-in name', () => {
      const baseHash = granularityConfigHash(withYear({ title: 'Year' }));
      expect(granularityConfigHash(withYear({ title: 'Jaar' }))).not.toBe(baseHash);
      expect(granularityConfigHash(withYear({ title: 'Year', format: '%y' }))).not.toBe(baseHash);
    });

    it('still hashes every field for a real (non-built-in) custom', () => {
      const baseHash = granularityConfigHash(config([], { fy: { interval: '1 year', origin: '2024-02-01' } }));
      expect(granularityConfigHash(config([], { fy: { interval: '2 years', origin: '2024-02-01' } })))
        .not.toBe(baseHash);
      expect(granularityConfigHash(config([], { fy: { interval: '1 year', origin: '2025-02-01' } })))
        .not.toBe(baseHash);
    });
  });
});
