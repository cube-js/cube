import { granularityConfigHash } from '../../src/compiler/GranularityConfigHash';
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
});
