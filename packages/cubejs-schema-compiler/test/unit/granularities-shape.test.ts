import {
  normalizeGranularitiesBlock,
  resolveDimensionGranularities,
} from '../../src/compiler/GranularityResolver';

const BUILT_INS = {
  year: { title: 'Year' },
  quarter: { title: 'Quarter' },
  month: { title: 'Month' },
  week: { title: 'Week' },
  day: { title: 'Day' },
  hour: { title: 'Hour' },
  minute: { title: 'Minute' },
  second: { title: 'Second' },
};

const ALL_ENABLED = Object.keys(BUILT_INS);
const FISCAL_YEAR = { title: 'Fiscal Year', interval: '1 year', origin: '2026-04-01' };
const GLOBAL_CUSTOM = { fiscal_year: FISCAL_YEAR };

describe('normalizeGranularitiesBlock', () => {
  it('treats missing input as wide-open: includes * / no excludes / no custom', () => {
    expect(normalizeGranularitiesBlock(undefined)).toEqual({
      includes: '*',
      excludes: [],
      custom: {},
    });
    expect(normalizeGranularitiesBlock(null)).toEqual({
      includes: '*',
      excludes: [],
      custom: {},
    });
  });

  it('legacy flat-array form maps each entry into custom; includes stays *', () => {
    const out = normalizeGranularitiesBlock([
      { name: 'fiscal_q', interval: '3 months', origin: '2026-04-01' },
    ]);
    expect(out.includes).toBe('*');
    expect(out.excludes).toEqual([]);
    expect(out.custom).toEqual({
      fiscal_q: { interval: '3 months', origin: '2026-04-01' },
    });
  });

  it('post-yamlArrayToObj keyed object is preserved as legacy custom-only block', () => {
    const out = normalizeGranularitiesBlock({
      fiscal_q: { interval: '3 months', origin: '2026-04-01' },
    });
    expect(out.includes).toBe('*');
    expect(out.custom.fiscal_q).toEqual({ interval: '3 months', origin: '2026-04-01' });
  });

  it('new dict shape is canonicalized with defaults', () => {
    const out = normalizeGranularitiesBlock({ includes: ['year'], custom: { fy: FISCAL_YEAR } });
    expect(out).toEqual({
      includes: ['year'],
      excludes: [],
      custom: { fy: FISCAL_YEAR },
    });
  });
});

describe('resolveDimensionGranularities — spec resolution table', () => {
  it('row 1: granularities omitted -> all enabled global granularities', () => {
    const out = resolveDimensionGranularities(
      normalizeGranularitiesBlock(undefined),
      ALL_ENABLED,
      {},
      BUILT_INS,
    );
    expect(Object.keys(out).sort()).toEqual([...ALL_ENABLED].sort());
    expect(out.year.type).toBe('built-in');
  });

  it('row 2: legacy flat array -> enabled globals plus local custom', () => {
    const out = resolveDimensionGranularities(
      normalizeGranularitiesBlock([{ name: 'fiscal_q', interval: '3 months', origin: '2026-04-01' }]),
      ['year', 'month'],
      {},
      BUILT_INS,
    );
    expect(out.year.type).toBe('built-in');
    expect(out.month.type).toBe('built-in');
    expect(out.fiscal_q).toMatchObject({ interval: '3 months', origin: '2026-04-01', type: 'custom' });
  });

  it('row 3: includes [a, b] + custom -> {a, b} plus custom', () => {
    const out = resolveDimensionGranularities(
      normalizeGranularitiesBlock({ includes: ['year', 'quarter'], custom: { fy: FISCAL_YEAR } }),
      ALL_ENABLED,
      {},
      BUILT_INS,
    );
    expect(Object.keys(out).sort()).toEqual(['fy', 'quarter', 'year']);
    expect(out.year.type).toBe('built-in');
    expect(out.fy.type).toBe('custom');
  });

  it('row 4: excludes [x] -> all enabled globals minus x', () => {
    const out = resolveDimensionGranularities(
      normalizeGranularitiesBlock({ excludes: ['day'] }),
      ALL_ENABLED,
      {},
      BUILT_INS,
    );
    expect(out.day).toBeUndefined();
    expect(out.year.type).toBe('built-in');
  });

  it('row 5: excludes "*" + custom -> custom only', () => {
    const out = resolveDimensionGranularities(
      normalizeGranularitiesBlock({ excludes: '*', custom: { fy: FISCAL_YEAR } }),
      ALL_ENABLED,
      {},
      BUILT_INS,
    );
    expect(Object.keys(out)).toEqual(['fy']);
    expect(out.fy.type).toBe('custom');
  });

  it('row 6: includes "*" + custom -> all enabled globals plus custom', () => {
    const out = resolveDimensionGranularities(
      normalizeGranularitiesBlock({ includes: '*', custom: { fy: FISCAL_YEAR } }),
      ['year', 'month'],
      {},
      BUILT_INS,
    );
    expect(Object.keys(out).sort()).toEqual(['fy', 'month', 'year']);
    expect(out.fy.type).toBe('custom');
  });

  it('global custom granularities flow through unless excluded', () => {
    const out = resolveDimensionGranularities(
      normalizeGranularitiesBlock(undefined),
      ['year'],
      GLOBAL_CUSTOM,
      BUILT_INS,
    );
    expect(out.fiscal_year.type).toBe('custom');
    expect(out.year.type).toBe('built-in');
  });

  it('local custom overrides global custom of same name', () => {
    const localFy = { interval: '1 year', origin: '2026-01-01' };
    const out = resolveDimensionGranularities(
      normalizeGranularitiesBlock({ custom: { fiscal_year: localFy } }),
      ['year'],
      GLOBAL_CUSTOM,
      BUILT_INS,
    );
    expect(out.fiscal_year.origin).toBe('2026-01-01');
  });

  it('built-in overridden via global custom stays type built-in (not relabeled custom)', () => {
    // `config.granularities: [{ name: 'year', title: 'Anno' }]` both enables 'year' and puts it in
    // globalCustom; the resolved entry must remain a built-in.
    const out = resolveDimensionGranularities(
      normalizeGranularitiesBlock(undefined),
      ['year'],
      { year: { title: 'Anno' } },
      { ...BUILT_INS, year: { title: 'Anno' } },
    );
    expect(out.year.type).toBe('built-in');
    expect(out.year.title).toBe('Anno');
  });
});

describe('normalizeGranularitiesBlock — reserved-name disambiguation', () => {
  it('custom granularity named "includes" is not misread as the dict form', () => {
    const out = normalizeGranularitiesBlock({
      includes: { interval: '1 year', origin: '2026-04-01' },
    });
    expect(out.includes).toBe('*');
    expect(out.custom.includes).toEqual({ interval: '1 year', origin: '2026-04-01' });
  });

  // Regression: a legacy custom granularity literally named `custom` whose value is a DEFINITION
  // (string-valued fields) must NOT be misread as the dict form's custom map (name -> definition).
  it('custom granularity named "custom" (definition value) is not misread as the dict form', () => {
    const out = normalizeGranularitiesBlock({
      custom: { interval: '1 year', origin: '2026-04-01' },
    });
    expect(out.includes).toBe('*');
    expect(out.custom.custom).toEqual({ interval: '1 year', origin: '2026-04-01' });
  });

  it('custom granularity named "custom" with only an sql function is not misread as the dict form', () => {
    const sql = () => 'date_trunc(\'year\', x)';
    const out = normalizeGranularitiesBlock({ custom: { sql } });
    expect(out.custom.custom).toEqual({ sql });
  });

  // The genuine dict form (custom is a map of name -> definition OBJECT) is still recognized.
  it('genuine dict form with a custom map is recognized', () => {
    const out = normalizeGranularitiesBlock({
      includes: ['year'],
      custom: { fiscal_year: { interval: '1 year', origin: '2026-04-01' } },
    });
    expect(out.includes).toEqual(['year']);
    expect(out.custom.fiscal_year).toEqual({ interval: '1 year', origin: '2026-04-01' });
    expect(out.custom.custom).toBeUndefined();
  });
});
