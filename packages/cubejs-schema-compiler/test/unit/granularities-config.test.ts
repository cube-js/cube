import {
  resolveGlobalGranularities,
  buildBuiltInsCatalog,
  granularityConfigHash,
  isBuiltInGranularity,
  BUILT_IN_GRANULARITY_NAMES,
} from '../../src/compiler/GlobalGranularitiesConfig';

describe('resolveGlobalGranularities', () => {
  const originalEnv = { ...process.env };

  afterEach(() => {
    for (const key of Object.keys(process.env)) {
      if (key.startsWith('CUBEJS_GRANULARITIES')) delete process.env[key];
    }
    Object.assign(process.env, originalEnv);
  });

  it('user value undefined + no env -> defaults to full built-in catalog', async () => {
    const cfg = await resolveGlobalGranularities(undefined, {});
    expect([...cfg.enabledBuiltIns].sort()).toEqual([...BUILT_IN_GRANULARITY_NAMES].sort());
    expect(cfg.customGranularities).toEqual({});
  });

  it('CUBEJS_GRANULARITIES restricts enabled built-ins', async () => {
    process.env.CUBEJS_GRANULARITIES = 'year,quarter,month';
    const cfg = await resolveGlobalGranularities(undefined, {});
    expect(cfg.enabledBuiltIns).toEqual(['year', 'quarter', 'month']);
    expect(cfg.customGranularities).toEqual({});
  });

  it('CUBEJS_GRANULARITIES with a custom name + companion env vars produces a custom granularity', async () => {
    process.env.CUBEJS_GRANULARITIES = 'year,fiscal_year';
    process.env.CUBEJS_GRANULARITIES_FISCAL_YEAR_INTERVAL = '1 year';
    process.env.CUBEJS_GRANULARITIES_FISCAL_YEAR_ORIGIN = '2026-04-01';
    process.env.CUBEJS_GRANULARITIES_FISCAL_YEAR_TITLE = 'Fiscal Year';
    const cfg = await resolveGlobalGranularities(undefined, {});
    expect(cfg.enabledBuiltIns).toEqual(['year']);
    expect(cfg.customGranularities.fiscal_year).toEqual({
      interval: '1 year',
      origin: '2026-04-01',
      title: 'Fiscal Year',
    });
  });

  it('file-config replaces env vars (full replacement, not merge)', async () => {
    process.env.CUBEJS_GRANULARITIES = 'year,quarter,month';
    const cfg = await resolveGlobalGranularities(['day', 'hour'], {});
    expect(cfg.enabledBuiltIns).toEqual(['day', 'hour']);
  });

  it('file-config can mix built-in names + custom objects', async () => {
    const cfg = await resolveGlobalGranularities(
      ['year', { name: 'fiscal_year', interval: '1 year', origin: '2026-04-01' }],
      {},
    );
    expect(cfg.enabledBuiltIns).toEqual(['year']);
    expect(cfg.customGranularities.fiscal_year).toEqual({ interval: '1 year', origin: '2026-04-01' });
  });

  it('file-config function is invoked with the request context', async () => {
    let receivedCtx: any;
    const cfg = await resolveGlobalGranularities(
      (ctx) => {
        receivedCtx = ctx;
        return ['year'];
      },
      { securityContext: { tenant: 't1' } },
    );
    expect(receivedCtx.securityContext.tenant).toBe('t1');
    expect(cfg.enabledBuiltIns).toEqual(['year']);
  });

  it('file-config function may return a Promise', async () => {
    const cfg = await resolveGlobalGranularities(
      async () => ['quarter'],
      {},
    );
    expect(cfg.enabledBuiltIns).toEqual(['quarter']);
  });

  // Regression: CUBEJS_GRANULARITIES_<NAME>_TITLE was dropped for built-in names.
  it('env title/format override applies to a built-in (folded into catalog, stays built-in)', async () => {
    process.env.CUBEJS_GRANULARITIES = 'year,month';
    process.env.CUBEJS_GRANULARITIES_YEAR_TITLE = 'Jaar';
    const cfg = await resolveGlobalGranularities(undefined, {});
    expect(cfg.enabledBuiltIns).toEqual(['year', 'month']);
    const catalog = buildBuiltInsCatalog(cfg);
    expect(catalog.year.title).toBe('Jaar');
    expect(catalog.year.interval).toBe('1 year');
  });

  // Regression: a config-provided interval must not override a built-in's fixed 1-unit interval.
  it('interval override on a built-in is ignored (SQL always buckets 1 unit)', async () => {
    const cfg = await resolveGlobalGranularities([{ name: 'month', interval: '2 months', title: 'Bi' }], {});
    const catalog = buildBuiltInsCatalog(cfg);
    expect(catalog.month.interval).toBe('1 month');
    expect(catalog.month.title).toBe('Bi');
  });

  // Regression: a config custom without an interval is unusable and must be dropped, not advertised.
  it('drops a config custom granularity that has no interval', async () => {
    const cfg = await resolveGlobalGranularities([{ name: 'fiscal_year', title: 'Fiscal Year' }], {});
    expect(cfg.customGranularities.fiscal_year).toBeUndefined();
  });

  // Regression: non-string definition values must not survive (hash/serialize must agree).
  it('sanitizes non-string custom fields so the config hash matches the wire output', async () => {
    const cfgClean = await resolveGlobalGranularities([{ name: 'fy', interval: '1 year', origin: '2024-02-01' }], {});
    const cfgDirty = await resolveGlobalGranularities(
      [{ name: 'fy', interval: '1 year', origin: '2024-02-01', title: 123 as any, junk: () => 'x' } as any],
      {},
    );
    expect(cfgDirty.customGranularities.fy).toEqual({ interval: '1 year', origin: '2024-02-01' });
    // Two configs that serialize identically must hash identically.
    expect(granularityConfigHash(cfgDirty)).toBe(granularityConfigHash(cfgClean));
  });

  // Regression (adversarial): prototype-chain names must not be classified as built-ins.
  it('does not classify prototype-chain names as built-in granularities', () => {
    expect(isBuiltInGranularity('__proto__')).toBe(false);
    expect(isBuiltInGranularity('constructor')).toBe(false);
    expect(isBuiltInGranularity('hasOwnProperty')).toBe(false);
    expect(isBuiltInGranularity('year')).toBe(true);
  });
});
