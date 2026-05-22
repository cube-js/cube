import { resolveGlobalGranularities, BUILT_IN_GRANULARITY_NAMES } from '../../src/compiler/GlobalGranularitiesConfig';

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
});
