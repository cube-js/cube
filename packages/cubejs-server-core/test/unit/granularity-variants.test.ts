import { SchemaFileRepository } from '@cubejs-backend/shared';
import type { Compiler, GlobalGranularitiesConfig } from '@cubejs-backend/schema-compiler';
import { CompilerApi } from '../../src/core/CompilerApi';
import { DbTypeInternalFn } from '../../src/core/types';

class TestableCompilerApi extends CompilerApi {
  public buildCount = 0;

  public failNextBuild = false;

  public defsBuildCount = 0;

  protected buildGranularityVariant(compilers: Compiler, config: GlobalGranularitiesConfig): any[] {
    if (this.failNextBuild) {
      this.failNextBuild = false;
      throw new Error('injected variant build failure');
    }
    this.buildCount++;
    return super.buildGranularityVariant(compilers, config);
  }

  protected buildGranularityDefinitions(compilers: Compiler, config: GlobalGranularitiesConfig): any {
    this.defsBuildCount++;
    return super.buildGranularityDefinitions(compilers, config);
  }

  public version(): string | undefined {
    return this.compilerVersion;
  }

  public async variantCache(): Promise<Map<string, Promise<any[]>> | undefined> {
    return (await this.getCompilers()).granularityVariants;
  }

  public async definitionsCache(): Promise<Map<string, any> | undefined> {
    return (await this.getCompilers()).granularityDefinitions;
  }
}

const repository: SchemaFileRepository = {
  localPath: () => '/mock/path',
  dataSchemaFiles: () => Promise.resolve([
    {
      fileName: 'orders.js',
      content: `
        cube('Orders', {
          sql: 'SELECT * FROM orders',
          measures: { count: { type: 'count' } },
          dimensions: {
            id: { sql: 'id', type: 'number', primaryKey: true },
            created_at: { sql: 'created_at', type: 'time' },
            updated_at: { sql: 'updated_at', type: 'time' },
            excluded_at: {
              sql: 'updated_at',
              type: 'time',
              granularities: { excludes: ['fiscal_year', 'sprint'] },
            },
          },
        });
        cube('Events', {
          sql: 'SELECT * FROM events',
          measures: { count: { type: 'count' } },
          dimensions: {
            ts: {
              sql: 'ts',
              type: 'time',
              granularities: {
                fiscal_year: { interval: '1 year', origin: '2024-02-01' },
              },
            },
          },
        });
        cube('Products', {
          sql: 'SELECT * FROM products',
          measures: { count: { type: 'count' } },
          dimensions: {
            name: { sql: 'name', type: 'string' },
          },
        });
      `,
    },
  ]),
};

const mockDbType: DbTypeInternalFn = async () => 'postgres';

const noopLogger = () => { /* silent */ };

const createApi = (options: any = {}) => new TestableCompilerApi(repository, mockDbType, {
  logger: options.capturedLogs
    ? (msg: string, params: any) => options.capturedLogs.push({ msg, params })
    : noopLogger,
  ...options,
});

const ctxFor = (tenant: string) => ({ securityContext: { tenant }, requestId: `req-${tenant}` });

const dimByName = (cubes: any[], name: string) => cubes
  .flatMap((c: any) => c.config.dimensions)
  .find((d: any) => d.name === name);

const granularityNames = (dim: any) => dim.effectiveGranularities.map((g: any) => g.name);

const ALL_BUILT_INS = ['year', 'quarter', 'month', 'week', 'day', 'hour', 'minute', 'second'];

const queryFor = (tenant: string, dimension: string, granularity: string): any => ({
  measures: [dimension.startsWith('Events.') ? 'Events.count' : 'Orders.count'],
  timeDimensions: [{ dimension, granularity }],
  timezone: 'UTC',
  contextSymbols: { securityContext: { tenant } },
  requestId: `sql-${tenant}-${dimension}-${granularity}`,
});

describe('granularity variants in CompilerApi', () => {
  describe('env/static configs (baked at compile time)', () => {
    afterEach(() => {
      delete process.env.CUBEJS_GRANULARITIES;
    });

    test('no config: every time dimension gets all built-ins, no variant builds happen', async () => {
      const api = createApi();
      const cubes = await api.metaConfig(ctxFor('a'), {});
      expect(granularityNames(dimByName(cubes, 'Orders.created_at'))).toEqual(ALL_BUILT_INS);
      // Local custom granularity survives on top of the enabled built-ins.
      expect(granularityNames(dimByName(cubes, 'Events.ts'))).toEqual([...ALL_BUILT_INS, 'fiscal_year']);
      // Non-time dimensions are untouched.
      expect(dimByName(cubes, 'Products.name').effectiveGranularities).toBeUndefined();

      await api.metaConfig(ctxFor('b'), {});
      expect(api.buildCount).toBe(0);
      expect(await api.variantCache()).toBeUndefined();
      api.dispose();
    });

    test('static list is baked in and internal fields stay off the wire', async () => {
      const api = createApi({ granularities: ['year', { name: 'half', interval: '6 months' }] });
      const cubes = await api.metaConfig(ctxFor('a'), {});
      const dim = dimByName(cubes, 'Orders.created_at');
      expect(dim.effectiveGranularities).toEqual([
        { name: 'year', type: 'built-in', title: 'Year', interval: '1 year', format: '%Y' },
        { name: 'half', type: 'custom', title: 'half', interval: '6 months' },
      ]);
      expect(dim.granularitiesBlock).toBeUndefined();
      // Legacy shape for the customized dimension is preserved (deprecated but not broken).
      const eventsTs = dimByName(cubes, 'Events.ts');
      expect(eventsTs.granularities).toEqual([
        { name: 'fiscal_year', title: 'Fiscal Year', interval: '1 year', offset: undefined, origin: '2024-02-01' },
      ]);
      expect(api.buildCount).toBe(0);
      api.dispose();
    });

    test('static global customs resolve in SQL, while dimension excludes and legacy behavior remain intact', async () => {
      const api = createApi({
        granularities: [{ name: 'fiscal_year', interval: '1 year', origin: '2024-02-01' }],
      });

      const globalSql = await api.getSql(queryFor('a', 'Orders.created_at', 'fiscal_year'));
      expect(globalSql.sql[0]).toContain('created_at');
      await expect(api.getSql(queryFor('a', 'Orders.excluded_at', 'fiscal_year')))
        .rejects.toThrow('Granularity "fiscal_year" does not exist in dimension Orders.excluded_at');

      // Existing local customs and predefined granularities still use their original paths.
      const localSql = await api.getSql(queryFor('a', 'Events.ts', 'fiscal_year'));
      expect(localSql.sql[0]).toContain('ts');
      const builtInSql = await api.getSql(queryFor('a', 'Orders.updated_at', 'day'));
      expect(builtInSql.sql[0]).toContain('updated_at');
      api.dispose();
    });

    test('time dimensions without customization share one default set instance', async () => {
      const api = createApi();
      const cubes = await api.metaConfig(ctxFor('a'), {});
      const created = dimByName(cubes, 'Orders.created_at');
      const updated = dimByName(cubes, 'Orders.updated_at');
      expect(created.effectiveGranularities).toBe(updated.effectiveGranularities);
      expect(dimByName(cubes, 'Events.ts').effectiveGranularities)
        .not.toBe(created.effectiveGranularities);
      api.dispose();
    });

    test('the resolved config hash is folded into compilerVersion, so an env change recompiles', async () => {
      const api = createApi();
      let cubes = await api.metaConfig(ctxFor('a'), {});
      expect(granularityNames(dimByName(cubes, 'Orders.created_at'))).toEqual(ALL_BUILT_INS);
      const versionBefore = api.version();
      expect(versionBefore).toContain('_gran_');

      process.env.CUBEJS_GRANULARITIES = 'year,month';
      cubes = await api.metaConfig(ctxFor('a'), {});
      expect(api.version()).not.toBe(versionBefore);
      expect(granularityNames(dimByName(cubes, 'Orders.created_at'))).toEqual(['year', 'month']);
      api.dispose();
    });
  });

  describe('function config (per-request variants)', () => {
    const perTenant = (ctx: any) => (ctx.securityContext.tenant === 'a'
      ? ['year', 'month']
      : ['week', { name: 'sprint', interval: '2 weeks' }]);

    test('tenants get their own sets; repeats and alternation hit the cache; no leaks', async () => {
      const api = createApi({ granularities: perTenant });

      for (let i = 0; i < 3; i++) {
        const cubesA = await api.metaConfig(ctxFor('a'), {});
        expect(granularityNames(dimByName(cubesA, 'Orders.created_at'))).toEqual(['year', 'month']);
        expect(granularityNames(dimByName(cubesA, 'Events.ts'))).toEqual(['year', 'month', 'fiscal_year']);

        const cubesB = await api.metaConfig(ctxFor('b'), {});
        expect(granularityNames(dimByName(cubesB, 'Orders.created_at'))).toEqual(['week', 'sprint']);
        expect(granularityNames(dimByName(cubesB, 'Events.ts'))).toEqual(['week', 'sprint', 'fiscal_year']);
      }

      expect(api.buildCount).toBe(2);
      expect((await api.variantCache())!.size).toBe(2);
      api.dispose();
    });

    test('context-function global customs resolve in SQL without crossing dimension exclusions', async () => {
      const api = createApi({ granularities: perTenant });

      await expect(api.getSql(queryFor('a', 'Orders.created_at', 'sprint')))
        .rejects.toThrow('Granularity "sprint" does not exist in dimension Orders.created_at');
      const sql = await api.getSql(queryFor('b', 'Orders.created_at', 'sprint'));
      expect(sql.sql[0]).toContain('created_at');
      await expect(api.getSql(queryFor('b', 'Orders.excluded_at', 'sprint')))
        .rejects.toThrow('Granularity "sprint" does not exist in dimension Orders.excluded_at');
      await expect(api.getSql(queryFor('a', 'Orders.created_at', 'sprint')))
        .rejects.toThrow('Granularity "sprint" does not exist in dimension Orders.created_at');
      api.dispose();
    });

    test('base meta cubes are never mutated by variant enrichment', async () => {
      const api = createApi({ granularities: perTenant });
      await api.metaConfig(ctxFor('a'), {});
      const compilers = await (api as any).getCompilers();
      const baseDim = dimByName(compilers.metaTransformer.cubes, 'Orders.created_at');
      expect(baseDim.effectiveGranularities).toBeUndefined();
      api.dispose();
    });

    // Regression: meta and SQL paths must resolve the config from the SAME securityContext-only
    // view. A function keyed on securityContext advertises `sprint` for tenant b in meta AND
    // resolves it in SQL — no path can advertise a custom the other can't execute.
    test('meta and SQL paths agree on the effective set (securityContext-keyed function)', async () => {
      const api = createApi({ granularities: perTenant });
      const cubes = await api.metaConfig(ctxFor('b'), {});
      expect(granularityNames(dimByName(cubes, 'Orders.created_at'))).toContain('sprint');
      // Same tenant, SQL path: the advertised custom actually resolves.
      const sql = await api.getSql(queryFor('b', 'Orders.created_at', 'sprint'));
      expect(sql.sql[0]).toContain('created_at');
      api.dispose();
    });

    test('distinct compilerIds per tenant, both distinct from the base', async () => {
      const api = createApi({ granularities: perTenant });
      const a = await api.metaConfig(ctxFor('a'), { includeCompilerId: true });
      const b = await api.metaConfig(ctxFor('b'), { includeCompilerId: true });
      const base = (await (api as any).getCompilers()).compilerId;
      expect(a.compilerId).not.toBe(b.compilerId);
      expect(a.compilerId).not.toBe(base);
      expect(b.compilerId).not.toBe(base);
      // Same tenant, same id — stable across calls.
      const a2 = await api.metaConfig(ctxFor('a'), { includeCompilerId: true });
      expect(a2.compilerId).toBe(a.compilerId);
      api.dispose();
    });

    test('static and function forms producing the same list emit identical meta', async () => {
      const list = ['year', 'month', { name: 'half', interval: '6 months', title: 'Half' }];
      const staticApi = createApi({ granularities: list });
      const fnApi = createApi({ granularities: () => list });
      const staticCubes = await staticApi.metaConfig(ctxFor('a'), {});
      const fnCubes = await fnApi.metaConfig(ctxFor('a'), {});
      expect(JSON.parse(JSON.stringify(fnCubes))).toEqual(JSON.parse(JSON.stringify(staticCubes)));
      staticApi.dispose();
      fnApi.dispose();
    });

    test('concurrent identical-config misses dedup to a single build', async () => {
      const api = createApi({ granularities: perTenant });
      await Promise.all(
        Array.from({ length: 10 }, () => api.metaConfig(ctxFor('a'), {}))
      );
      expect(api.buildCount).toBe(1);
      api.dispose();
    });

    test('uncustomized time dimensions share the default set within a variant', async () => {
      const api = createApi({ granularities: perTenant });
      const cubes = await api.metaConfig(ctxFor('a'), {});
      expect(dimByName(cubes, 'Orders.created_at').effectiveGranularities)
        .toBe(dimByName(cubes, 'Orders.updated_at').effectiveGranularities);
      // A cube without time dimensions is passed through by reference, not copied.
      const compilers = await (api as any).getCompilers();
      const baseProducts = compilers.metaTransformer.cubes.find((c: any) => c.config.name === 'Products');
      const variantProducts = (await api.metaConfig(ctxFor('a'), {}))
        .find((c: any) => c.config.name === 'Products');
      expect(variantProducts).toBe(baseProducts);
      api.dispose();
    });

    test('LRU eviction beyond the bound, with a logged warning and rebuild on re-request', async () => {
      const bound = (CompilerApi as any).MAX_GRANULARITY_VARIANTS;
      const capturedLogs: any[] = [];
      const api = createApi({
        capturedLogs,
        granularities: (ctx: any) => [{ name: `g_${ctx.securityContext.tenant}`, interval: '1 week' }],
      });

      for (let i = 0; i < bound + 1; i++) {
        await api.metaConfig(ctxFor(`t${i}`), {});
      }
      expect(api.buildCount).toBe(bound + 1);
      expect((await api.variantCache())!.size).toBe(bound);
      expect(capturedLogs.some(l => l.msg === 'Granularity variant cache is full')).toBe(true);

      // t0 was evicted (least recently used) — asking again rebuilds.
      await api.metaConfig(ctxFor('t0'), {});
      expect(api.buildCount).toBe(bound + 2);
      // The newest entry is still cached — no rebuild.
      await api.metaConfig(ctxFor(`t${bound}`), {});
      expect(api.buildCount).toBe(bound + 2);
      api.dispose();
    });

    test('a failed variant build self-evicts and the next request retries', async () => {
      const api = createApi({ granularities: perTenant });
      api.failNextBuild = true;
      await expect(api.metaConfig(ctxFor('a'), {})).rejects.toThrow('injected variant build failure');
      const cubes = await api.metaConfig(ctxFor('a'), {});
      expect(granularityNames(dimByName(cubes, 'Orders.created_at'))).toEqual(['year', 'month']);
      expect(api.buildCount).toBe(1);
      api.dispose();
    });

    test('a recompile discards the variant cache with the compilers object', async () => {
      let version = 'v1';
      const api = createApi({ granularities: perTenant, schemaVersion: () => version });
      await api.metaConfig(ctxFor('a'), {});
      expect(api.buildCount).toBe(1);

      version = 'v2';
      await api.metaConfig(ctxFor('a'), {});
      expect(api.buildCount).toBe(2);
      expect((await api.variantCache())!.size).toBe(1);
      api.dispose();
    });
  });

  describe('SQL-path global-custom definitions cache', () => {
    const perTenant = (ctx: any) => (ctx.securityContext.tenant === 'a'
      ? [{ name: 'sprint', interval: '2 weeks', origin: '2024-01-01' }]
      : [{ name: 'fortnight', interval: '2 weeks', origin: '2024-01-08' }]);

    test('scan runs once per distinct config, then serves from cache; result is byte-identical', async () => {
      const api = createApi({ granularities: perTenant });

      const a1 = await api.getSql(queryFor('a', 'Orders.created_at', 'sprint'));
      const a2 = await api.getSql(queryFor('a', 'Orders.created_at', 'sprint'));
      // Two identical-config queries → one scan.
      expect(api.defsBuildCount).toBe(1);
      expect(a1.sql[0]).toBe(a2.sql[0]);

      // A different tenant is a distinct config → one more scan, and a second cache entry.
      await api.getSql(queryFor('b', 'Orders.created_at', 'fortnight'));
      expect(api.defsBuildCount).toBe(2);
      expect((await api.definitionsCache())!.size).toBe(2);
      api.dispose();
    });

    test('no scan and no cache entry when no global customs are configured', async () => {
      const api = createApi({ granularities: () => ['year', 'month'] });
      await api.getSql(queryFor('a', 'Orders.created_at', 'month'));
      await api.getSql(queryFor('a', 'Orders.created_at', 'year'));
      expect(api.defsBuildCount).toBe(0);
      expect(await api.definitionsCache()).toBeUndefined();
      api.dispose();
    });

    test('distinct tenants never share a definitions entry (no cross-tenant bleed)', async () => {
      const api = createApi({ granularities: perTenant });
      // Tenant a's custom is `sprint`; tenant b's is `fortnight`. Each resolves only its own.
      const aSql = await api.getSql(queryFor('a', 'Orders.created_at', 'sprint'));
      expect(aSql.sql[0]).toContain('created_at');
      await expect(api.getSql(queryFor('a', 'Orders.created_at', 'fortnight')))
        .rejects.toThrow('Granularity "fortnight" does not exist in dimension Orders.created_at');
      const bSql = await api.getSql(queryFor('b', 'Orders.created_at', 'fortnight'));
      expect(bSql.sql[0]).toContain('created_at');
      await expect(api.getSql(queryFor('b', 'Orders.created_at', 'sprint')))
        .rejects.toThrow('Granularity "sprint" does not exist in dimension Orders.created_at');
      api.dispose();
    });

    test('a recompile discards the definitions cache with the compilers object', async () => {
      let version = 'v1';
      const api = createApi({ granularities: perTenant, schemaVersion: () => version });
      await api.getSql(queryFor('a', 'Orders.created_at', 'sprint'));
      expect(api.defsBuildCount).toBe(1);

      version = 'v2';
      await api.getSql(queryFor('a', 'Orders.created_at', 'sprint'));
      expect(api.defsBuildCount).toBe(2);
      expect((await api.definitionsCache())!.size).toBe(1);
      api.dispose();
    });

    test('static config caches a single entry reused across queries', async () => {
      const api = createApi({ granularities: [{ name: 'sprint', interval: '2 weeks', origin: '2024-01-01' }] });
      await api.getSql(queryFor('a', 'Orders.created_at', 'sprint'));
      await api.getSql(queryFor('b', 'Orders.created_at', 'sprint'));
      // Context-independent config → one scan, one entry, regardless of tenant.
      expect(api.defsBuildCount).toBe(1);
      expect((await api.definitionsCache())!.size).toBe(1);
      api.dispose();
    });
  });

  describe('composition with RBAC visibility', () => {
    const rbacRepository: SchemaFileRepository = {
      localPath: () => '/mock/path',
      dataSchemaFiles: () => Promise.resolve([
        {
          fileName: 'orders.js',
          content: `
            cube('Orders', {
              sql: 'SELECT * FROM orders',
              measures: { count: { type: 'count' } },
              dimensions: {
                created_at: { sql: 'created_at', type: 'time' },
                secret: { sql: 'secret', type: 'string' },
              },
              accessPolicy: [
                {
                  group: '*',
                  rowLevel: { allowAll: true },
                  memberLevel: { includes: ['count', 'created_at'] },
                },
              ],
            });
          `,
        },
      ]),
    };

    test('granularity variant selects first, visibility patches on top, compilerId mixes both', async () => {
      const api = new TestableCompilerApi(rbacRepository, mockDbType, {
        logger: noopLogger,
        granularities: (ctx: any) => (ctx.securityContext.tenant === 'a' ? ['year'] : ['month']),
      });

      const result = await api.metaConfig(ctxFor('a'), { includeCompilerId: true });
      const createdAt = dimByName(result.cubes, 'Orders.created_at');
      expect(granularityNames(createdAt)).toEqual(['year']);
      // RBAC hid `secret` but kept the enriched time dimension intact.
      const secret = dimByName(result.cubes, 'Orders.secret');
      expect(secret.isVisible).toBe(false);
      expect(createdAt.isVisible).toBe(true);

      // compilerId differs across tenants (granularity), and from the base (visibility + granularity).
      const base = (await (api as any).getCompilers()).compilerId;
      const resultB = await api.metaConfig(ctxFor('b'), { includeCompilerId: true });
      expect(result.compilerId).not.toBe(resultB.compilerId);
      expect(result.compilerId).not.toBe(base);
      api.dispose();
    });
  });
});
