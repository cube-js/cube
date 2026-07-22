import { SchemaFileRepository } from '@cubejs-backend/shared';
import { CompilerApi } from '../../src/core/CompilerApi';
import { DbTypeInternalFn } from '../../src/core/types';

// CUB-2567 rev 2: global granularity config is resolved ONCE per appId at compile time
// (env | static list | function(ctx)), its hash folded into compilerVersion for ALL forms, and the
// effective per-time-dimension set baked into the compiled model. Global CUSTOM granularities are
// merged into each time dimension's `granularities` symbol map at compile (locals win). Both
// /v1/meta and the SQL path read the baked values — no per-request resolution. This suite verifies
// the bake, the fold-into-compilerVersion, and the resolve-once guarantee.

// A CompilerApi that counts how many times the granularities FUNCTION form is invoked, so we can
// assert it runs once per compile (per appId), not once per metaConfig/getSql call.
class TestableCompilerApi extends CompilerApi {
  public version(): string | undefined {
    return this.compilerVersion;
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
            collide_at: {
              sql: 'created_at',
              type: 'time',
              // Local custom named 'fiscal_year' collides with a same-named global custom.
              granularities: { fiscal_year: { interval: '1 year', origin: '2020-01-01' } },
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
  logger: noopLogger,
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

// The compiled dimension symbol's `granularities` map — the map the SQL layer and the pre-agg
// matcher resolve customs by name from. Global customs are baked into it at compile time.
const bakedDimGranularities = async (api: CompilerApi, dimPath: string): Promise<Record<string, any>> => {
  const compilers = await (api as any).getCompilers();
  return compilers.cubeEvaluator.dimensionByPath(dimPath).granularities || {};
};

// The SEPARATE `symbols[cube][dim]` map that CubeSymbols.resolveGranularity reads for SQL. The
// compile-time bake dual-writes here too; without it SQL can't resolve a baked global custom.
const symbolsDimGranularities = async (api: CompilerApi, cube: string, dim: string): Promise<Record<string, any>> => {
  const compilers = await (api as any).getCompilers();
  return (compilers.cubeEvaluator as any).symbols?.[cube]?.[dim]?.granularities || {};
};

describe('granularities baked at compile time (CUB-2567 rev 2)', () => {
  describe('env / static config', () => {
    afterEach(() => {
      delete process.env.CUBEJS_GRANULARITIES;
    });

    // 1. No config -> every time dimension gets all built-ins baked as effectiveGranularities;
    // internal fields stay off the wire.
    test('no config: every time dimension gets all built-ins baked; internal fields stay off the wire', async () => {
      const api = createApi();
      const cubes = await api.metaConfig(ctxFor('a'), {});
      expect(granularityNames(dimByName(cubes, 'Orders.created_at'))).toEqual(ALL_BUILT_INS);
      // Local custom granularity survives on top of the enabled built-ins.
      expect(granularityNames(dimByName(cubes, 'Events.ts'))).toEqual([...ALL_BUILT_INS, 'fiscal_year']);
      // Non-time dimensions are untouched.
      expect(dimByName(cubes, 'Products.name').effectiveGranularities).toBeUndefined();
      // The raw normalized block never reaches the wire.
      expect(dimByName(cubes, 'Orders.created_at').granularitiesBlock).toBeUndefined();
      api.dispose();
    });

    // 2. Static global customs are baked into BOTH dimension object graphs the downstream paths
    // read: the evaluatedCubes/cubeList copy (dimensionByPath, timeDimensionsForCube, pre-agg
    // matching) AND the separate symbols copy (CubeSymbols.resolveGranularity for SQL). The merge
    // must reach both — writing only the first makes SQL unable to resolve a global custom, and
    // pre-agg matching (granularityHierarchies builds a Granularity for every baked custom) then
    // throws. This test drives a real getSql() on a model with a configured global custom to lock
    // that in, and confirms per-dimension excludes are honored.
    test('static global customs are baked into the consumed map; SQL resolves them; excludes honored', async () => {
      const api = createApi({
        granularities: [{ name: 'fiscal_year', interval: '1 year', origin: '2024-02-01' }],
      });

      // Baked into a plain dimension's granularities map (the map SQL / pre-agg matching read).
      const baked = await bakedDimGranularities(api, 'Orders.created_at');
      expect(baked.fiscal_year).toMatchObject({ interval: '1 year', origin: '2024-02-01' });

      // The dimension that excludes the global custom does NOT get it baked in, matching meta.
      const bakedExcluded = await bakedDimGranularities(api, 'Orders.excluded_at');
      expect(bakedExcluded.fiscal_year).toBeUndefined();

      // Meta agrees: excluded dimension omits it; plain dimensions advertise it.
      const cubes = await api.metaConfig(ctxFor('a'), {});
      expect(granularityNames(dimByName(cubes, 'Orders.created_at'))).toContain('fiscal_year');
      expect(granularityNames(dimByName(cubes, 'Orders.excluded_at'))).not.toContain('fiscal_year');

      // Real SQL path: a query at the global custom grain must resolve (both object graphs carry the
      // merge) and not throw during pre-agg matching. Previously this threw "Granularity does not
      // exist" because the symbols copy lacked the merge.
      const { sql } = await api.getSql({
        measures: [],
        dimensions: [],
        segments: [],
        filters: [],
        timeDimensions: [{
          dimension: 'Orders.created_at',
          // Custom granularity name — allowed at runtime; the wire type only enumerates built-ins.
          granularity: 'fiscal_year' as any,
          dateRange: ['2024-01-01', '2025-01-01'],
        }],
        order: [],
      } as any);
      const sqlText = Array.isArray(sql) ? sql[0] : sql;
      expect(typeof sqlText).toBe('string');
      expect(sqlText.length).toBeGreaterThan(0);
      api.dispose();
    });

    // 3. Time dimensions without local customization share ONE default set instance (memory saver).
    test('time dimensions without local customization share one default set instance', async () => {
      const api = createApi();
      const cubes = await api.metaConfig(ctxFor('a'), {});
      const created = dimByName(cubes, 'Orders.created_at');
      const updated = dimByName(cubes, 'Orders.updated_at');
      expect(created.effectiveGranularities).toBe(updated.effectiveGranularities);
      // A dimension with a local block gets its own set, not the shared default.
      expect(dimByName(cubes, 'Events.ts').effectiveGranularities)
        .not.toBe(created.effectiveGranularities);
      api.dispose();
    });

    // 4. Resolved config hash is folded into compilerVersion, so a different env config yields a
    // different compilerVersion (and therefore a recompile / different compiled model).
    //
    // Note: the resolved config is cached per CompilerApi instance for its lifetime (resolved once
    // per appId), so changing the env var on the SAME instance does NOT re-resolve. The fold-into-
    // compilerVersion contract is verified across two instances reading different env values — a
    // process restart / new appId is exactly when env is re-read.
    test('resolved env config hash folds into compilerVersion (different env -> different version)', async () => {
      const apiDefault = createApi();
      const cubesDefault = await apiDefault.metaConfig(ctxFor('a'), {});
      expect(granularityNames(dimByName(cubesDefault, 'Orders.created_at'))).toEqual(ALL_BUILT_INS);
      const versionDefault = apiDefault.version();
      expect(versionDefault).toContain('_gran_');

      process.env.CUBEJS_GRANULARITIES = 'year,month';
      const apiEnv = createApi();
      const cubesEnv = await apiEnv.metaConfig(ctxFor('a'), {});
      // Different resolved config -> different folded hash -> different compilerVersion.
      expect(apiEnv.version()).not.toBe(versionDefault);
      expect(granularityNames(dimByName(cubesEnv, 'Orders.created_at'))).toEqual(['year', 'month']);

      apiDefault.dispose();
      apiEnv.dispose();
    });
  });

  describe('function config resolved once per compile', () => {
    // 5. THE key new-architecture guarantee: the function form is invoked ONCE per compile (per
    // appId), not once per metaConfig/getSql call. Multiple reads on the same instance add zero
    // additional invocations.
    test('function form is invoked once per compile, not per metaConfig/getSql call', async () => {
      let calls = 0;
      const api = createApi({
        granularities: () => {
          calls += 1;
          return ['year', 'month'];
        },
      });

      // First read triggers the single compile-time resolution.
      await api.metaConfig(ctxFor('a'), {});
      expect(calls).toBe(1);

      // Further reads (meta or SQL, any context) reuse the baked compile — no re-invocation.
      await api.metaConfig(ctxFor('b'), {});
      await api.metaConfig(ctxFor('a'), {});
      await api.getSqlGenerator(queryFor('a', 'Orders.created_at', 'month'));
      await api.getSqlGenerator(queryFor('b', 'Orders.created_at', 'year'));
      expect(calls).toBe(1);
      api.dispose();
    });

    // 5b. INTENDED per-appId contract (CUB-2567 rev 2 narrowing decision): within a single appId
    // (one CompilerApi instance), granularities do NOT vary by request securityContext. The
    // function form is resolved ONCE with the instance's compile context, so two requests with
    // DIFFERENT request securityContexts see the SAME baked granularities.
    test('two request contexts on the same appId get identical baked granularities', async () => {
      let seenCtx: any;
      const api = createApi({
        // Would branch per-tenant IF it were called per request — but it is resolved once with the
        // instance's compile context, so both requests below get the compile-context result ('c').
        granularities: (ctx: any) => {
          seenCtx = ctx;
          return ctx?.securityContext?.tenant === 'c' ? ['year', 'quarter'] : ['month'];
        },
        compileContext: { securityContext: { tenant: 'c' } },
      });

      const cubesA = await api.metaConfig(ctxFor('a'), {});
      const cubesB = await api.metaConfig(ctxFor('b'), {});

      // Both requests see the compile-context resolution, not their own securityContext.
      expect(seenCtx?.securityContext?.tenant).toBe('c');
      expect(granularityNames(dimByName(cubesA, 'Orders.created_at'))).toEqual(['year', 'quarter']);
      expect(granularityNames(dimByName(cubesB, 'Orders.created_at')))
        .toEqual(granularityNames(dimByName(cubesA, 'Orders.created_at')));
      api.dispose();
    });

    // 6. Function form baked + folded into compilerVersion: two CompilerApi instances with different
    // appId-level compile contexts (function returns differ) produce a different compilerVersion and
    // different baked meta.
    test('different compile contexts (different function results) -> different version and meta', async () => {
      const granularities = (ctx: any) => (ctx.securityContext.tenant === 'a'
        ? ['year']
        : ['month']);

      const apiA = createApi({ granularities, compileContext: { securityContext: { tenant: 'a' } } });
      const apiB = createApi({ granularities, compileContext: { securityContext: { tenant: 'b' } } });

      const cubesA = await apiA.metaConfig(ctxFor('x'), {});
      const cubesB = await apiB.metaConfig(ctxFor('x'), {});

      // Different baked effective sets.
      expect(granularityNames(dimByName(cubesA, 'Orders.created_at'))).toEqual(['year']);
      expect(granularityNames(dimByName(cubesB, 'Orders.created_at'))).toEqual(['month']);

      // Different compilerVersion (the resolved-config hash is folded in). compilerId is NOT asserted:
      // two independent CompilerApi instances get random UUIDs regardless of granularities, so that
      // check would be vacuous.
      expect(apiA.version()).not.toBe(apiB.version());
      apiA.dispose();
      apiB.dispose();
    });
  });

  describe('meta and SQL read the same baked values', () => {
    // 7. Meta and SQL read the SAME baked values, so a custom the meta advertises on a dimension is
    // exactly the one baked into that dimension's granularities map (the map the SQL path / pre-agg
    // matcher consume) — no meta/SQL contract gap. (Real SQL execution of a baked global custom is
    // exercised in test 2 and the view test; here we assert the per-dimension meta/baked-map parity.)
    test('meta and the baked SQL/matcher map agree on the effective set', async () => {
      const api = createApi({
        granularities: [{ name: 'sprint', interval: '2 weeks', origin: '2024-01-01' }],
      });
      const cubes = await api.metaConfig(ctxFor('a'), {});

      // Plain dimension: meta advertises the global custom, and the baked map carries the same one.
      expect(granularityNames(dimByName(cubes, 'Orders.created_at'))).toContain('sprint');
      const baked = await bakedDimGranularities(api, 'Orders.created_at');
      expect(baked.sprint).toMatchObject({ interval: '2 weeks', origin: '2024-01-01' });

      // Excluded dimension: absent from meta AND absent from the baked map — they agree on the gap.
      expect(granularityNames(dimByName(cubes, 'Orders.excluded_at'))).not.toContain('sprint');
      const bakedExcluded = await bakedDimGranularities(api, 'Orders.excluded_at');
      expect(bakedExcluded.sprint).toBeUndefined();
      api.dispose();
    });
  });

  describe('global customs participate in pre-agg / hierarchy matching', () => {
    // 8a. NEW deliberate behavior: a global custom merged into td.granularities is visible to the
    // granularity-hierarchy / pre-agg matching path. The matcher resolves custom granularities off
    // the compiled dimension symbol's `granularities` map, so this first test locks in that the
    // global custom is present in that map (and the by-reference / copy-on-write sharing). Test 8b
    // then drives the REAL matcher end-to-end.
    test('global custom is baked into the dimension granularities map the matcher consumes', async () => {
      const api = createApi({
        granularities: [{ name: 'sprint', interval: '2 weeks', origin: '2024-01-01' }],
      });
      // Force a compile.
      await api.metaConfig(ctxFor('a'), {});

      const baked = await bakedDimGranularities(api, 'Orders.created_at');
      expect(baked.sprint).toMatchObject({ interval: '2 weeks', origin: '2024-01-01' });

      // Plain dimensions (no local block) share the SAME globalCustoms object by reference — the
      // by-reference merge assigns one shared map to every plain dim (copy-on-write only for locals).
      const bakedUpdated = await bakedDimGranularities(api, 'Orders.updated_at');
      expect(bakedUpdated).toBe(baked);

      // A dimension with a local block gets a fresh copy-on-write object, not the shared map.
      const bakedCollide = await bakedDimGranularities(api, 'Orders.collide_at');
      expect(bakedCollide).not.toBe(baked);

      // A dimension that excludes it does NOT get it baked in.
      const bakedExcluded = await bakedDimGranularities(api, 'Orders.excluded_at');
      expect(bakedExcluded.sprint).toBeUndefined();
      api.dispose();
    });

    // 8b. REAL pre-agg matching: a rollup defined on the GLOBAL-custom grain must be matchable by a
    // query at that grain. This drives the actual matcher end-to-end via getSql (which runs
    // PreAggregations.transformQueryToCanUseForm + canUsePreAggregationForTransformedQueryFn and
    // resolves the custom granularity off the baked map). A matching query uses the pre-agg; a query
    // at an incompatible grain does not.
    const preAggRepository: SchemaFileRepository = {
      localPath: () => '/mock/path',
      dataSchemaFiles: () => Promise.resolve([
        {
          fileName: 'orders.js',
          content: `
            cube('Orders', {
              sql: 'SELECT * FROM orders',
              measures: { count: { type: 'count' } },
              dimensions: { created_at: { sql: 'created_at', type: 'time' } },
              preAggregations: {
                byFiscal: {
                  type: 'rollup',
                  measures: [Orders.count],
                  timeDimension: Orders.created_at,
                  // Rollup granularity is the GLOBAL custom — only resolvable if the bake reached
                  // the map the matcher consumes.
                  granularity: 'fiscal_year',
                  partitionGranularity: 'year',
                },
              },
            });
          `,
        },
      ]),
    };

    const preAggQuery = (grain: string): any => ({
      measures: ['Orders.count'],
      dimensions: [],
      segments: [],
      filters: [],
      timeDimensions: [{
        dimension: 'Orders.created_at',
        // Custom grain name — allowed at runtime; the wire type only enumerates built-ins.
        granularity: grain as any,
        dateRange: ['2024-01-01', '2025-01-01'],
      }],
      order: [],
      timezone: 'UTC',
    });

    test('a rollup on the global-custom grain is matched by a query at that grain', async () => {
      const api = new TestableCompilerApi(preAggRepository, mockDbType, {
        logger: noopLogger,
        granularities: [{ name: 'fiscal_year', interval: '1 year', origin: '2024-01-01' }],
      });

      // Query at the global-custom grain -> the matcher resolves the custom and USES the pre-agg.
      const matchSql = await api.getSql(preAggQuery('fiscal_year'), { includeDebugInfo: true });
      expect(matchSql.preAggregations.map((p: any) => p.preAggregationId)).toContain('Orders.byFiscal');

      // Query at an incompatible grain -> no pre-agg matches.
      const missSql = await api.getSql(preAggQuery('month'), { includeDebugInfo: true });
      expect(missSql.preAggregations).toHaveLength(0);
      api.dispose();
    });
  });

  describe('global custom on a view member (dual-write to symbols)', () => {
    // Orders with a plain time dimension + a view that includes it. A global custom must be baked
    // into the VIEW member's dimension too, in BOTH object graphs (evaluatedCubes AND symbols), so a
    // query against the view member at the global grain resolves in SQL.
    const viewRepository: SchemaFileRepository = {
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
              },
            });
            view('OrdersView', {
              cubes: [
                { join_path: 'Orders', includes: ['count', 'created_at'] },
              ],
            });
          `,
        },
      ]),
    };

    const createViewApi = () => new TestableCompilerApi(viewRepository, mockDbType, {
      logger: noopLogger,
      granularities: [{ name: 'fiscal_year', interval: '1 year', origin: '2024-02-01' }],
    });

    test('global custom is baked into the view member and resolves in SQL', async () => {
      const api = createViewApi();

      // Baked into the view member in BOTH graphs: evaluatedCubes (dimensionByPath / matcher) ...
      const bakedEval = await bakedDimGranularities(api, 'OrdersView.created_at');
      expect(bakedEval.fiscal_year).toMatchObject({ interval: '1 year', origin: '2024-02-01' });
      // ... AND the symbols copy (CubeSymbols.resolveGranularity for SQL) — the load-bearing dual write.
      const bakedSym = await symbolsDimGranularities(api, 'OrdersView', 'created_at');
      expect(bakedSym.fiscal_year).toMatchObject({ interval: '1 year', origin: '2024-02-01' });

      // Meta advertises it on the view member.
      const cubes = await api.metaConfig(ctxFor('a'), {});
      expect(granularityNames(dimByName(cubes, 'OrdersView.created_at'))).toContain('fiscal_year');

      // Real SQL against the VIEW member at the global custom grain resolves and does not throw.
      const { sql } = await api.getSql({
        measures: ['OrdersView.count'],
        dimensions: [],
        segments: [],
        filters: [],
        timeDimensions: [{
          dimension: 'OrdersView.created_at',
          // Custom granularity name — allowed at runtime; the wire type only enumerates built-ins.
          granularity: 'fiscal_year' as any,
          dateRange: ['2024-01-01', '2025-01-01'],
        }],
        order: [],
      } as any);
      const sqlText = Array.isArray(sql) ? sql[0] : sql;
      expect(typeof sqlText).toBe('string');
      expect(sqlText.length).toBeGreaterThan(0);
      api.dispose();
    });
  });

  describe('locals win over globals on name collision', () => {
    // 9. A dimension-local custom with the same name as a global custom keeps the LOCAL definition
    // after the merge (locals always win).
    test('a local custom shadows a same-named global custom after bake', async () => {
      const api = createApi({
        // Global fiscal_year has origin 2024-02-01; the local on Orders.collide_at / Events.ts uses
        // a different origin and must win.
        granularities: [{ name: 'fiscal_year', interval: '1 year', origin: '2024-02-01' }],
      });
      await api.metaConfig(ctxFor('a'), {});

      // Orders.collide_at declares local fiscal_year { origin: 2020-01-01 } -> local wins.
      const collide = await bakedDimGranularities(api, 'Orders.collide_at');
      expect(collide.fiscal_year).toMatchObject({ origin: '2020-01-01' });

      // Events.ts declares local fiscal_year { origin: 2024-02-01 } — same name, local definition
      // preserved (not replaced by the global).
      const eventsTs = await bakedDimGranularities(api, 'Events.ts');
      expect(eventsTs.fiscal_year).toMatchObject({ origin: '2024-02-01' });

      // A plain dimension with no local fiscal_year takes the GLOBAL one.
      const created = await bakedDimGranularities(api, 'Orders.created_at');
      expect(created.fiscal_year).toMatchObject({ origin: '2024-02-01' });
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

    // 10. compilerId still mixes the visibility mask; granularities no longer contribute a separate
    // per-request hash (they live in compilerVersion / the base compilerId). So the composed
    // compilerId is stable across requests with the same visibility and differs with the mask.
    test('compilerId mixes the visibility mask; stable per visibility, differs across masks', async () => {
      const api = new TestableCompilerApi(rbacRepository, mockDbType, {
        logger: noopLogger,
        granularities: ['year', 'month'],
        contextToGroups: async (ctx: any) => (ctx.securityContext.tenant === 'admin' ? ['admin'] : []),
      });

      const base = (await (api as any).getCompilers()).compilerId;

      // RBAC hides `secret` for a non-admin; the composed compilerId differs from the base.
      const nonAdmin = await api.metaConfig(ctxFor('user'), { includeCompilerId: true });
      const secret = dimByName(nonAdmin.cubes, 'Orders.secret');
      expect(secret.isVisible).toBe(false);
      const createdAt = dimByName(nonAdmin.cubes, 'Orders.created_at');
      expect(createdAt.isVisible).toBe(true);
      // Granularities are baked, not per-request — the effective set is still present.
      expect(granularityNames(createdAt)).toEqual(['year', 'month']);
      expect(nonAdmin.compilerId).not.toBe(base);

      // Same visibility mask across two requests -> identical composed compilerId.
      const nonAdmin2 = await api.metaConfig(ctxFor('user2'), { includeCompilerId: true });
      expect(nonAdmin2.compilerId).toBe(nonAdmin.compilerId);
      api.dispose();
    });
  });
});
