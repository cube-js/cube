import vm from 'vm';
import { LRUCache } from 'lru-cache';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { CubePropContextTranspiler, ImportExportTranspiler, ValidationTranspiler } from '../../src/compiler/transpilers';
import { prepareCompiler } from './PrepareCompiler';

type Files = { fileName: string, content: string }[];

// Every compile starts its own transpiler worker pool (CPU count - 1 threads by default), and the
// tests below run several compiles at once: keep them to one thread each, so this file doesn't
// starve suites that run next to it.
const previousWorkerThreads = process.env.CUBEJS_TRANSPILATION_WORKER_THREADS_COUNT;

beforeAll(() => {
  process.env.CUBEJS_TRANSPILATION_WORKER_THREADS_COUNT = '1';
});

afterAll(() => {
  if (previousWorkerThreads === undefined) {
    delete process.env.CUBEJS_TRANSPILATION_WORKER_THREADS_COUNT;
  } else {
    process.env.CUBEJS_TRANSPILATION_WORKER_THREADS_COUNT = previousWorkerThreads;
  }
});

const tenantFiles = (): Files => [
  {
    fileName: 'helpers.js',
    content: `
      export const tableFor = (name) => name + '_' + COMPILE_CONTEXT.securityContext.tenant;
    `,
  },
  {
    fileName: 'orders.js',
    content: `
      import { tableFor } from './helpers';

      // The same top-level names in every tenant and every recompile.
      const TENANT = COMPILE_CONTEXT.securityContext.tenant;
      let counter = 0;
      // Hoisted declarations: in a shared realm these must not become its globals.
      var LEGACY_TENANT = COMPILE_CONTEXT.securityContext.tenant;
      function helper() { return TENANT; }
      function legacyTenant() { return LEGACY_TENANT; }

      asyncModule(async () => {
        // Let concurrent compiles interleave.
        for (let i = 0; i < 5; i += 1) await null;
        counter += 1;

        cube(\`orders_\${helper()}\`, {
          sql_table: tableFor('orders'),

          measures: {
            count: { type: 'count' },
          },

          dimensions: {
            id: { sql: 'id', type: 'number', primary_key: true },
            // Evaluated lazily, when SQL is generated after the compile has finished.
            tenant: { sql: \`'\${COMPILE_CONTEXT.securityContext.tenant}'\`, type: 'string' },
            legacy: { sql: () => \`'legacy_\${LEGACY_TENANT}_\${legacyTenant()}'\`, type: 'string' },
          },
        });
      });
    `,
  },
];

const compileTenant = async (tenant: string, files: Files, options: Record<string, any>) => {
  const prepared = prepareCompiler(files, {
    compileContext: { securityContext: { tenant } },
    ...options,
  });
  await prepared.compiler.compile();
  return prepared;
};

const buildSql = ({ joinGraph, cubeEvaluator, compiler }, tenant: string) => new PostgresQuery(
  { joinGraph, cubeEvaluator, compiler },
  {
    measures: [`orders_${tenant}.count`],
    dimensions: [`orders_${tenant}.tenant`, `orders_${tenant}.legacy`],
  }
).buildSqlAndParams()[0];

describe.each([
  ['off', false],
  ['on', true],
])('Compile isolation, shared VM context %s', (_name, sharedVmContext) => {
  const options = { sharedVmContext };

  it('compiles the same top-level declarations twice', async () => {
    const first = await compileTenant('a', tenantFiles(), options);
    const second = await compileTenant('a', tenantFiles(), options);

    expect(first.cubeEvaluator.cubeNames()).toEqual(['orders_a']);
    expect(second.cubeEvaluator.cubeNames()).toEqual(['orders_a']);
  });

  it('keeps concurrent tenants apart, including lazy COMPILE_CONTEXT reads', async () => {
    const tenants = ['a', 'b', 'c', 'd'];
    const compiled = await Promise.all(tenants.map(t => compileTenant(t, tenantFiles(), options)));

    compiled.forEach((c, i) => {
      const tenant = tenants[i];
      expect(c.cubeEvaluator.cubeNames()).toEqual([`orders_${tenant}`]);

      const sql = buildSql(c, tenant);
      expect(sql).toContain(`orders_${tenant}`);
      expect(sql).toContain(`'${tenant}'`);
      tenants.filter(t => t !== tenant).forEach((other) => {
        expect(sql).not.toContain(`'${other}'`);
      });
    });
  });

  it('keeps concurrent tenants apart when they share one script and YAML cache', async () => {
    // As CUBEJS_COMPILER_MULTI_TENANT_SHARING does: the same vm.Script runs for every tenant
    const shared = {
      compiledScriptCache: new LRUCache<string, vm.Script>({ max: 250 }),
      compiledYamlCache: new LRUCache<string, string>({ max: 250 }),
    };
    const tenants = ['a', 'b', 'c', 'd'];
    const compiled = await Promise.all(tenants.map(t => compileTenant(t, tenantFiles(), { ...options, ...shared })));
    const entries = shared.compiledScriptCache.size;

    compiled.forEach((c, i) => {
      const tenant = tenants[i];
      expect(c.cubeEvaluator.cubeNames()).toEqual([`orders_${tenant}`]);
      const sql = buildSql(c, tenant);
      expect(sql).toContain(`'${tenant}'`);
      tenants.filter(t => t !== tenant).forEach((other) => {
        expect(sql).not.toContain(`'${other}'`);
      });
    });

    await compileTenant('e', tenantFiles(), { ...options, ...shared });
    // Byte-identical files: the fifth tenant adds no script
    expect(shared.compiledScriptCache.size).toBe(entries);
  });

  it('keeps top-level var and function declarations per compile', async () => {
    const a = await compileTenant('a', tenantFiles(), options);
    const b = await compileTenant('b', tenantFiles(), options);

    // Read lazily, after the other tenant's compile ran the same declarations
    expect(buildSql(a, 'a')).toContain(`'legacy_a_a'`);
    expect(buildSql(b, 'b')).toContain(`'legacy_b_b'`);
    expect(buildSql(a, 'a')).not.toContain('legacy_b');

    const concurrent = await Promise.all(['c', 'd', 'e'].map(t => compileTenant(t, tenantFiles(), options)));
    ['c', 'd', 'e'].forEach((t, i) => expect(buildSql(concurrent[i], t)).toContain(`'legacy_${t}_${t}'`));
  });

  it('resolves a bare view_group reference', async () => {
    const { metaTransformer } = await compileTenant('a', [{
      fileName: 'model.js',
      content: `
        cube('Orders', {
          sql: 'select * from orders',
          measures: { count: { type: 'count' } },
          dimensions: { id: { type: 'number', sql: 'id', primaryKey: true } },
        });

        view_group('sales', { title: 'Sales' });

        view('revenue', {
          viewGroup: sales,
          cubes: [{ joinPath: Orders, includes: '*' }],
        });
      `,
    }], options);

    const revenue = metaTransformer.cubes.find(c => c.config.name === 'revenue');
    expect(revenue?.config.viewGroups).toEqual(['sales']);
  });

  it('does not leak implicit globals or view_group names into the next compile', async () => {
    await compileTenant('a', [{
      fileName: 'model.js',
      content: `
        leakedValue = 'from-a';
        cube('Orders', {
          sql: 'select * from orders',
          measures: { count: { type: 'count' } },
        });
        view_group('leaked_group', {});
      `,
    }], options);

    const second = prepareCompiler([{
      fileName: 'model.js',
      content: `
        const seen = leakedValue;
        cube('Orders', {
          sql: \`select '\${seen}' from orders\`,
          measures: { count: { type: 'count' } },
        });
      `,
    }], { ...options, compileContext: { securityContext: { tenant: 'b' } } });

    await expect(second.compiler.compile()).rejects.toThrow(/leakedValue is not defined/);

    const third = prepareCompiler([{
      fileName: 'model.js',
      content: `
        cube('Orders', {
          sql: 'select * from orders',
          measures: { count: { type: 'count' } },
        });
        view('v', {
          viewGroup: leaked_group,
          cubes: [{ joinPath: Orders, includes: '*' }],
        });
      `,
    }], { ...options, compileContext: { securityContext: { tenant: 'c' } } });

    await expect(third.compiler.compile()).rejects.toThrow(/leaked_group/);
  });
});

describe('Shared VM context realm', () => {
  const realmOf = async (tenant: string, sharedVmContext: boolean) => {
    const { cubeEvaluator } = await compileTenant(tenant, tenantFiles(), { sharedVmContext });
    const cube = cubeEvaluator.cubeFromPath(`orders_${tenant}`);
    // Function#constructor identifies the realm a member's sql closure was created in.
    return cube.dimensions.tenant.sql.constructor;
  };

  it('creates a new realm per compile when off', async () => {
    expect(await realmOf('a', false)).not.toBe(await realmOf('b', false));
  });

  it('is on by default with CUBEJS_COMPILER_MULTI_TENANT_SHARING', async () => {
    const previous = process.env.CUBEJS_COMPILER_MULTI_TENANT_SHARING;

    try {
      process.env.CUBEJS_COMPILER_MULTI_TENANT_SHARING = 'true';
      const { cubeEvaluator: a } = await compileTenant('a', tenantFiles(), {});
      const { cubeEvaluator: b } = await compileTenant('b', tenantFiles(), {});
      expect(b.cubeFromPath('orders_b').dimensions.tenant.sql.constructor)
        .toBe(a.cubeFromPath('orders_a').dimensions.tenant.sql.constructor);

      process.env.CUBEJS_COMPILER_MULTI_TENANT_SHARING = 'false';
      const { cubeEvaluator: c } = await compileTenant('c', tenantFiles(), {});
      expect(c.cubeFromPath('orders_c').dimensions.tenant.sql.constructor)
        .not.toBe(a.cubeFromPath('orders_a').dimensions.tenant.sql.constructor);
    } finally {
      if (previous === undefined) {
        delete process.env.CUBEJS_COMPILER_MULTI_TENANT_SHARING;
      } else {
        process.env.CUBEJS_COMPILER_MULTI_TENANT_SHARING = previous;
      }
    }
  });

  it('leaves no declarations or compile scope on the shared realm global', async () => {
    const { cubeEvaluator } = await compileTenant('warmup', tenantFiles(), { sharedVmContext: true });
    const realmFunction = cubeEvaluator.cubeFromPath('orders_warmup').dimensions.tenant.sql.constructor;
    const sharedGlobal = realmFunction('return globalThis')();
    const before = Reflect.ownKeys(sharedGlobal);

    await compileTenant('a', tenantFiles(), { sharedVmContext: true });
    await Promise.all(['b', 'c'].map(t => compileTenant(t, tenantFiles(), { sharedVmContext: true })));

    expect(Reflect.ownKeys(sharedGlobal)).toEqual(before);
    expect(before).not.toEqual(expect.arrayContaining(['LEGACY_TENANT', 'helper', 'legacyTenant', 'TENANT']));
  });

  it('keeps declarations per compile without the IIFE transpiler', async () => {
    // The shared-realm wrapper must scope declarations by itself, not rely on IIFETranspiler.
    // Only the names reach the transpiler workers.
    const transpilers = [
      new ValidationTranspiler(),
      new ImportExportTranspiler(),
      Object.create(CubePropContextTranspiler.prototype),
    ];
    const withoutIife = { sharedVmContext: true, transpilers };
    const { cubeEvaluator } = await compileTenant('warmup', tenantFiles(), withoutIife);
    const realmFunction = cubeEvaluator.cubeFromPath('orders_warmup').dimensions.tenant.sql.constructor;
    const before = Reflect.ownKeys(realmFunction('return globalThis')());

    const a = await compileTenant('a', tenantFiles(), withoutIife);
    const b = await compileTenant('b', tenantFiles(), withoutIife);
    expect(buildSql(a, 'a')).toContain(`'legacy_a_a'`);
    expect(buildSql(b, 'b')).toContain(`'legacy_b_b'`);

    const concurrent = await Promise.all(['c', 'd'].map(t => compileTenant(t, tenantFiles(), withoutIife)));
    ['c', 'd'].forEach((t, i) => expect(buildSql(concurrent[i], t)).toContain(`'legacy_${t}_${t}'`));

    expect(Reflect.ownKeys(realmFunction('return globalThis')())).toEqual(before);
  });

  it('reuses one realm across compiles when on', async () => {
    const realm = await realmOf('a', true);
    expect(await realmOf('b', true)).toBe(realm);
    expect(realm).not.toBe(Function);
  });
});
