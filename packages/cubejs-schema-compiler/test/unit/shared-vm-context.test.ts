import vm from 'vm';
import { LRUCache } from 'lru-cache';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareCompiler } from './PrepareCompiler';

type Files = { fileName: string, content: string }[];

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
      var legacy = 1;
      function helper() { return TENANT; }

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
    dimensions: [`orders_${tenant}.tenant`],
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

  it('reuses one realm across compiles when on', async () => {
    const realm = await realmOf('a', true);
    expect(await realmOf('b', true)).toBe(realm);
    expect(realm).not.toBe(Function);
  });
});
