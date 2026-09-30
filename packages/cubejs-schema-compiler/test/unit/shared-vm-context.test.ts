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

const globalWriterFilesForRealm = (): Files => [{
  fileName: 'model.js',
  content: `
    this.tableSuffix = COMPILE_CONTEXT.securityContext.tenant;
    globalThis.otherSuffix = COMPILE_CONTEXT.securityContext.tenant;
    cube(\`orders_\${tableSuffix}\`, {
      sql_table: 'orders',
      measures: { count: { type: 'count' } },
      dimensions: { tenant: { sql: () => \`'\${tableSuffix}_\${otherSuffix}'\`, type: 'string' } },
    });
  `,
}];

const builtInFiles = (reassign: boolean): Files => [{
  fileName: 'model.js',
  content: `
    ${reassign ? `
    JSON = { stringify: () => 'fakejson' };
    this.Math = { max: () => -1 };
    escape = () => 'fakeescape';
    ` : ''}
    const atCompile = [JSON.stringify({ a: 1 }), Math.max(1, 2), escape('a b')].join('|');
    cube(\`orders_\${COMPILE_CONTEXT.securityContext.tenant}\`, {
      sql_table: 'orders',
      measures: { count: { type: 'count' } },
      dimensions: {
        id: { sql: 'id', type: 'number', primary_key: true },
        tenant: { sql: () => \`'\${atCompile}#\${[JSON.stringify({ b: 2 }), Math.max(3, 4), escape('c d')].join('|')}'\`, type: 'string' },
      },
    });
  `,
}];

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

  const globalWriterFiles = (): Files => [{
    fileName: 'model.js',
    content: `
      this.tableSuffix = COMPILE_CONTEXT.securityContext.tenant;
      globalThis.otherSuffix = COMPILE_CONTEXT.securityContext.tenant;
      cube(\`orders_\${tableSuffix}\`, {
        sql_table: 'orders',
        measures: { count: { type: 'count' } },
        dimensions: {
          id: { sql: 'id', type: 'number', primary_key: true },
          tenant: { sql: () => \`'\${tableSuffix}_\${otherSuffix}'\`, type: 'string' },
        },
      });
    `,
  }];

  it('keeps globals written through this and globalThis per compile', async () => {
    const a = await compileTenant('a', globalWriterFiles(), options);
    const b = await compileTenant('b', globalWriterFiles(), options);
    const concurrent = await Promise.all(['c', 'd'].map(t => compileTenant(t, globalWriterFiles(), options)));

    const sqlOf = (c, tenant: string) => new PostgresQuery(c, {
      measures: [`orders_${tenant}.count`],
      dimensions: [`orders_${tenant}.tenant`],
    }).buildSqlAndParams()[0];
    expect(sqlOf(a, 'a')).toContain(`'a_a'`);
    expect(sqlOf(b, 'b')).toContain(`'b_b'`);
    ['c', 'd'].forEach((t, i) => expect(sqlOf(concurrent[i], t)).toContain(`'${t}_${t}'`));

    // A tenant that doesn't write them doesn't see them, as with a realm of its own
    for (const read of ['tableSuffix', 'otherSuffix']) {
      const reader = prepareCompiler([{
        fileName: 'model.js',
        content: `
          const seen = ${read};
          cube('Orders', { sql: \`select '\${seen}' from orders\`, measures: { count: { type: 'count' } } });
        `,
      }], { ...options, compileContext: { securityContext: { tenant: 'e' } } });
      await expect(reader.compiler.compile()).rejects.toThrow(/tableSuffix|otherSuffix/);
    }
  });

  it('keeps a reassigned built-in to the compile that reassigned it', async () => {
    const sqlOf = (c, tenant: string) => new PostgresQuery(c, {
      measures: [`orders_${tenant}.count`],
      dimensions: [`orders_${tenant}.tenant`],
    }).buildSqlAndParams()[0];

    const a = await compileTenant('a', builtInFiles(true), options);
    const b = await compileTenant('b', builtInFiles(false), options);
    const [c, d] = await Promise.all([
      compileTenant('c', builtInFiles(true), options),
      compileTenant('d', builtInFiles(false), options),
    ]);

    const fake = `'fakejson|-1|fakeescape#fakejson|-1|fakeescape'`;
    const real = `'{"a":1}|2|a%20b#{"b":2}|4|c%20d'`;
    // Read lazily, at query time, after every compile has finished
    expect(sqlOf(a, 'a')).toContain(fake);
    expect(sqlOf(b, 'b')).toContain(real);
    expect(sqlOf(c, 'c')).toContain(fake);
    expect(sqlOf(d, 'd')).toContain(real);

    const e = await compileTenant('e', builtInFiles(false), options);
    expect(sqlOf(e, 'e')).toContain(real);
  });

  it('reflects on globalThis as on an ordinary global', async () => {
    const reflect = (tenant: string, write: boolean) => compileTenant(tenant, [{
      fileName: 'model.js',
      content: `
        ${write ? `
        globalThis.reflected = 1;
        Object.defineProperty(globalThis, 'fixed', { value: 2, configurable: false, enumerable: false });
        ` : ''}
        const names = Object.getOwnPropertyNames(globalThis);
        const info = [
          Object.keys(globalThis).includes('reflected'),
          names.includes('fixed'),
          Object.prototype.hasOwnProperty.call(globalThis, 'reflected'),
          JSON.stringify(Object.getOwnPropertyDescriptor(globalThis, 'reflected')),
          names.includes('JSON'),
          typeof Object.getOwnPropertyDescriptor(globalThis, 'JSON'),
          'fixed' in globalThis ? typeof fixed : 'none',
          Object.keys(globalThis).includes('globalThis'),
          globalThis.globalThis === globalThis,
        ].join('|');
        cube('Orders', { sql: 'select 1', description: info, measures: { count: { type: 'count' } } });
      `,
    }], options);

    const writer = await reflect('a', true);
    expect(writer.metaTransformer.cubes[0].config.description)
      .toBe('true|true|true|{"value":1,"writable":true,"enumerable":true,"configurable":true}|true|object|number|false|true');

    // Another tenant doesn't see them
    const other = await reflect('b', false);
    expect(other.metaTransformer.cubes[0].config.description).toBe('false|false|false||true|object|none|false|true');
  });

  it('lets a compile reassign, redefine and delete a built-in like on its own global', async () => {
    const { metaTransformer } = await compileTenant('a', [{
      fileName: 'model.js',
      content: `
        this.Math = { v: 1 };
        this.Math = { v: 2 };
        const afterTwo = Math.v;
        Math = { v: 3 };
        const afterBare = Math.v;
        Object.defineProperty(globalThis, 'escape', { value: { v: 4 }, writable: true, configurable: true });
        escape = { v: 5 };
        const afterDefine = escape.v;
        const deleted = delete globalThis.Math;
        const gone = !('Math' in globalThis);
        let caught;
        try {
          neverDeclaredName;
        } catch (e) {
          caught = e instanceof ReferenceError;
        }
        const info = [afterTwo, afterBare, afterDefine, deleted, gone, caught].join('|');
        cube('Orders', { sql: 'select 1', description: info, measures: { count: { type: 'count' } } });
      `,
    }], options);

    expect(metaTransformer.cubes[0].config.description).toBe('2|3|5|true|true|true');
    // The realm's own built-ins are untouched
    const other = await compileTenant('b', tenantFiles(), options);
    expect(buildSql(other, 'b')).toContain(`'legacy_b_b'`);
  });

  const functionFiles = (): Files => [{
    fileName: 'model.js',
    content: `
      const viaNew = new Function('return COMPILE_CONTEXT.securityContext.tenant')();
      const viaCall = Function('a', 'return a + "_" + COMPILE_CONTEXT.securityContext.tenant')('x');
      implicitGlobal = 'ig';
      const readsImplicit = new Function('return implicitGlobal')();
      const fnCheck = [
        new Function('') instanceof Function,
        Object.getPrototypeOf(new Function('')) === Function.prototype,
        Function.prototype === Object.getPrototypeOf(function () {}),
        Function instanceof Function,
        Function.length === 1,
        (() => { try { new Function('}; x(); {'); return 'parsed'; } catch (e) { return e instanceof SyntaxError; } })(),
      ].join(',');
      const directEval = eval('COMPILE_CONTEXT.securityContext.tenant');
      // Built outside of cube(), as generated members are: parameter names are references, and the
      // body reads COMPILE_CONTEXT lazily
      const generated = () => ({
        tenant: {
          sql: new Function('CUBE', 'return CUBE ? "\\'" + COMPILE_CONTEXT.securityContext.tenant + "_lazy\\'" : ""'),
          type: 'string',
        },
      });
      cube(\`orders_\${COMPILE_CONTEXT.securityContext.tenant}\`, {
        sql_table: 'orders',
        description: [viaNew, viaCall, readsImplicit, fnCheck, directEval].join('|'),
        measures: { count: { type: 'count' } },
        dimensions: {
          id: { sql: 'id', type: 'number', primary_key: true },
          ...generated(),
        },
      });
    `,
  }];

  it('compiles new Function bodies and direct eval in the compile scope', async () => {
    const [a, b] = await Promise.all(['a', 'b'].map(t => compileTenant(t, functionFiles(), options)));

    for (const [tenant, compiled] of [['a', a], ['b', b]] as const) {
      expect(compiled.metaTransformer.cubes[0].config.description).toBe(`${tenant}|x_${tenant}|ig|true,true,true,true,true,true|${tenant}`);
      const sql = new PostgresQuery(compiled, {
        measures: [`orders_${tenant}.count`],
        dimensions: [`orders_${tenant}.tenant`],
      }).buildSqlAndParams()[0];
      expect(sql).toContain(`'${tenant}_lazy'`);
    }
  });

  it('throws for an undeclared name assigned in strict code', async () => {
    const strict = prepareCompiler([
      { fileName: 'sloppy.js', content: `sharedImplicit = 'si';` },
      {
        fileName: 'model.js',
        content: `'use strict';
          const seen = [typeof neverDeclared, sharedImplicit].join('|');
          cube('Orders', { sql: 'select 1', description: seen, measures: { count: { type: 'count' } } });
        `,
      },
    ], options);
    await strict.compiler.compile();
    expect(strict.metaTransformer.cubes[0].config.description).toBe('undefined|si');

    const typo = prepareCompiler([{
      fileName: 'model.js',
      content: `'use strict';
        typo = 1;
        cube('Orders', { sql: 'select 1', measures: { count: { type: 'count' } } });
      `,
    }], options);
    await expect(typo.compiler.compile()).rejects.toThrow(/typo is not defined/);
  });

  it('gives UMD typeof checks their usual result', async () => {
    const { metaTransformer } = await compileTenant('a', [{
      fileName: 'model.js',
      content: `
        const umd = (typeof module === 'object' && module.exports) ? 'cjs'
          : (typeof define === 'function' ? 'amd' : 'global');
        const kinds = [typeof module, typeof exports, umd, typeof JSON, typeof COMPILE_CONTEXT].join(',');
        cube('Orders', {
          sql: 'select * from orders',
          measures: { count: { type: 'count' } },
          dimensions: { id: { sql: 'id', type: 'number', primary_key: true } },
          description: kinds,
        });
      `,
    }], options);

    expect(metaTransformer.cubes[0].config.description).toBe('undefined,undefined,global,object,object');

    const undeclared = prepareCompiler([{
      fileName: 'model.js',
      content: `
        cube('Orders', { sql: 'select 1', description: typeof neverDeclared, measures: { count: { type: 'count' } } });
      `,
    }], options);
    if (sharedVmContext) {
      // The documented difference: the scope can't tell `typeof x` from reading `x`
      await expect(undeclared.compiler.compile()).rejects.toThrow(/neverDeclared is not defined/);
    } else {
      await undeclared.compiler.compile();
      expect(undeclared.metaTransformer.cubes[0].config.description).toBe('undefined');
    }
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

  it('stores no tenant value on the shared realm global', async () => {
    const { cubeEvaluator } = await compileTenant('a', globalWriterFilesForRealm(), { sharedVmContext: true });
    const realmFunction = cubeEvaluator.cubeFromPath('orders_a').dimensions.tenant.sql.constructor;
    const sharedGlobal = realmFunction('return globalThis')();

    for (const key of ['tableSuffix', 'otherSuffix']) {
      expect(Object.getOwnPropertyDescriptor(sharedGlobal, key)).toBeUndefined();
      expect(sharedGlobal[key]).toBeUndefined();
    }
    // Whatever still reaches the real global fails loudly
    expect(() => realmFunction('newGlobal = 1')()).toThrow(/Cannot set global 'newGlobal'/);
    expect(() => realmFunction('globalThis.newGlobal = 1')()).toThrow(/Cannot set global 'newGlobal'/);

    await compileTenant('b', builtInFiles(true), { sharedVmContext: true });
    // Outside of a compile the realm keeps its built-ins
    expect(realmFunction('return [JSON.stringify({}), Math.max(1, 2), escape(" ")].join()')()).toBe('{},2,%20');
  });

  it('fails loudly on a write that would reach the shared global', async () => {
    const nested = prepareCompiler([{
      fileName: 'model.js',
      content: `
        function setGlobal() { this.nested = COMPILE_CONTEXT.securityContext.tenant; }
        setGlobal();
        cube('Orders', { sql: 'select 1', measures: { count: { type: 'count' } } });
      `,
    }], { sharedVmContext: true, compileContext: { securityContext: { tenant: 'a' } } });

    await expect(nested.compiler.compile()).rejects.toThrow(/Cannot set global 'nested'/);

    // Model code sees errors of its own realm
    const caught = await compileTenant('a', [{
      fileName: 'model.js',
      content: `
        let kind;
        try {
          (function () { this.nestedWrite = 1; })();
        } catch (e) {
          kind = e instanceof TypeError;
        }
        cube('Orders', { sql: 'select 1', description: String(kind), measures: { count: { type: 'count' } } });
      `,
    }], { sharedVmContext: true });
    expect(caught.metaTransformer.cubes[0].config.description).toBe('true');
  });

  it('runs indirect eval and Function via a prototype in the shared global scope', async () => {
    const compileWith = async (sharedVmContext: boolean) => {
      const { metaTransformer } = await compileTenant('a', [{
        fileName: 'model.js',
        content: `
          const indirect = (0, eval)('typeof COMPILE_CONTEXT');
          const viaPrototype = (function () {}).constructor('return typeof COMPILE_CONTEXT')();
          cube('Orders', { sql: 'select 1', description: [indirect, viaPrototype].join('|'), measures: { count: { type: 'count' } } });
        `,
      }], { sharedVmContext });
      return metaTransformer.cubes[0].config.description;
    };

    expect(await compileWith(false)).toBe('object|object');
    // The documented difference: these reach the realm's own eval and Function
    expect(await compileWith(true)).toBe('undefined|undefined');
  });

  it('reuses one realm across compiles when on', async () => {
    const realm = await realmOf('a', true);
    expect(await realmOf('b', true)).toBe(realm);
    expect(realm).not.toBe(Function);
  });
});
