import { FileContent, isNativeSupported, SchemaFileRepository } from '@cubejs-backend/shared';
import { CompilerApi, CompilerApiOptions } from '../../src/core/CompilerApi';
import { resetSharedCompilerCaches } from '../../src/core/SharedCompilerCaches';
import { DbTypeInternalFn } from '../../src/core/types';

class CompilerApiTestable extends CompilerApi {
  public get scriptCache() {
    return this.compiledScriptCache;
  }

  public get yamlCache() {
    return this.compiledYamlCache;
  }

  public get jinjaCache() {
    return this.compiledJinjaCache;
  }
}

const dbType: DbTypeInternalFn = async () => 'postgres';

const repository = (files: FileContent[]): SchemaFileRepository => ({
  localPath: () => __dirname,
  // A copy per call, as a repository reading from disk returns
  dataSchemaFiles: () => Promise.resolve(files.map((f) => ({ ...f }))),
});

const jsCube = (name: string, table: string) => `
cube('${name}', {
  sql_table: '${table}',
  measures: { count: { type: 'count' } },
  dimensions: { id: { sql: 'id', type: 'number', primaryKey: true } },
});
`;

const yamlCube = (name: string, table: string) => `
cubes:
  - name: ${name}
    sql_table: ${table}
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
`;

const createApi = (files: FileContent[], options: Partial<CompilerApiOptions> = {}) => new CompilerApiTestable(
  repository(files),
  dbType,
  { logger: () => undefined, compileContext: { securityContext: {} }, ...options },
);

const sqlTable = async (api: CompilerApi, cubeName: string) => {
  const { cubeEvaluator } = await api.getCompilers();
  const { sqlTable: table } = cubeEvaluator.cubeFromPath(cubeName);

  return typeof table === 'function' ? table() : table;
};

const apis: CompilerApi[] = [];
const track = <T extends CompilerApi>(api: T): T => {
  apis.push(api);
  return api;
};

beforeEach(() => {
  process.env.CUBEJS_COMPILER_MULTI_TENANT_SHARING = 'true';
});

afterEach(() => {
  delete process.env.CUBEJS_COMPILER_MULTI_TENANT_SHARING;
  apis.splice(0).forEach((api) => api.dispose());
  resetSharedCompilerCaches();
});

describe('Shared compiler caches (CUBEJS_COMPILER_MULTI_TENANT_SHARING)', () => {
  const files: FileContent[] = [
    { fileName: 'orders.js', content: jsCube('orders', 'public.orders') },
    { fileName: 'users.yml', content: yamlCube('users', 'public.users') },
  ];

  test('two apps share one vm.Script for identical files', async () => {
    const a = track(createApi(files));
    const b = track(createApi(files));

    expect(a.scriptCache).toBe(b.scriptCache);
    expect(a.yamlCache).toBe(b.yamlCache);
    // Jinja output depends on the app's COMPILE_CONTEXT: never shared
    expect(a.jinjaCache).not.toBe(b.jinjaCache);

    await a.getCompilers();
    const keys = [...a.scriptCache.keys()];
    // Compiled for the shared realm (the `with` wrapper), keyed apart from plain scripts
    expect(keys.length).toBeGreaterThan(0);
    expect(keys.every((k) => k.startsWith('shared:'))).toBe(true);
    const scripts = keys.map((k) => a.scriptCache.get(k));
    const yamlEntries = a.yamlCache.size;

    await b.getCompilers();
    // b hit every entry a created instead of adding its own
    expect([...b.scriptCache.keys()].sort()).toEqual([...keys].sort());
    expect(keys.map((k) => b.scriptCache.get(k))).toEqual(scripts);
    keys.forEach((k, i) => expect(b.scriptCache.get(k)).toBe(scripts[i]));
    expect(b.yamlCache.size).toBe(yamlEntries);

    expect(await sqlTable(a, 'orders')).toBe('public.orders');
    expect(await sqlTable(b, 'users')).toBe('public.users');
  });

  test('different content does not collide', async () => {
    const a = track(createApi(files));
    const b = track(createApi([
      { fileName: 'orders.js', content: jsCube('orders', 'tenant_b.orders') },
      { fileName: 'users.yml', content: yamlCube('users', 'tenant_b.users') },
    ]));

    await a.getCompilers();
    const scriptEntries = a.scriptCache.size;
    const yamlEntries = a.yamlCache.size;
    await b.getCompilers();

    expect(b.scriptCache.size).toBeGreaterThan(scriptEntries);
    expect(b.yamlCache.size).toBe(yamlEntries * 2);
    expect(await sqlTable(a, 'orders')).toBe('public.orders');
    expect(await sqlTable(a, 'users')).toBe('public.users');
    expect(await sqlTable(b, 'orders')).toBe('tenant_b.orders');
    expect(await sqlTable(b, 'users')).toBe('tenant_b.users');
  });

  test('identical content under another file name gets its own script', async () => {
    const a = track(createApi([{ fileName: 'orders.js', content: jsCube('orders', 'public.orders') }]));
    const b = track(createApi([{ fileName: 'renamed.js', content: jsCube('orders', 'public.orders') }]));

    await a.getCompilers();
    const entries = a.scriptCache.size;
    await b.getCompilers();
    expect(b.scriptCache.size).toBe(entries * 2);
  });

  test('without the flag every app has its own caches', async () => {
    delete process.env.CUBEJS_COMPILER_MULTI_TENANT_SHARING;
    const a = track(createApi(files));
    const b = track(createApi(files));

    expect(a.scriptCache).not.toBe(b.scriptCache);
    expect(a.yamlCache).not.toBe(b.yamlCache);
    await a.getCompilers();
    await b.getCompilers();
    expect(a.scriptCache.size).toBe(b.scriptCache.size);
  });

  test('the sharedCompilerCaches option overrides the flag', () => {
    const a = track(createApi(files, { sharedCompilerCaches: false }));
    const b = track(createApi(files));
    const c = track(createApi(files));
    expect(a.scriptCache).not.toBe(b.scriptCache);
    expect(b.scriptCache).toBe(c.scriptCache);
  });

  test('dispose leaves the shared caches to the other apps', async () => {
    const a = track(createApi(files));
    const b = track(createApi(files));
    await a.getCompilers();
    const entries = a.scriptCache.size;
    a.dispose();

    expect(b.scriptCache.size).toBe(entries);
    await b.getCompilers();
    expect(await sqlTable(b, 'orders')).toBe('public.orders');
  });

  test('a YAML file that fails to transpile is not cached', async () => {
    const broken: FileContent[] = [{ fileName: 'broken.yml', content: 'cubes:\n  - name: broken\n    sql: "{"\n' }];
    const a = track(createApi(broken));
    const b = track(createApi(broken));

    await expect(a.getCompilers()).rejects.toThrow();
    expect(a.yamlCache.size).toBe(0);
    // b reports the error too, rather than compiling a cached half-transpiled file
    await expect(b.getCompilers()).rejects.toThrow();
  });

  const nativeSuite = isNativeSupported() === true ? describe : xdescribe;

  nativeSuite('Jinja', () => {
    const template: FileContent[] = [{
      fileName: 'tenant.yml',
      content: `cubes:
  - name: tenant_cube
    sql_table: {{ COMPILE_CONTEXT.securityContext.tenant }}
    measures:
      - name: count
        type: count
`,
    }];

    test('rendered output is not shared between apps with different security contexts', async () => {
      const a = track(createApi(template, { compileContext: { securityContext: { tenant: 'tenant_a' } } }));
      const b = track(createApi(template, { compileContext: { securityContext: { tenant: 'tenant_b' } } }));

      expect(await sqlTable(a, 'tenant_cube')).toBe('tenant_a');
      expect(await sqlTable(b, 'tenant_cube')).toBe('tenant_b');
      // Rendered YAML is keyed by its content: two different renders, two entries
      expect(a.yamlCache.size).toBe(2);
    });
  });
});
