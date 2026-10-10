import { FileContent, SchemaFileRepository } from '@cubejs-backend/shared';
import { CompilerApi } from '../../src/core/CompilerApi';
import { CubejsServerCore } from '../../src/core/server';
import { DbTypeInternalFn } from '../../src/core/types';

const dbType: DbTypeInternalFn = async () => 'postgres';

/**
 * What `includes: "*"` copies onto the view: orders' 3 measures, its 3
 * dimensions and its segment. Pinned so a change in view expansion is visible.
 */
const VIEW_MEMBERS = 7;

const repository = (files: FileContent[]): SchemaFileRepository => ({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve(files.map((f) => ({ ...f }))),
});

/**
 * `members` measures + `members` dimensions + one segment, so the expected
 * count for a cube is 2 * members + 1.
 */
const cubeOf = (name: string, members: number) => `
cube('${name}', {
  sql_table: '${name}_table',
  measures: {
${Array.from({ length: members }, (_, i) => `    m${i}: { type: 'count' },`).join('\n')}
  },
  dimensions: {
    id: { sql: 'id', type: 'number', primaryKey: true },
${Array.from({ length: members - 1 }, (_, i) => `    d${i}: { sql: 'd${i}', type: 'string' },`).join('\n')}
  },
  segments: {
    s0: { sql: \`\${CUBE}.id > 0\` },
  },
});
`;

/** A view holds its own copy of the members it includes, on its own cubeList entry. */
const ordersView = `
views:
  - name: orders_view
    cubes:
      - join_path: orders
        includes: "*"
`;

const compilerApiFor = (files: FileContent[]) => new CompilerApi(
  repository(files),
  dbType,
  { logger: jest.fn() },
);

/**
 * A core whose compiler cache and budget pass are reachable from the test,
 * rather than casting the instance to `any`.
 */
class CoreOpen extends CubejsServerCore {
  public get cache() {
    return this.compilerCache;
  }

  public evict(keepAppId: string) {
    return this.evictCompilersOverMemberBudget(keepAppId);
  }
}

const coreWithBudget = (maxCompiledMembers?: number) => new CoreOpen(<any>{
  apiSecret: 'secret',
  driverFactory: () => ({ type: 'postgres' }),
  maxCompiledMembers,
  logger: jest.fn(),
});

/**
 * A cached model of a known size, without compiling one. `dispose` is real
 * because the cache calls it on eviction, which is how a dropped model frees
 * its compiled scripts rather than merely losing its reference.
 */
const cachedModel = (memberCount: number) => ({
  compiledMemberCount: memberCount,
  dispose: jest.fn(),
} as unknown as CompilerApi);

describe('compiled member counting', () => {
  test('counts measures, dimensions and segments across every cube', async () => {
    const api = compilerApiFor([
      { fileName: 'orders.js', content: cubeOf('orders', 3) },
      { fileName: 'users.js', content: cubeOf('users', 2) },
    ]);

    await api.getCompilers();

    // orders: 3 measures + 3 dimensions + 1 segment, users: 2 + 2 + 1
    expect(api.compiledMemberCount).toBe(12);
  });

  test("counts a view's members, which is where a wide model's usually are", async () => {
    const cubes = [{ fileName: 'orders.js', content: cubeOf('orders', 3) }];

    const withoutView = compilerApiFor(cubes);
    await withoutView.getCompilers();

    const withView = compilerApiFor([...cubes, { fileName: 'orders_view.yml', content: ordersView }]);
    await withView.getCompilers();

    // Views are counted only because applyIncludeMembers writes the included
    // members onto the view's own definition. If that ever stops, a view-heavy
    // model would read as near-empty and the budget would stop bounding it.
    expect(withView.compiledMemberCount - withoutView.compiledMemberCount).toBe(VIEW_MEMBERS);
  });

  test('reports zero while a recompile is in flight', async () => {
    const api = compilerApiFor([{ fileName: 'orders.js', content: cubeOf('orders', 3) }]);
    await api.getCompilers();
    expect(api.compiledMemberCount).toBe(7);

    const recompiling = api.compileSchema('v2');

    // Still carrying the old count here would let the budget walk evict the
    // entry and throw away the compile that is running.
    expect(api.compiledMemberCount).toBe(0);

    await recompiling;
    expect(api.compiledMemberCount).toBe(7);
  });

  test('recompiles after eviction instead of handing back a disposed proxy', async () => {
    const api = compilerApiFor([{ fileName: 'orders.js', content: cubeOf('orders', 3) }]);
    await api.getCompilers();

    // What the cache does to an evicted entry. A request that already holds
    // this instance comes back to getCompilers(), and before dispose() cleared
    // the version it was handed the proxy, which throws on `.then`.
    api.dispose();

    await expect(api.getCompilers()).resolves.toBeDefined();
    expect(api.compiledMemberCount).toBe(7);
  });

  test('is zero before anything has compiled', () => {
    expect(compilerApiFor([]).compiledMemberCount).toBe(0);
  });
});

describe('evictCompilersOverMemberBudget', () => {
  test('does nothing when no budget is configured', () => {
    const core = coreWithBudget(undefined);
    core.cache.set('a', cachedModel(10_000));
    core.cache.set('b', cachedModel(10_000));

    core.evict('b');

    expect([...core.cache.keys()].sort()).toEqual(['a', 'b']);
  });

  test('keeps everything while the total is inside the budget', () => {
    const core = coreWithBudget(100);
    core.cache.set('a', cachedModel(40));
    core.cache.set('b', cachedModel(40));

    core.evict('b');

    expect([...core.cache.keys()].sort()).toEqual(['a', 'b']);
  });

  test('drops least-recently-used models until the total fits', () => {
    const core = coreWithBudget(100);
    core.cache.set('a', cachedModel(50));
    core.cache.set('b', cachedModel(50));
    core.cache.set('c', cachedModel(50));

    core.evict('c');

    // 150 over a budget of 100: `a` is the least recently used and goes first,
    // which brings the total to 100 and stops the walk.
    expect([...core.cache.keys()].sort()).toEqual(['b', 'c']);
  });

  test('evicts in recency order, not insertion order', () => {
    const core = coreWithBudget(100);
    core.cache.set('a', cachedModel(50));
    core.cache.set('b', cachedModel(50));
    core.cache.set('c', cachedModel(50));
    // `a` is used again, so `b` becomes the least recently used.
    core.cache.get('a');

    core.evict('c');

    expect([...core.cache.keys()].sort()).toEqual(['a', 'c']);
  });

  test('disposes an evicted model, so its compiled scripts are freed', () => {
    const core = coreWithBudget(100);
    const stale = cachedModel(50);
    core.cache.set('a', stale);
    core.cache.set('b', cachedModel(50));
    core.cache.set('c', cachedModel(50));

    core.evict('c');

    expect(stale.dispose).toHaveBeenCalledTimes(1);
  });

  test('never evicts the model that just compiled, even alone over budget', () => {
    const core = coreWithBudget(100);
    core.cache.set('big', cachedModel(5_000));

    core.evict('big');

    // Dropping it would make every request for this tenant a recompile.
    expect([...core.cache.keys()]).toEqual(['big']);
  });

  test('leaves a model that is still compiling alone', () => {
    const core = coreWithBudget(100);
    const compiling = cachedModel(0);
    core.cache.set('compiling', compiling);
    core.cache.set('big', cachedModel(5_000));

    core.evict('big');

    // Dropping it would free nothing and restart a compile already in flight.
    expect([...core.cache.keys()].sort()).toEqual(['big', 'compiling']);
    expect(compiling.dispose).not.toHaveBeenCalled();
  });

  test('clears the rest around an oversized model rather than giving up', () => {
    const core = coreWithBudget(100);
    core.cache.set('a', cachedModel(50));
    core.cache.set('b', cachedModel(50));
    core.cache.set('big', cachedModel(5_000));

    core.evict('big');

    expect([...core.cache.keys()]).toEqual(['big']);
  });
});
