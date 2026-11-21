import { FileContent, SchemaFileRepository } from '@cubejs-backend/shared';
import { CubejsServerCore } from '../../src/core/server';
import { CreateOptions, RequestContext } from '../../src/core/types';

const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
};

const files: FileContent[] = [{
  fileName: 'orders.js',
  content: `cube('Orders', {
    sql: 'SELECT 1 AS id',
    measures: { count: { type: 'count' } },
    dimensions: { id: { sql: 'id', type: 'number', primaryKey: true } }
  });`,
}];

const context = (tenant: string): RequestContext => ({
  securityContext: { tenant },
  requestId: tenant,
});

const cores: CubejsServerCore[] = [];
const gates: ReturnType<typeof deferred>[] = [];
const pending: Promise<unknown>[] = [];
const originalConcurrency = process.env.CUBEJS_COMPILER_CONCURRENCY;

beforeEach(() => {
  delete process.env.CUBEJS_COMPILER_CONCURRENCY;
});

const track = <T>(promise: Promise<T>): Promise<T> => {
  pending.push(promise);
  // Attach immediately so failure tests cannot leak an unhandled rejection.
  promise.catch((): void => undefined);
  return promise;
};

const repository = (name: string, admissions: string[]) => {
  const started = deferred();
  const release = deferred();
  gates.push(release);
  const read = jest.fn(async () => {
    admissions.push(name);
    started.resolve();
    await release.promise;
    return files;
  });
  const schema: SchemaFileRepository = { localPath: () => __dirname, dataSchemaFiles: read };
  return { schema, read, started: started.promise, release: release.resolve };
};

const createCore = (repositories: Record<string, SchemaFileRepository>, options: CreateOptions = {}) => {
  const core = new CubejsServerCore({
    apiSecret: 'secret',
    driverFactory: () => ({ type: 'postgres' }),
    devServer: false,
    telemetry: false,
    scheduledRefreshTimer: false,
    logger: jest.fn(),
    contextToAppId: (ctx) => ctx.securityContext.tenant,
    repositoryFactory: (ctx) => repositories[ctx.securityContext.tenant],
    ...options,
  });
  cores.push(core);
  return core;
};

const nextTurn = () => new Promise<void>((resolve) => { setImmediate(resolve); });

afterEach(async () => {
  if (originalConcurrency === undefined) {
    delete process.env.CUBEJS_COMPILER_CONCURRENCY;
  } else {
    process.env.CUBEJS_COMPILER_CONCURRENCY = originalConcurrency;
  }
  gates.splice(0).forEach((gate) => gate.resolve());
  await Promise.allSettled(pending.splice(0));
  await Promise.all(cores.splice(0).map((core) => core.shutdown()));
  jest.restoreAllMocks();
});

describe('compiler concurrency', () => {
  test('admits cold tenants in FIFO order up to the configured limit', async () => {
    const admissions: string[] = [];
    const repos = ['a', 'b', 'c', 'd'].map((name) => repository(name, admissions));
    const core = createCore(Object.fromEntries(repos.map((repo, i) => ['abcd'[i], repo.schema])), { compilerConcurrency: 2 });
    const apis = await Promise.all(['a', 'b', 'c', 'd'].map((tenant) => core.getCompilerApi(context(tenant))));
    const work = apis.map((api) => track(api.getCompilers()));
    await Promise.all([repos[0].started, repos[1].started]);
    await nextTurn();
    expect(admissions).toEqual(['a', 'b']);

    repos[1].release();
    await repos[2].started;
    expect(admissions).toEqual(['a', 'b', 'c']);
    repos[2].release();
    await repos[3].started;
    expect(admissions).toEqual(['a', 'b', 'c', 'd']);
    repos.forEach((repo) => repo.release());
    await Promise.all(work);
  });

  test('leaves concurrency unlimited when unset', async () => {
    const admissions: string[] = [];
    const a = repository('a', admissions);
    const b = repository('b', admissions);
    const core = createCore({ a: a.schema, b: b.schema });
    const apis = await Promise.all(['a', 'b'].map((tenant) => core.getCompilerApi(context(tenant))));
    apis.forEach((api) => track(api.getCompilers()));
    await Promise.all([a.started, b.started]);
    expect(admissions).toEqual(['a', 'b']);
  });

  test('separate server cores have independent limits', async () => {
    const admissions: string[] = [];
    const a = repository('a', admissions);
    const b = repository('b', admissions);
    const first = createCore({ a: a.schema }, { compilerConcurrency: 1 });
    const second = createCore({ b: b.schema }, { compilerConcurrency: 1 });
    track((await first.getCompilerApi(context('a'))).getCompilers());
    track((await second.getCompilerApi(context('b'))).getCompilers());
    await Promise.all([a.started, b.started]);
    expect(admissions).toEqual(['a', 'b']);
  });

  test('shares queued and running revisions while warm cache hits bypass the queue', async () => {
    const admissions: string[] = [];
    const a = repository('a', admissions);
    const b = repository('b', admissions);
    const c = repository('c', admissions);
    const core = createCore({ a: a.schema, b: b.schema, c: c.schema }, { compilerConcurrency: 1 });
    const warm = await core.getCompilerApi(context('a'));
    a.release();
    const compiled = await warm.getCompilers();
    const active = await core.getCompilerApi(context('b'));
    const running = track(active.getCompilers());
    const runningFollower = track(active.getCompilers());
    await b.started;
    const queued = await core.getCompilerApi(context('c'));
    const first = track(queued.getCompilers());
    const follower = track(queued.getCompilers());
    await nextTurn();
    expect(c.read).not.toHaveBeenCalled();
    expect(await warm.getCompilers()).toBe(compiled);
    expect(a.read).toHaveBeenCalledTimes(1);
    b.release();
    expect(await runningFollower).toBe(await running);
    await c.started;
    c.release();
    expect(await follower).toBe(await first);
    expect(b.read).toHaveBeenCalledTimes(1);
    expect(c.read).toHaveBeenCalledTimes(1);
  });

  test('a failed compilation releases its slot and can be retried', async () => {
    const admissions: string[] = [];
    const a = repository('a', admissions);
    const b = repository('b', admissions);
    const read = a.schema.dataSchemaFiles;
    a.schema.dataSchemaFiles = jest.fn(async () => {
      await read();
      throw new Error('repository unavailable');
    });
    const core = createCore({ a: a.schema, b: b.schema }, { compilerConcurrency: 1 });
    const api = await core.getCompilerApi(context('a'));
    const failure = track(api.getCompilers());
    await a.started;
    const next = track((await core.getCompilerApi(context('b'))).getCompilers());
    a.release();
    await expect(failure).rejects.toThrow('repository unavailable');
    await b.started;
    b.release();
    await next;
    a.schema.dataSchemaFiles = read;
    await api.getCompilers();
    expect(a.read).toHaveBeenCalledTimes(2);
  });

  test.each([false, true])('holds a slot until query-factory creation settles (failure: %s)', async (fail) => {
    const admissions: string[] = [];
    const a = repository('a', admissions);
    const b = repository('b', admissions);
    const core = createCore({ a: a.schema, b: b.schema }, { compilerConcurrency: 1 });
    const api = await core.getCompilerApi(context('a'));
    const factoryStarted = deferred();
    const factoryRelease = deferred();
    gates.push(factoryRelease);
    const original = api.createQueryFactory.bind(api);
    jest.spyOn(api, 'createQueryFactory').mockImplementationOnce(async (compilers) => {
      factoryStarted.resolve();
      await factoryRelease.promise;
      if (fail) {
        throw new Error('query factory unavailable');
      }
      return original(compilers);
    });
    a.release();
    const work = track(api.getCompilers());
    await factoryStarted.promise;
    const next = track((await core.getCompilerApi(context('b'))).getCompilers());
    await nextTurn();
    expect(b.read).not.toHaveBeenCalled();
    factoryRelease.resolve();
    if (fail) {
      await expect(work).rejects.toThrow('query factory unavailable');
    } else {
      await work;
    }
    await b.started;
    b.release();
    await next;
    if (fail) {
      await api.getCompilers();
    }
  });

  test.each(['eviction', 'reset'])('retains the same queue across cache %s', async (operation) => {
    const admissions: string[] = [];
    const a = repository('a', admissions);
    const b = repository('b', admissions);
    const c = repository('c', admissions);
    const core = createCore({ a: a.schema, b: b.schema, c: c.schema }, {
      compilerConcurrency: 1,
      compilerCacheSize: 1,
    });
    const first = track((await core.getCompilerApi(context('a'))).getCompilers());
    await a.started;
    const second = track((await core.getCompilerApi(context('b'))).getCompilers());
    if (operation === 'reset') {
      await core.resetInstanceState();
    }
    const third = track((await core.getCompilerApi(context('c'))).getCompilers());
    await nextTurn();
    expect(admissions).toEqual(['a']);
    a.release();
    await first;
    await b.started;
    expect(admissions).toEqual(['a', 'b']);
    b.release();
    await second;
    await c.started;
    c.release();
    await third;
  });

  test('clears member counts before a recompile queues to prevent budget eviction', async () => {
    const admissions: string[] = [];
    const a = repository('a', admissions);
    const b = repository('b', admissions);
    let version = 'v1';
    const core = createCore({ a: a.schema, b: b.schema }, {
      compilerConcurrency: 1,
      maxCompiledMembers: 2,
      schemaVersion: () => version,
    });
    const api = await core.getCompilerApi(context('a'));
    a.release();
    await api.getCompilers();
    expect(api.compiledMemberCount).toBe(2);
    const blocker = track((await core.getCompilerApi(context('b'))).getCompilers());
    await b.started;
    version = 'v2';
    const recompile = track(api.getCompilers());
    await nextTurn();
    expect(api.compiledMemberCount).toBe(0);
    b.release();
    await blocker;
    await recompile;
    expect(await core.getCompilerApi(context('a'))).toBe(api);
  });

  test('a caller stopping its wait does not release shared running work', async () => {
    const admissions: string[] = [];
    const a = repository('a', admissions);
    const b = repository('b', admissions);
    const core = createCore({ a: a.schema, b: b.schema }, { compilerConcurrency: 1 });
    const api = await core.getCompilerApi(context('a'));
    const shared = track(api.getCompilers());
    await a.started;
    const disconnect = deferred();
    const caller = Promise.race([shared, disconnect.promise.then(() => 'disconnected')]);
    disconnect.resolve();
    expect(await caller).toBe('disconnected');
    const follower = track(api.getCompilers());
    const next = track((await core.getCompilerApi(context('b'))).getCompilers());
    await nextTurn();
    expect(b.read).not.toHaveBeenCalled();
    a.release();
    expect(await follower).toBe(await shared);
    await b.started;
    b.release();
    await next;
    expect(a.read).toHaveBeenCalledTimes(1);
  });

  test('preserves cold/recompile log labels and excludes queue waiting from duration', async () => {
    const admissions: string[] = [];
    const a = repository('a', admissions);
    const b = repository('b', admissions);
    const logger = jest.fn();
    let version = 'v1';
    let now = 0;
    jest.spyOn(Date.prototype, 'getTime').mockImplementation(() => now);
    const core = createCore({ a: a.schema, b: b.schema }, {
      compilerConcurrency: 1,
      schemaVersion: () => version,
      logger,
    });
    const first = track((await core.getCompilerApi(context('a'))).getCompilers());
    await a.started;
    const api = await core.getCompilerApi(context('b'));
    const second = track(api.getCompilers());
    await nextTurn();
    now = 10000;
    a.release();
    await first;
    await b.started;
    now = 10025;
    b.release();
    await second;
    expect(logger).toHaveBeenCalledWith('Compiling schema completed', expect.objectContaining({ requestId: undefined, duration: 25 }));
    expect(logger.mock.calls.filter(([message]) => message === 'Compiling schema')).toHaveLength(2);
    version = 'v2';
    await api.getCompilers();
    expect(logger.mock.calls.filter(([message]) => message === 'Recompiling schema')).toHaveLength(1);
  });
});
