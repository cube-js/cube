import { LRUCache } from 'lru-cache';
import vm from 'vm';
import { prepareCompiler } from '../../src/compiler/PrepareCompiler';

describe('Template cache invalidation with compilerId', () => {
  it('should use different cache entries when compilerId changes', async () => {
    // Shared caches between compilations (simulating CompilerApi behavior)
    const compiledScriptCache = new LRUCache<string, vm.Script>({ max: 250 });
    const compiledYamlCache = new LRUCache<string, string>({ max: 250 });
    const compiledJinjaCache = new LRUCache<string, string>({ max: 250 });

    const yamlContent = `
cubes:
  - name: Orders
    sql: "SELECT * FROM orders"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
`;

    const repo = {
      localPath: () => __dirname,
      dataSchemaFiles: () => Promise.resolve([
        { fileName: 'orders.yml', content: yamlContent }
      ]),
    };

    // First compilation
    const compiler1 = prepareCompiler(repo, {
      adapter: 'postgres',
      compiledScriptCache,
      compiledYamlCache,
      compiledJinjaCache,
    });
    await compiler1.compiler.compile();

    // Record cache sizes after first compilation
    const yamlCacheSizeAfterFirst = compiledYamlCache.size;
    const scriptCacheSizeAfterFirst = compiledScriptCache.size;

    // Second compilation with same shared caches (simulates schemaVersion change)
    // prepareCompiler generates a new compilerId each time
    const compiler2 = prepareCompiler(repo, {
      adapter: 'postgres',
      compiledScriptCache,
      compiledYamlCache,
      compiledJinjaCache,
    });
    await compiler2.compiler.compile();

    // Different compilerId = different cache key = new cache entries
    // Cache size should increase because same content with different compilerId
    // creates a new cache entry instead of reusing the old one
    expect(compiledYamlCache.size).toBeGreaterThan(yamlCacheSizeAfterFirst);
    expect(compiledScriptCache.size).toBeGreaterThan(scriptCacheSizeAfterFirst);

    // Verify we have two different compilerIds
    expect(compiler1.compilerId).not.toEqual(compiler2.compilerId);
  });

  it('should reuse cache entries when compilerId is the same', async () => {
    const compiledScriptCache = new LRUCache<string, vm.Script>({ max: 250 });
    const compiledYamlCache = new LRUCache<string, string>({ max: 250 });
    const compiledJinjaCache = new LRUCache<string, string>({ max: 250 });

    const yamlContent = `
cubes:
  - name: Products
    sql: "SELECT * FROM products"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
`;

    const repo = {
      localPath: () => __dirname,
      dataSchemaFiles: () => Promise.resolve([
        { fileName: 'products.yml', content: yamlContent }
      ]),
    };

    // First compilation
    const compiler1 = prepareCompiler(repo, {
      adapter: 'postgres',
      compiledScriptCache,
      compiledYamlCache,
      compiledJinjaCache,
    });
    await compiler1.compiler.compile();

    const yamlCacheSizeAfterFirst = compiledYamlCache.size;
    const scriptCacheSizeAfterFirst = compiledScriptCache.size;

    // Compile again with the SAME compiler instance (same compilerId)
    await compiler1.compiler.compile();

    // Cache size should NOT increase - entries are reused
    expect(compiledYamlCache.size).toEqual(yamlCacheSizeAfterFirst);
    expect(compiledScriptCache.size).toEqual(scriptCacheSizeAfterFirst);
  });
});
