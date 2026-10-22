import { CompilerApi } from '../../src/core/CompilerApi';

// https://github.com/cube-js/cube/issues/9035
// In dev mode the compiler version is suffixed with a hash of `dataSchemaFiles()`.
// A Python `repository_factory` crosses into JS through a Rust HashMap, so the
// key order of each `{ fileName, content }` object changes between calls even
// though the files themselves are identical. That must not trigger a recompile.
describe('issue #9035: dev mode recompiles on every request', () => {
  const content = `
    cubes:
      - name: orders
        sql: SELECT 1 AS id
        measures:
          - name: count
            type: count
  `;

  test('identical files with a different key order reuse the compiled model', async () => {
    let contentFirst = false;
    const compiledVersions: string[] = [];
    const compilerApi = new CompilerApi(
      {
        localPath: () => '/mock/path',
        // Same file every time; only the property order differs, as it does
        // for objects returned by `file_repository()` in cube.py.
        dataSchemaFiles: async () => [
          contentFirst
            ? { content, fileName: 'orders.yml' }
            : { fileName: 'orders.yml', content },
        ],
      },
      async () => 'postgres',
      {
        logger: (msg, params) => {
          if (msg === 'Compiling schema' || msg === 'Recompiling schema') {
            compiledVersions.push(params.version);
          }
        },
        devServer: true,
        schemaVersion: () => 'fixed_schema_version',
      }
    );

    try {
      await compilerApi.getCompilers();
      contentFirst = true;
      await compilerApi.getCompilers();

      expect(compiledVersions).toHaveLength(1);
    } finally {
      compilerApi.dispose();
    }
  });
});
