import { FileContent, isNativeSupported, SchemaFileRepository } from '@cubejs-backend/shared';
// Not exported from the package index: deep import to observe template rendering
import { YamlCompiler } from '@cubejs-backend/schema-compiler/dist/src/compiler/YamlCompiler';

import { CompilerApi } from '../../src/core/CompilerApi';
import { DbTypeInternalFn } from '../../src/core/types';

// https://github.com/cube-js/cube/issues/10383
// `schema_version` is documented as the way to recompile a data model that depends on
// dynamic definitions (e.g. Jinja calling Python functions that fetch from an API).
// CompilerApi recompiles when the version changes, but the per-app `compiledJinjaCache`
// is keyed only by file content + macros fingerprint, so the template is never re-rendered.
const nativeSuite = isNativeSupported() === true ? describe : xdescribe;

nativeSuite('issue #10383: Jinja template cache ignores schemaVersion', () => {
  const dbType: DbTypeInternalFn = async () => 'postgres';

  const template: FileContent = {
    fileName: 'dynamic.yml.jinja',
    content: `cubes:
  - name: dynamic_cube
    sql_table: {{ load_table_name() }}
    measures:
      - name: count
        type: count
`,
  };

  const repository: SchemaFileRepository = {
    localPath: () => __dirname,
    dataSchemaFiles: () => Promise.resolve([{ ...template }]),
  };

  afterEach(() => {
    jest.restoreAllMocks();
  });

  test('a new schemaVersion re-renders Jinja templates', async () => {
    // Stand-in for the Python function `load_table_name()` (the native module in this
    // environment is built without the `python` feature): every render sees fresh data.
    let renders = 0;
    const renderSpy = jest.spyOn(YamlCompiler.prototype, 'renderTemplate')
      .mockImplementation(async (file) => {
        renders++;
        return {
          fileName: file.fileName,
          content: file.content.replace('{{ load_table_name() }}', `table_v${renders}`),
        };
      });

    let version = 'v1';
    const api = new CompilerApi(repository, dbType, {
      logger: () => undefined,
      compileContext: { securityContext: {} },
      schemaVersion: () => version,
    });

    const sqlTable = async () => {
      const { cubeEvaluator } = await api.getCompilers();
      const table = cubeEvaluator.cubeFromPath('dynamic_cube').sqlTable;
      return typeof table === 'function' ? table() : table;
    };

    try {
      expect(await sqlTable()).toBe('table_v1');

      version = 'v2';
      expect(await sqlTable()).toBe('table_v2');
      expect(renderSpy).toHaveBeenCalledTimes(2);
    } finally {
      api.dispose();
    }
  });
});
