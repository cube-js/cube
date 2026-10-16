// https://github.com/cube-js/cube/issues/10383
// Jinja render cache ignores schemaVersion: when schemaVersion changes, the model is
// recompiled, but templates whose output depends on dynamic data (Python template
// functions) keep serving the output rendered for a previous version.
import { FileContent, isNativeSupported, SchemaFileRepository } from '@cubejs-backend/shared';
import { CompilerApi } from '../../src/core/CompilerApi';
import { DbTypeInternalFn } from '../../src/core/types';

const dbType: DbTypeInternalFn = async () => 'postgres';

const files: FileContent[] = [
  {
    fileName: 'globals.py',
    content: `from cube import TemplateContext
import uuid

template = TemplateContext()

@template.function('dynamic_table')
def dynamic_table() -> str:
    # Stands for a definition fetched from an external API: differs on every render
    return 't_' + uuid.uuid4().hex
`,
  },
  {
    fileName: 'dynamic.yml.jinja',
    content: `cubes:
  - name: dynamic_cube
    sql_table: {{ dynamic_table() }}
    measures:
      - name: count
        type: count
`,
  },
];

const repository: SchemaFileRepository = {
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve(files.map((f) => ({ ...f }))),
};

const nativeSuite = isNativeSupported() === true ? describe : xdescribe;

nativeSuite('issue #10383: Jinja cache and schemaVersion', () => {
  test('re-renders Jinja templates when schemaVersion changes', async () => {
    let version = 'v1';
    const api = new CompilerApi(repository, dbType, {
      logger: () => undefined,
      compileContext: { securityContext: {} },
      schemaVersion: () => version,
    });

    try {
      const sqlTable = async () => {
        const { cubeEvaluator } = await api.getCompilers();
        const table = cubeEvaluator.cubeFromPath('dynamic_cube').sqlTable;
        return typeof table === 'function' ? table() : table;
      };

      const first = await sqlTable();
      expect(first).toMatch(/^t_[0-9a-f]{32}$/);

      version = 'v2';
      const second = await sqlTable();
      expect(second).toMatch(/^t_[0-9a-f]{32}$/);

      // The model was recompiled for the new schemaVersion, so the template must be rendered again
      expect(second).not.toBe(first);
    } finally {
      api.dispose();
    }
  });
});
