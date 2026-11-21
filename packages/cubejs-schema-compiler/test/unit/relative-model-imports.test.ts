import path from 'path';
import { prepareCompiler } from './PrepareCompiler';

type Files = { fileName: string, content: string }[];

const helper = (fileName: string, cubeName: string) => ({
  fileName,
  content: `export const cubeName = '${cubeName}';`,
});

const ordersImporting = (fileName: string, importPath: string) => ({
  fileName,
  content: `
    import { cubeName } from '${importPath}';

    cube(cubeName, {
      sql_table: 'orders',

      measures: {
        count: { type: 'count' },
      },

      dimensions: {
        id: { sql: 'id', type: 'number', primary_key: true },
      },
    });
  `,
});

const compile = async (files: Files) => {
  const compilers = prepareCompiler(files);
  await compilers.compile();
  return compilers.cubeEvaluator.cubeNames();
};

describe('Relative imports in model files', () => {
  const previousFlag = process.env.CUBEJS_RELATIVE_MODEL_IMPORTS;

  afterEach(() => {
    if (previousFlag === undefined) {
      delete process.env.CUBEJS_RELATIVE_MODEL_IMPORTS;
    } else {
      process.env.CUBEJS_RELATIVE_MODEL_IMPORTS = previousFlag;
    }
  });

  describe('CUBEJS_RELATIVE_MODEL_IMPORTS is not set', () => {
    beforeEach(() => {
      delete process.env.CUBEJS_RELATIVE_MODEL_IMPORTS;
    });

    it('resolves ./ against the model root', async () => {
      const cubeNames = await compile([
        helper('helpers.js', 'root_orders'),
        helper(path.join('sub', 'helpers.js'), 'sub_orders'),
        ordersImporting(path.join('sub', 'orders.js'), './helpers'),
      ]);

      expect(cubeNames).toEqual(['root_orders']);
    });

    it('does not find a sibling file of a file in a subdirectory', async () => {
      const compilers = prepareCompiler([
        helper(path.join('sub', 'helpers.js'), 'sub_orders'),
        ordersImporting(path.join('sub', 'orders.js'), './helpers'),
      ]);

      await expect(compilers.compile()).rejects.toThrow(/Required import for helpers\.js is not found/);
    });
  });

  describe('CUBEJS_RELATIVE_MODEL_IMPORTS=true', () => {
    beforeEach(() => {
      process.env.CUBEJS_RELATIVE_MODEL_IMPORTS = 'true';
    });

    it('resolves ./ against the directory of the importing file', async () => {
      const cubeNames = await compile([
        helper('helpers.js', 'root_orders'),
        helper(path.join('sub', 'helpers.js'), 'sub_orders'),
        ordersImporting(path.join('sub', 'orders.js'), './helpers'),
      ]);

      expect(cubeNames).toEqual(['sub_orders']);
    });

    it('resolves ../ against the directory of the importing file', async () => {
      const cubeNames = await compile([
        helper(path.join('shared', 'helpers.js'), 'shared_orders'),
        ordersImporting(path.join('sub', 'orders.js'), '../shared/helpers.js'),
      ]);

      expect(cubeNames).toEqual(['shared_orders']);
    });

    it('resolves ./ from a file in the model root as before', async () => {
      const cubeNames = await compile([
        helper(path.join('sub', 'helpers.js'), 'sub_orders'),
        ordersImporting('orders.js', './sub/helpers'),
      ]);

      expect(cubeNames).toEqual(['sub_orders']);
    });

    it('resolves ./ against the model root when the importing file has no such sibling', async () => {
      const cubeNames = await compile([
        helper(path.join('shared_utils', 'utils.js'), 'shared_orders'),
        ordersImporting(path.join('sales', 'orders.js'), './shared_utils/utils'),
      ]);

      expect(cubeNames).toEqual(['shared_orders']);
    });

    it('does not resolve an import outside of the model root to a model file', async () => {
      const compilers = prepareCompiler([
        helper('helpers.js', 'root_orders'),
        ordersImporting(path.join('sub', 'orders.js'), '../../helpers'),
      ]);

      await expect(compilers.compile()).rejects.toThrow(/'\.\.\/\.\.\/helpers' restricted/);
    });

    it('falls back to Node.js require for a file that is not a model file', async () => {
      // As in the "Node.js globals" docs example: a file outside of the model root, resolved
      // against the model root the way it is without the flag.
      const compilers = prepareCompiler([{
        fileName: path.join('sub', 'orders.js'),
        content: `
          import { foo } from '../../test/unit/TestHelperForImport.js';

          cube(foo(), {
            sql_table: 'orders',

            measures: {
              count: { type: 'count' },
            },
          });
        `,
      }], { allowNodeRequire: true });
      await compilers.compile();

      expect(compilers.cubeEvaluator.cubeNames()).toEqual(['bar']);
    });
  });
});
