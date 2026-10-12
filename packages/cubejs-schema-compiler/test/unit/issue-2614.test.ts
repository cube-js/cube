import { ScaffoldingTemplate, SchemaFormat } from '../../src/scaffolding/ScaffoldingTemplate';
import { prepareCompiler } from './PrepareCompiler';

// https://github.com/cube-js/cube/issues/2614
// Table names that start with a digit or contain hyphens (e.g. UUIDs in BigQuery)
// produce cube names that fail the identifier pattern on compile.
const driver = {
  quoteIdentifier: (name: string) => `"${name}"`,
};

const IDENTIFIER = /^[_a-zA-Z][_a-zA-Z0-9]*$/;

describe('issue #2614: scaffolded identifiers must be valid', () => {
  const tableName = '0e4fb908-ad9a-4e3a-a21e-316ed8e72995';
  const dbSchema = {
    public: {
      [tableName]: [
        { name: 'id', type: 'integer', attributes: [] },
        { name: 'amount', type: 'integer', attributes: [] },
      ],
    },
  };

  [SchemaFormat.Yaml, SchemaFormat.JavaScript].forEach((format) => {
    it(`generates a compilable model (${format})`, async () => {
      const template = new ScaffoldingTemplate(dbSchema, driver, {
        format,
        snakeCase: true,
      });

      const [file] = template.generateFilesByTableNames([`public.${tableName}`]);
      const cubeName = file.fileName.replace(/\.(yml|js)$/, '');
      expect(cubeName).toMatch(IDENTIFIER);

      const { compiler } = prepareCompiler(file);
      await compiler.compile();
    });
  });
});
