// https://github.com/cube-js/cube/issues/2614
// Data model generation (Playground, `cubejs generate`) derives cube names from table
// names as is, so a table whose name contains a hyphen or starts with a digit produces
// a cube name that fails to match the identifier pattern and the model does not compile.

import { ScaffoldingTemplate, SchemaFormat } from '../../src/scaffolding/ScaffoldingTemplate';
import { prepareCompiler } from './PrepareCompiler';

const driver = {
  quoteIdentifier: (name: string) => `"${name}"`,
};

const columns = [
  { name: 'id', type: 'integer', attributes: [] },
  { name: 'amount', type: 'integer', attributes: [] },
  { name: 'status', type: 'character varying', attributes: [] },
];

const dbSchema = {
  public: {
    'order-items': columns,
    '0e4fb908-ad9a-4e3a-a21e-316ed8e72995': columns,
  },
};

const tableNames = [
  'public.order-items',
  'public.0e4fb908-ad9a-4e3a-a21e-316ed8e72995',
];

describe('Issue #2614: generated cube names must be valid identifiers', () => {
  const cases: [string, { format?: SchemaFormat, snakeCase?: boolean }][] = [
    ['JavaScript, camelCase', { format: SchemaFormat.JavaScript, snakeCase: false }],
    ['JavaScript, snake_case', { format: SchemaFormat.JavaScript, snakeCase: true }],
    ['YAML, snake_case', { format: SchemaFormat.Yaml, snakeCase: true }],
  ];

  test.each(cases)('%s: generated model compiles', async (_, options) => {
    const template = new ScaffoldingTemplate(dbSchema, driver, options);
    const files = template.generateFilesByTableNames(tableNames);

    const { compiler, cubeEvaluator } = prepareCompiler(files);
    await compiler.compile();

    const cubeNames = cubeEvaluator.cubeNames();
    expect(cubeNames).toHaveLength(2);
    cubeNames.forEach(name => expect(name).toMatch(/^[A-Za-z_][A-Za-z0-9_]*$/));
  });
});
