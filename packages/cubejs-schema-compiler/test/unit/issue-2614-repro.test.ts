import { ScaffoldingTemplate, SchemaFormat } from '../../src/scaffolding/ScaffoldingTemplate';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareCompiler } from './PrepareCompiler';

// Repro for https://github.com/cube-js/cube/issues/2614
// Data model generation (Playground /playground/generate-schema, CLI) turns table and column
// names into cube/member names without making sure they are valid identifiers
// (must start with a letter or underscore). Tables named by a UUID or starting with a digit,
// and columns starting with a digit, produce a data model that does not compile.
// Additionally, SQL identifiers that start with a digit are emitted unquoted
// (`sql_table: public.2020_sales`, `sql: 1st_value`), which is invalid SQL
// (Postgres: "trailing junk after numeric literal").
describe('Issue 2614: generated identifiers must be valid', () => {
  const driver = {
    quoteIdentifier: (name: string) => `"${name}"`,
  };

  const dbSchema = {
    public: {
      '0e4fb908-ad9a-4e3a-a21e-316ed8e72995': [
        { name: 'id', type: 'integer', attributes: [] },
        { name: 'amount_total', type: 'integer', attributes: [] },
        { name: 'created_at', type: 'timestamp without time zone', attributes: [] },
      ],
      '2020_sales': [
        { name: 'id', type: 'integer', attributes: [] },
        { name: 'price', type: 'numeric', attributes: [] },
      ],
      'my-orders': [
        { name: 'id', type: 'integer', attributes: [] },
        { name: '1st_value', type: 'integer', attributes: [] },
        { name: 'order-status', type: 'text', attributes: [] },
      ],
    },
  };

  const generate = () => {
    const template = new ScaffoldingTemplate(dbSchema, driver, {
      format: SchemaFormat.Yaml,
      snakeCase: true,
    });
    return template.generateFilesByTableNames(
      Object.keys(dbSchema.public).map((t) => ['public', t] as [string, string]),
      { dataSource: 'default' }
    );
  };

  it('generated YAML data model compiles and every cube can be queried', async () => {
    const files = generate();
    const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(
      files.map((f) => ({ fileName: f.fileName, content: f.content }))
    );

    // Currently fails with:
    //   0e4fb908_ad9a_4e3a_a21e_316ed8e72995 cube: (name = ...) fails to match the identifier pattern
    //   2020_sales cube: (name = 2020_sales) ... fails to match the identifier pattern
    //   my_orders cube: (measures.1st_value = [object Object]) is not allowed
    await compiler.compile();

    for (const cube of cubeEvaluator.cubeNames()) {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [`${cube}.count`],
      });
      expect(query.buildSqlAndParams()[0]).toBeTruthy();
    }
  });

  it('SQL identifiers starting with a digit are quoted in generated SQL', () => {
    const content = generate().map((f) => f.content).join('\n');

    // Currently generated as `sql_table: public.2020_sales` and `sql: 1st_value`
    expect(content).not.toMatch(/sql_table: public\.2020_sales/);
    expect(content).not.toMatch(/sql: 1st_value/);
  });
});
