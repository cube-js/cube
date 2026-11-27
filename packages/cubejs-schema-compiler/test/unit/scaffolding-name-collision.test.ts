import { ScaffoldingTemplate, SchemaFormat } from '../../src/scaffolding/ScaffoldingTemplate';

// https://github.com/cube-js/cube/issues/13060
// Distinct source tables must not be scaffolded into the same cube name / output file.
// Both the Playground (`/playground/generate-schema`, snakeCase) and `cubejs generate`
// (camelCase) write each returned file to `model/cubes/<fileName>`, so a collision
// silently drops (or, with concurrent writes, corrupts) one of the generated cubes.
const driver = {
  quoteIdentifier: (name: string) => `"${name}"`,
};

const columns = [{ name: 'id', type: 'integer', attributes: ['primaryKey'] }];

const hyphenVsUnderscore = { public: { 'foo-bar': columns, foo_bar: columns } };
const sameTableInTwoSchemas = { public: { orders: columns }, analytics: { orders: columns } };

describe('Scaffolding cube name collisions', () => {
  const cases: [string, Record<string, Record<string, any>>, string[], boolean][] = [
    ['camelCase: foo-bar vs foo_bar', hyphenVsUnderscore, ['public.foo-bar', 'public.foo_bar'], false],
    ['snakeCase: foo-bar vs foo_bar', hyphenVsUnderscore, ['public.foo-bar', 'public.foo_bar'], true],
    ['camelCase: same table name in two schemas', sameTableInTwoSchemas, ['public.orders', 'analytics.orders'], false],
    ['snakeCase: same table name in two schemas', sameTableInTwoSchemas, ['public.orders', 'analytics.orders'], true],
  ];

  for (const [title, dbSchema, tables, snakeCase] of cases) {
    for (const format of [SchemaFormat.JavaScript, SchemaFormat.Yaml]) {
      it(`${title} (${format}) produces distinct file names`, () => {
        const template = new ScaffoldingTemplate(dbSchema, driver, { format, snakeCase });
        const files = template.generateFilesByTableNames(tables);

        expect(files).toHaveLength(tables.length);
        expect(new Set(files.map(f => f.fileName)).size).toBe(tables.length);
      });
    }
  }
});
