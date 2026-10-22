import { ScaffoldingTemplate, SchemaFormat } from '../../src/scaffolding/ScaffoldingTemplate';
import { prepareCompiler } from './PrepareCompiler';

// https://github.com/cube-js/cube/issues/2614
// Table and column names may start with a digit or contain hyphens (e.g. a UUID table
// name in BigQuery). Scaffolding must still emit cube and member names that compile.
const driver = {
  quoteIdentifier: (name: string) => `"${name}"`,
};

const IDENTIFIER = /^[_a-zA-Z][_a-zA-Z0-9]*$/;

const uuidTable = '0e4fb908-ad9a-4e3a-a21e-316ed8e72995';

const dbSchema = {
  public: {
    [uuidTable]: [
      { name: 'id', type: 'integer', attributes: ['primaryKey'] },
      { name: '2024_amount', type: 'integer', attributes: [] },
      { name: '1st_seen', type: 'timestamp', attributes: [] },
    ],
    orders: [
      { name: 'id', type: 'integer', attributes: ['primaryKey'] },
      {
        name: 'uuid_id',
        type: 'integer',
        attributes: [],
        foreign_keys: [{ target_table: uuidTable, target_column: 'id' }],
      },
    ],
  },
};

describe('Scaffolding identifiers that start with a digit', () => {
  [SchemaFormat.Yaml, SchemaFormat.JavaScript].forEach((format) => {
    [true, false].forEach((snakeCase) => {
      it(`generates a compilable model (${format}, snakeCase: ${snakeCase})`, async () => {
        const template = new ScaffoldingTemplate(dbSchema, driver, { format, snakeCase });

        const files = template.generateFilesByTableNames([`public.${uuidTable}`, 'public.orders']);
        const cubeName = files[0].fileName.replace(/\.(yml|js)$/, '');
        expect(cubeName).toMatch(IDENTIFIER);
        // An unquoted `2024_amount` reads as the number 2024 aliased `amount` in SQL.
        expect(files[0].content).toMatch(/\.\\?"2024_amount\\?"/);

        const compilers = prepareCompiler(files);
        await compilers.compile();

        const cube = compilers.cubeEvaluator.cubeFromPath(cubeName);
        const members = [...Object.keys(cube.dimensions), ...Object.keys(cube.measures)];
        members.forEach((member) => expect(member).toMatch(IDENTIFIER));

        const ordersName = files[1].fileName.replace(/\.(yml|js)$/, '');
        expect(compilers.cubeEvaluator.cubeFromPath(ordersName).joins.map((j: any) => j.name)).toEqual([cubeName]);
      });
    });
  });

  it('keeps distinct file names for non-Latin table names in camelCase', () => {
    const template = new ScaffoldingTemplate({
      public: {
        заказы: [{ name: 'id', type: 'integer', attributes: ['primaryKey'] }],
        клиенты: [{ name: 'id', type: 'integer', attributes: ['primaryKey'] }],
      },
    }, driver, { format: SchemaFormat.JavaScript, snakeCase: false });

    const fileNames = template.generateFilesByTableNames(['public.заказы', 'public.клиенты']).map((f) => f.fileName);
    expect(fileNames).toEqual(['Заказы.js', 'Клиенты.js']);
  });
});
