import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { ClickHouseQuery } from '../../../src/adapter/ClickHouseQuery';
import { ClickHouseDbRunner } from './ClickHouseDbRunner';

// Every entry of `types` is a name the dialect hands to a cast, so each one has to be a
// type this data source really has. ClickHouse takes several of them in one casing only,
// holds no NULL in a type unless the type says so, and rejects the whole query for the
// rest — none of which is visible from the rendered SQL alone.
describe('ClickHouse cast types', () => {
  jest.setTimeout(200000);

  const dbRunner = new ClickHouseDbRunner();

  // The casts stand on their own, so none of these queries reads a table
  const noDataSet = async () => Promise.resolve();

  afterAll(async () => {
    await dbRunner.tearDown();
  });

  const compilers = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: "SELECT 1 AS id, 1 AS product_id"
    joins:
      - name: payments
        sql: "{CUBE}.id = {payments}.order_id"
        relationship: one_to_many
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: product_id
        sql: "{CUBE}.product_id"
        type: number
        primary_key: true
    measures:
      - name: count
        type: count
  - name: payments
    sql: "SELECT 1 AS id, 1 AS order_id, 10 AS amount"
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
    measures:
      - name: amount
        sql: "{CUBE}.amount"
        type: sum
`);

  const query = (options: any = {}) => new ClickHouseQuery(compilers, { timezone: 'UTC', ...options });

  // A value each type can hold, so that the cast under test is what a failure is about
  const CASTABLE_VALUE: Record<string, string> = {
    string: '\'a\'',
    boolean: '1',
    tinyint: '1',
    smallint: '1',
    integer: '1',
    bigint: '1',
    float: '1',
    double: '1',
    decimal: '1',
    timestamp: '\'2020-01-01 10:00:00\'',
    date: '\'2020-01-01\'',
  };

  const fillIn = (template: string) => template
    .replace('{{ precision }}', '10')
    .replace('{{ scale }}', '2');

  let types: Record<string, string>;

  beforeAll(async () => {
    await compilers.compiler.compile();
    types = query().sqlTemplates().types;
  });

  it('names a type this ClickHouse takes for every value it casts', async () => {
    // Read off the dialect rather than listed here, so a type added later cannot quietly
    // escape the check. `nullable` is not a type of its own and is covered below.
    const castTypes = Object.entries(types).filter(([name]) => name !== 'nullable');
    expect(castTypes.length).toBeGreaterThan(5);

    for (const [name, template] of castTypes) {
      const value = CASTABLE_VALUE[name];
      expect(value).toBeDefined();

      const [row] = await dbRunner.testQuery(
        [`SELECT CAST(${value} AS ${fillIn(template)}) AS v`, []],
        noDataSet,
      );
      expect(row.v).toBeDefined();
    }
  });

  it('holds a NULL in the nullable form of a type', async () => {
    // No ClickHouse type takes a NULL unless the type itself says so, so the dialect has
    // to name that form for a cast that produces one
    expect(types.nullable).toBeDefined();

    const nullableString = types.nullable.replace('{{ data_type }}', types.string);

    const [row] = await dbRunner.testQuery(
      [`SELECT CAST(NULL AS ${nullableString}) AS v`, []],
      noDataSet,
    );

    expect(row.v).toBeNull();
  });

  it('runs the count it renders for a composite primary key', async () => {
    const [sql, params] = query({ measures: ['orders.count', 'payments.amount'] }).buildSqlAndParams();

    const [row] = await dbRunner.testQuery([sql, params], noDataSet);

    expect(Number(row.orders__count)).toEqual(1);
  });
});
