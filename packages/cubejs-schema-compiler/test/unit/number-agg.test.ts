import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { UserError } from '../../src/compiler/UserError';
import { prepareYamlCompiler } from './PrepareCompiler';

describe('number_agg measures', () => {
  const compilers = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string
    measures:
      - name: max_amount_agg
        sql: "MAX({CUBE}.amount)"
        type: number_agg
`);

  beforeAll(async () => {
    await compilers.compile();
  });

  it('rejects a custom aggregate in the legacy planner', () => {
    const buildLegacy = () => compilers.createQuery(PostgresQuery, {
      measures: ['orders.max_amount_agg'],
      dimensions: ['orders.status'],
      useNativeSqlPlanner: false,
    }).buildSqlAndParams();

    expect(buildLegacy).toThrow(UserError);
    expect(buildLegacy).toThrow("Measure 'orders.max_amount_agg' uses the number_agg type, which requires the Tesseract SQL planner");
  });

  it('generates a custom aggregate without multi_stage', () => {
    const query = compilers.createQuery(PostgresQuery, {
      measures: ['orders.max_amount_agg'],
      dimensions: ['orders.status'],
      useNativeSqlPlanner: true,
    });
    const [sql] = query.buildSqlAndParams();

    expect(sql).toContain('MAX("orders".amount)');
    expect(sql).not.toMatch(/\w+\(MAX\("orders"\.amount\)\)/);
    expect(sql).toContain('GROUP BY');
  });
});
