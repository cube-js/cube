import { BigqueryQuery } from '../../src/adapter/BigqueryQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// Pins the cast chosen per value: driver-suite rows cannot show that fractional and
// out-of-range values fall back to FLOAT64.
describe('BigQuery number filter cast', () => {
  const compilers = prepareYamlCompiler(`
cubes:
  - name: fact
    sql_table: ds.fact
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: org_id
        sql: org_id
        type: number
    measures:
      - name: count
        type: count
`);

  beforeAll(async () => {
    await compilers.compiler.compile();
  });

  function build(useNativeSqlPlanner: boolean, filters: any[]) {
    const query = compilers.createQuery(BigqueryQuery, {
      measures: ['fact.count'],
      filters,
      timezone: 'UTC',
      useNativeSqlPlanner,
    });
    const [sql, params] = query.buildSqlAndParams();
    return { sql, params };
  }

  describe.each([
    ['tesseract', true],
    ['legacy', false],
  ])('%s', (_name, useNativeSqlPlanner) => {
    it('casts a whole number to INT64', () => {
      const { sql, params } = build(useNativeSqlPlanner, [
        { member: 'fact.org_id', operator: 'equals', values: ['9007199254740993'] },
      ]);

      expect(sql).toMatch(/org_id\s*=\s*CAST\(\?\s+AS INT64\)/);
      expect(sql).not.toMatch(/FLOAT64/);
      expect(params).toEqual(['9007199254740993']);
    });

    it('keeps FLOAT64 for fractional and out-of-range values', () => {
      const { sql, params } = build(useNativeSqlPlanner, [
        { member: 'fact.org_id', operator: 'gt', values: ['4.5'] },
        { member: 'fact.org_id', operator: 'lt', values: ['99999999999999999999'] },
        { member: 'fact.id', operator: 'gte', values: ['1e3'] },
      ]);

      expect(sql).toMatch(/org_id\s*>\s*CAST\(\?\s+AS FLOAT64\)/);
      expect(sql).toMatch(/org_id\s*<\s*CAST\(\?\s+AS FLOAT64\)/);
      expect(sql).toMatch(/id\s*>=\s*CAST\(\?\s+AS FLOAT64\)/);
      expect(sql).not.toMatch(/INT64/);
      expect(params).toEqual(['4.5', '99999999999999999999', '1e3']);
    });

    it('casts each value of an IN list by its own shape', () => {
      const { sql, params } = build(useNativeSqlPlanner, [
        { member: 'fact.org_id', operator: 'equals', values: ['-7', '2.5'] },
      ]);

      expect(sql).toMatch(/org_id\s+IN\s+\(CAST\(\?\s+AS INT64\),\s*CAST\(\?\s+AS FLOAT64\)\)/);
      expect(params).toEqual(['-7', '2.5']);
    });
  });
});
