import { getEnv } from '@cubejs-backend/shared';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// A rollup stores raw values, so a masked member read from it must get the mask;
// a mask the rollup can't render may fail the query or go to the source, never be skipped.
describe('PreAggregations with masked members', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: workers
    sql_table: public.workers
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: full_name
        sql: full_name
        type: string
      - name: gender
        sql: gender
        type: string
      - name: gender_source_mask
        sql: gender
        type: string
        mask:
          sql: "CONCAT('***', {CUBE}.gender)"
      - name: gender_stored_member_mask
        sql: gender
        type: string
        mask:
          sql: "CONCAT('***', {full_name})"
      - name: full_name_upper
        sql: "UPPER({full_name})"
        type: string
    measures:
      - name: count
        type: count
      - name: full_name_length
        sql: "LENGTH({full_name})"
        type: sum
    segments:
      - name: females
        sql: "{gender} = 'F'"
    pre_aggregations:
      - name: by_gender
        measures:
          - count
          - full_name_length
        dimensions:
          - full_name
          - full_name_upper
          - gender
          - gender_source_mask
          - gender_stored_member_mask
        segments:
          - females

views:
  - name: people
    cubes:
      - join_path: workers
        includes: "*"
`);

  const buildQuery = (query: Record<string, any>, maskedMembers: string[]) => {
    const pgQuery = new PostgresQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        ...query,
        maskedMembers: maskedMembers.map(member => ({ member })),
        timezone: 'UTC',
        preAggregationsSchema: '',
      }
    );
    const preAggregations: any[] = pgQuery.preAggregations?.preAggregationsDescription() || [];
    const [sql] = pgQuery.buildSqlAndParams();
    return { sql, tableNames: preAggregations.map(p => p.tableName) };
  };

  beforeAll(async () => {
    await compiler.compile();
  });

  it('masks a cube dimension read from the rollup', () => {
    const { sql, tableNames } = buildQuery(
      { dimensions: ['workers.gender'], measures: ['workers.count'] },
      ['workers.gender']
    );

    expect(tableNames).toEqual(['workers_by_gender']);
    expect(sql).toMatch(/\(?NULL\)? "workers__gender"/);
    expect(sql).not.toContain('"workers__gender" "workers__gender"');
  });

  it('masks a dimension-only query read from the rollup', () => {
    const { sql, tableNames } = buildQuery({ dimensions: ['workers.gender'] }, ['workers.gender']);

    expect(tableNames).toEqual(['workers_by_gender']);
    expect(sql).toMatch(/\(?NULL\)? "workers__gender"/);
    expect(sql).not.toContain('"workers__gender" "workers__gender"');
  });

  it('masks a view member whose cube member is masked', () => {
    const { sql, tableNames } = buildQuery({ dimensions: ['people.gender'] }, ['workers.gender']);

    expect(tableNames).toEqual(['workers_by_gender']);
    expect(sql).not.toContain('"workers__gender" "people__gender"');
  });

  it('masks a view member read from the rollup', () => {
    const { sql, tableNames } = buildQuery(
      { dimensions: ['people.gender'] },
      ['people.gender', 'workers.gender']
    );

    expect(tableNames).toEqual(['workers_by_gender']);
    expect(sql).toMatch(/\(?NULL\)? "people__gender"/);
    expect(sql).not.toContain('"workers__gender" "people__gender"');
  });

  it('masks a dimension used only in a filter', () => {
    const { sql, tableNames } = buildQuery(
      {
        measures: ['people.count'],
        filters: [{ member: 'people.gender', operator: 'equals', values: ['F'] }],
      },
      ['people.gender', 'workers.gender']
    );

    expect(tableNames).toEqual(['workers_by_gender']);
    expect(sql).toMatch(/\(?NULL\)? = \$1/);
    expect(sql).not.toContain('"workers__gender" = $1');
  });

  it('renders a mask over a stored member on top of the rollup', () => {
    const { sql, tableNames } = buildQuery(
      { dimensions: ['people.gender_stored_member_mask'] },
      ['people.gender_stored_member_mask', 'workers.gender_stored_member_mask']
    );

    expect(tableNames).toEqual(['workers_by_gender']);
    expect(sql).toContain('CONCAT(\'***\', "workers__full_name")');
  });

  it('does not serve a mask over a source column raw', () => {
    const { sql, tableNames } = buildQuery(
      { dimensions: ['people.gender_source_mask'] },
      ['people.gender_source_mask', 'workers.gender_source_mask']
    );

    if (getEnv('nativeSqlPlanner')) {
      expect(tableNames).toEqual([]);
    }
    expect(sql).toContain('CONCAT(\'***\', "workers".gender)');
    expect(sql).not.toContain('"workers__gender_source_mask" "people__gender_source_mask"');
  });

  // Stored columns computed from a masked member hold raw-derived values, so
  // such a query is answered from the source, where the mask applies.
  const tesseractIt = getEnv('nativeSqlPlanner') ? it : it.skip;

  tesseractIt('does not serve a stored dimension computed from a masked member', () => {
    const { sql, tableNames } = buildQuery({ dimensions: ['people.full_name_upper'] }, ['workers.full_name']);

    expect(tableNames).toEqual([]);
    expect(sql).toContain('UPPER((NULL))');
  });

  tesseractIt('does not serve a stored measure computed from a masked member', () => {
    const { sql, tableNames } = buildQuery({ measures: ['people.full_name_length'] }, ['workers.full_name']);

    expect(tableNames).toEqual([]);
    expect(sql).toContain('LENGTH((NULL))');
  });

  tesseractIt('does not serve a stored segment computed from a masked member', () => {
    const { sql, tableNames } = buildQuery(
      { measures: ['people.count'], segments: ['people.females'] },
      ['workers.gender']
    );

    expect(tableNames).toEqual([]);
    expect(sql).toContain('(NULL) = \'F\'');
  });
});
