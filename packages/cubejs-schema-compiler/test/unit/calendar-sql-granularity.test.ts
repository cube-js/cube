import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

describe('Calendar cube granularities defined with sql', () => {
  // language=YAML
  const modelWith = (granularity: string) => `
cubes:
  - name: fiscal_calendar
    calendar: true
    sql: >
      SELECT '2023-12-17'::DATE AS cal_date, '2023-12-17'::DATE AS wk_start_dt UNION ALL
      SELECT '2023-12-24'::DATE, '2023-12-24'::DATE
    dimensions:
      - name: date_key
        sql: cal_date
        type: time
        primary_key: true
      - name: date
        sql: cal_date
        type: time
        granularities:
${granularity}

  - name: sales
    sql: >
      SELECT 1 AS id, '2023-12-24'::DATE AS date, 100 AS amount
    joins:
      - name: fiscal_calendar
        sql: "{CUBE}.date = {fiscal_calendar.date_key}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: total_amount
        sql: amount
        type: sum
`;

  const sqlOverride = (name: string) => `          - name: ${name}
            sql: "{CUBE}.wk_start_dt"`;

  const compile = async (granularity: string) => {
    const compilers = prepareYamlCompiler(modelWith(granularity));
    await compilers.compiler.compile();
    return compilers;
  };

  it('rejects a sql override named after a non-predefined granularity', async () => {
    await expect(compile(sqlOverride('fiscal_week'))).rejects.toThrow(
      /granularity defined with 'sql' must be named after one of the predefined granularities/
    );
  });

  it('accepts a sql override whose predefined name differs in case', async () => {
    // Predefined names resolve case-insensitively, so this one resolves today.
    await expect(compile(sqlOverride('Week'))).resolves.toBeDefined();
  });

  describe.each([
    ['legacy planner', false],
    ['native planner', true],
  ])('%s', (_name, useNativeSqlPlanner) => {
    const newQuery = (compilers: any, query: any) => new PostgresQuery(
      compilers,
      { ...query, timezone: 'UTC', useNativeSqlPlanner }
    );

    it('keeps queries that do not touch the calendar cube working', async () => {
      const compilers = await compile(sqlOverride('week'));
      const query = newQuery(compilers, { measures: ['sales.total_amount'] });

      expect(() => query.preAggregations.canUseTransformedQuery()).not.toThrow();
      expect(() => query.buildSqlAndParams()).not.toThrow();
    });

    it('keeps queries using the overridden granularity working', async () => {
      const compilers = await compile(sqlOverride('week'));
      const query = newQuery(compilers, {
        measures: ['sales.total_amount'],
        timeDimensions: [{ dimension: 'fiscal_calendar.date', granularity: 'week' }],
      });

      expect(() => query.preAggregations.canUseTransformedQuery()).not.toThrow();
      expect(() => query.buildSqlAndParams()).not.toThrow();
    });

    it('keeps hierarchies of interval based granularities', async () => {
      const compilers = await compile(`          - name: fortnight
            interval: 2 week
            origin: "2025-01-01"`);
      const query = newQuery(compilers, { measures: ['sales.total_amount'] });

      expect(query.granularityHierarchies()['fiscal_calendar.date.fortnight'])
        .toEqual(['fortnight', 'day', 'hour', 'minute', 'second']);
    });
  });

  describe('granularity without an interval reaching the query', () => {
    // Validation rejects such a granularity, so it is injected into the compiled
    // model to check that it stays contained to the queries that ask for it.
    const compileWithInjected = async () => {
      const compilers = await compile(sqlOverride('week'));
      const dimension: any = compilers.cubeEvaluator.symbols.fiscal_calendar.date;
      dimension.granularities.fiscal_week = { sql: () => 'wk_start_dt' };
      return compilers;
    };

    it('does not affect queries that do not use it', async () => {
      const compilers = await compileWithInjected();
      const query = new PostgresQuery(compilers, {
        measures: ['sales.total_amount'],
        timezone: 'UTC',
      });

      expect(() => query.preAggregations.canUseTransformedQuery()).not.toThrow();
      expect(query.granularityHierarchies()['fiscal_calendar.date.fiscal_week']).toBeUndefined();
    });

    it('fails with a readable error when a query uses it', async () => {
      const compilers = await compileWithInjected();

      expect(() => new PostgresQuery(compilers, {
        measures: ['sales.total_amount'],
        timeDimensions: [{ dimension: 'fiscal_calendar.date', granularity: 'fiscal_week' }],
        timezone: 'UTC',
      })).toThrow(/is defined with 'sql', which is only supported for predefined granularities/);
    });
  });

  it('renders the calendar sql for the overridden granularity', async () => {
    const compilers = await compile(sqlOverride('week'));
    const query = new PostgresQuery(compilers, {
      measures: ['sales.total_amount'],
      timeDimensions: [{ dimension: 'fiscal_calendar.date', granularity: 'week' }],
      timezone: 'UTC',
      useNativeSqlPlanner: true,
    });

    expect(query.buildSqlAndParams()[0]).toContain('wk_start_dt');
  });
});
