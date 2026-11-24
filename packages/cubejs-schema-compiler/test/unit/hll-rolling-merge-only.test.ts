import { BigqueryQuery } from '../../src/adapter/BigqueryQuery';
import { PrestodbQuery } from '../../src/adapter/PrestodbQuery';
import { SnowflakeQuery } from '../../src/adapter/SnowflakeQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// A rolling window over a source-database rollup unions the stored HLL sketches
// per day, then unions those across the window: the per-day union must stay a
// sketch, since only the outer union may take its cardinality.
describe('Rolling count_distinct_approx over a source-database rollup', () => {
  const compilers = prepareYamlCompiler(`
cubes:
  - name: sessions
    sql: SELECT * FROM sessions

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: created_at
        sql: created_at
        type: time

    measures:
      - name: users_approx
        sql: user_id
        type: count_distinct_approx

      - name: weekly_users_approx
        sql: user_id
        type: count_distinct_approx
        rolling_window:
          trailing: 1 week

    pre_aggregations:
      - name: daily
        measures:
          - users_approx
          - weekly_users_approx
        time_dimension: created_at
        granularity: day
`);

  beforeAll(async () => {
    await compilers.compile();
  });

  const dialects = [
    { name: 'Snowflake', queryClass: SnowflakeQuery, cardinality: 'HLL_ESTIMATE' },
    { name: 'BigQuery', queryClass: BigqueryQuery, cardinality: 'HLL_COUNT.MERGE' },
    { name: 'Presto', queryClass: PrestodbQuery, cardinality: 'cardinality' },
  ];

  // Matches `fn(` only as a whole name: `HLL_COUNT.MERGE(` is not `HLL_COUNT.MERGE_PARTIAL(`.
  const count = (sql: string, fn: string) => {
    const escaped = fn.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return (sql.match(new RegExp(`(?<![\\w.])${escaped}\\(`, 'g')) || []).length;
  };

  describe.each(dialects)('$name', ({ queryClass, cardinality }) => {
    function buildSql(measure: string): string {
      const query = compilers.createQuery(queryClass, {
        measures: [measure],
        timeDimensions: [{
          dimension: 'sessions.created_at',
          granularity: 'day',
          dateRange: ['2024-01-01', '2024-01-10'],
        }],
        timezone: 'UTC',
        preAggregationsSchema: '',
      });
      const [sql] = query.buildSqlAndParams();
      const usedPreAggregations = query.preAggregations?.preAggregationsDescription() || [];
      expect(usedPreAggregations.map((p: any) => p.preAggregationId)).toEqual(['sessions.daily']);
      return sql;
    }

    it('keeps the per-day union a sketch and takes the cardinality once', () => {
      const sql = buildSql('sessions.weekly_users_approx');

      expect(count(sql, cardinality)).toBe(1);
    });

    it('reads a non-rolling measure with one cardinality', () => {
      const sql = buildSql('sessions.users_approx');

      expect(count(sql, cardinality)).toBe(1);
    });
  });
});
