import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// The batch and streaming cubes split the table at a point in time, as a
// lambda's branches do, so a read that loses either branch loses rows.
const CUBES = `
cubes:
  - name: events
    sql: "SELECT * FROM visitor_checkins WHERE created_at < '2017-01-05'"
    dimensions:
      - name: id
        type: number
        sql: id
        primary_key: true
      - name: visitor_id
        type: number
        sql: visitor_id
      - name: created_at
        type: time
        sql: created_at
    measures:
      - name: count
        type: count
    pre_aggregations:
      __LAMBDA__
      - name: batch
        type: rollup
        measures:
          - count
        dimensions:
          - id
          - visitor_id
        time_dimension: created_at
        granularity: day
        partition_granularity: day

      - name: recent
        type: rollup
        measures:
          - count
        dimensions:
          - id
          - visitor_id
        time_dimension: created_at
        granularity: day
        partition_granularity: day

      - name: strict
        type: rollup
        measures:
          - count
        dimensions:
          - id
          - visitor_id
        time_dimension: created_at
        granularity: day
        partition_granularity: day
        allow_non_strict_date_range_match: false

  - name: events_stream
    sql: "SELECT * FROM visitor_checkins WHERE created_at >= '2017-01-05'"
    dimensions:
      - name: id
        type: number
        sql: id
        primary_key: true
      - name: visitor_id
        type: number
        sql: visitor_id
      - name: created_at
        type: time
        sql: created_at
    measures:
      - name: count
        type: count
    pre_aggregations:
      - name: stream
        type: rollup
        measures:
          - count
        dimensions:
          - id
          - visitor_id
        time_dimension: created_at
        granularity: day
        partition_granularity: day

  # Keyed on \`row_id\`; its \`id\` only shares the short name with \`events.id\`
  # and is not unique.
  - name: events_keyless_stream
    sql: "SELECT * FROM visitor_checkins WHERE created_at >= '2017-01-05'"
    dimensions:
      - name: row_id
        type: number
        sql: id
        primary_key: true
      - name: id
        type: number
        sql: visitor_id
      - name: visitor_id
        type: number
        sql: visitor_id
      - name: created_at
        type: time
        sql: created_at
    measures:
      - name: count
        type: count
    pre_aggregations:
      - name: stream
        type: rollup
        measures:
          - count
        dimensions:
          - id
          - visitor_id
        time_dimension: created_at
        granularity: day
        partition_granularity: day
`;

// Declared ahead of its member rollups, so a refused lambda would otherwise hand
// the query to its batch branch.
const lambdaOver = (rollups: string[]) => CUBES.replace('__LAMBDA__', `- name: lambda
        type: rollupLambda
        rollups:
${rollups.map(r => `          - ${r}`).join('\n')}
`);

const QUERY = {
  measures: ['events.count'],
  dimensions: ['events.id', 'events.visitor_id'],
  timeDimensions: [{
    dimension: 'events.created_at',
    granularity: 'day',
  }],
  order: [{ id: 'events.id' }],
  ungrouped: true,
  timezone: 'UTC',
  preAggregationsSchema: '',
};

const PARTIAL_DAYS_QUERY = {
  measures: ['events.count'],
  timeDimensions: [{
    dimension: 'events.created_at',
    granularity: 'day',
    dateRange: ['2017-01-03T10:00:00.000', '2017-01-05T12:00:00.000'],
  }],
  order: [{ id: 'events.created_at' }],
  timezone: 'UTC',
  preAggregationsSchema: '',
};

const PLANNERS: [string, boolean][] = [
  ['legacy planner', false],
  ['native planner', true],
];

const usedPreAggregations = (query: any): string[] => query.preAggregations?.preAggregationsDescription()
  .map((d: any) => d.preAggregationId);

describe('PreAggregations ungrouped query over a rollupLambda', () => {
  jest.setTimeout(200000);

  describe('with both branches on the same cube', () => {
    const compilers = prepareYamlCompiler(lambdaOver(['events.batch', 'events.recent']));

    it.each(PLANNERS)('is served by the lambda (%s)', async (_label, useNativeSqlPlanner) => {
      await compilers.compile();

      const query = compilers.createQuery(PostgresQuery, { ...QUERY, useNativeSqlPlanner } as any);

      expect(usedPreAggregations(query)).toEqual(['events.batch', 'events.recent']);
    });
  });

  describe('with a branch on another cube', () => {
    const compilers = prepareYamlCompiler(lambdaOver(['events.batch', 'events_stream.stream']));

    it.each(PLANNERS)('is served by the lambda (%s)', async (_label, useNativeSqlPlanner) => {
      await compilers.compile();

      const query = compilers.createQuery(PostgresQuery, { ...QUERY, useNativeSqlPlanner } as any);

      expect(usedPreAggregations(query)).toEqual(['events.batch', 'events_stream.stream']);
    });

    // Native planner only: legacy renders a non-external lambda as an unaliased
    // subquery that Postgres refuses to run.
    it('returns the rows of both branches (native planner)', async () => {
      await compilers.compile();

      const query = compilers.createQuery(PostgresQuery, { ...QUERY, useNativeSqlPlanner: true } as any);

      const res = await dbRunner.evaluateQueryWithPreAggregations(query);
      expect(res).toEqual([
        { events__id: 1, events__visitor_id: 1, events__created_at_day: '2017-01-03T00:00:00.000Z', events__count: '1' },
        { events__id: 2, events__visitor_id: 1, events__created_at_day: '2017-01-04T00:00:00.000Z', events__count: '1' },
        { events__id: 3, events__visitor_id: 1, events__created_at_day: '2017-01-05T00:00:00.000Z', events__count: '1' },
        { events__id: 4, events__visitor_id: 2, events__created_at_day: '2017-01-05T00:00:00.000Z', events__count: '1' },
        { events__id: 5, events__visitor_id: 2, events__created_at_day: '2017-01-05T00:00:00.000Z', events__count: '1' },
        { events__id: 6, events__visitor_id: 3, events__created_at_day: '2017-01-06T00:00:00.000Z', events__count: '1' },
      ]);
    });
  });

  // Neither branch states date-range strictness, so both take the lenient
  // default, and so must the lambda.
  describe('grouped query over partial days', () => {
    const compilers = prepareYamlCompiler(lambdaOver(['events.batch', 'events_stream.stream']));

    it.each(PLANNERS)('is served by the lambda (%s)', async (_label, useNativeSqlPlanner) => {
      await compilers.compile();

      const query = compilers.createQuery(PostgresQuery, { ...PARTIAL_DAYS_QUERY, useNativeSqlPlanner } as any);

      expect(usedPreAggregations(query)).toEqual(['events.batch', 'events_stream.stream']);
    });

    // Native planner only, for the same reason as the ungrouped rows above.
    it('counts the rows of both branches (native planner)', async () => {
      await compilers.compile();

      const query = compilers.createQuery(PostgresQuery, { ...PARTIAL_DAYS_QUERY, useNativeSqlPlanner: true } as any);

      // The range is applied to day-truncated values, so the 3rd starts before it.
      // The 5th lives only in the streaming branch.
      const res = await dbRunner.evaluateQueryWithPreAggregations(query);
      expect(res).toEqual([
        { events__created_at_day: '2017-01-04T00:00:00.000Z', events__count: '1' },
        { events__created_at_day: '2017-01-05T00:00:00.000Z', events__count: '3' },
      ]);
    });
  });

  // Native planner only: legacy refuses this lambda too, but then answers from
  // its lenient batch branch alone.
  describe('grouped query over partial days, strict through the first branch', () => {
    const compilers = prepareYamlCompiler(lambdaOver(['events.strict', 'events.batch']));

    it('skips the batch branch for the next rollup (native planner)', async () => {
      await compilers.compile();

      const query = compilers.createQuery(PostgresQuery, { ...PARTIAL_DAYS_QUERY, useNativeSqlPlanner: true } as any);

      expect(usedPreAggregations(query)).toEqual(['events.recent']);
    });
  });

  // Native planner only: legacy matches this lambda and returns the streaming
  // branch's collapsed rows, so asserting legacy here would pin that bug.
  describe('with a branch that does not store its key', () => {
    const compilers = prepareYamlCompiler(lambdaOver(['events.batch', 'events_keyless_stream.stream']));

    // `events.recent` is no branch of this lambda, so it stays a candidate.
    it('skips the batch branch for the next rollup (native planner)', async () => {
      await compilers.compile();

      const query = compilers.createQuery(PostgresQuery, { ...QUERY, useNativeSqlPlanner: true } as any);

      expect(usedPreAggregations(query)).toEqual(['events.recent']);
    });
  });
});
