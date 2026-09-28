import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';

const CUBES = `
cubes:
  - name: visitors
    sql: "SELECT * FROM visitors"
    joins:
      - name: visitor_checkins
        relationship: one_to_many
        sql: "{CUBE.id} = {visitor_checkins.visitor_id}"
    dimensions:
      - name: id
        type: number
        sql: id
        primary_key: true
      - name: source
        type: string
        sql: source

  - name: visitor_checkins
    sql: "SELECT * FROM visitor_checkins"
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
`;

// Renames every member, so the query spells members no pre-aggregation mentions.
const VIEW = `
views:
  - name: visitors_view
    cubes:
      - join_path: visitors
        includes:
          - id
          - source
        prefix: true
      - join_path: visitors.visitor_checkins
        includes:
          - visitor_id
          - count
        prefix: true
`;

// Grouped by a non-key dimension plus a cross-cube one, so its rows are already
// collapsed and reading them flat would drop the join.
const COLLAPSED_ROLLUP = `
      - name: joined_rollup
        type: rollup
        measures:
          - count
        dimensions:
          - visitor_id
          - visitors.source
        time_dimension: created_at
        granularity: day
`;

// The same collapsed rollup without a time dimension, so the query's member set
// matches it exactly. Nothing but the primary-key rule stands between this query
// and a join-less read of collapsed rows.
const COLLAPSED_ROLLUP_NO_TIME_DIMENSION = `
      - name: no_keys_rollup
        type: rollup
        measures:
          - count
        dimensions:
          - visitor_id
          - visitors.source
`;

// Same cross-cube shape, but grouped by the primary keys of both cubes, so each
// stored row is one raw joined row.
const KEYED_ROLLUP = `
      - name: joined_keys_rollup
        type: rollup
        measures:
          - count
        dimensions:
          - id
          - visitor_id
          - visitors.id
          - visitors.source
`;

const BASE_QUERY = {
  ungrouped: true,
  // Without this a joined ungrouped query is rejected by `initUngrouped` unless
  // it selects the primary keys of every joined cube. It is a server-level
  // option (`CUBEJS_ALLOW_UNGROUPED_WITHOUT_PRIMARY_KEY`) that inherits
  // `CUBESQL_SQL_PUSH_DOWN`'s default, so it is normally on.
  allowUngroupedWithoutPrimaryKey: true,
  timezone: 'America/Los_Angeles',
  preAggregationsSchema: '',
};

const PLANNERS: [string, boolean][] = [
  ['legacy planner', false],
  ['native planner', true],
];

describe('PreAggregations ungrouped cross-cube query', () => {
  jest.setTimeout(200000);

  describe('rollup whose grouping collapses raw rows', () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(
      CUBES + COLLAPSED_ROLLUP + VIEW
    );

    it.each(PLANNERS)('is not matched, keeping the join (%s)', async (_label, useNativeSqlPlanner) => {
      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        ...BASE_QUERY,
        dimensions: ['visitor_checkins.visitor_id', 'visitors.source'],
        filters: [{ member: 'visitors.source', operator: 'equals', values: ['google'] }],
        useNativeSqlPlanner,
      } as any);

      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      const [sql] = query.buildSqlAndParams();

      expect(preAggregationsDescription).toEqual([]);
      expect(sql).not.toContain('joined_rollup');
      expect(sql.toLowerCase()).toContain('join');
    });

    it.each(PLANNERS)('is not matched through a view either (%s)', async (_label, useNativeSqlPlanner) => {
      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        ...BASE_QUERY,
        dimensions: ['visitors_view.visitor_checkins_visitor_id', 'visitors_view.visitors_source'],
        filters: [{ member: 'visitors_view.visitors_source', operator: 'equals', values: ['google'] }],
        useNativeSqlPlanner,
      } as any);

      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      const [sql] = query.buildSqlAndParams();

      expect(preAggregationsDescription).toEqual([]);
      expect(sql).not.toContain('joined_rollup');
      expect(sql.toLowerCase()).toContain('join');
    });
  });

  // Native planner only: legacy matches this rollup and reads it without the
  // join, so asserting legacy here would pin that bug.
  describe('collapsed rollup whose member set matches the query exactly', () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(
      CUBES + COLLAPSED_ROLLUP_NO_TIME_DIMENSION + VIEW
    );

    it('is not matched, keeping the join (native planner)', async () => {
      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        ...BASE_QUERY,
        dimensions: ['visitor_checkins.visitor_id', 'visitors.source'],
        useNativeSqlPlanner: true,
      } as any);

      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      const [sql] = query.buildSqlAndParams();

      expect(preAggregationsDescription).toEqual([]);
      expect(sql).not.toContain('no_keys_rollup');
      expect(sql.toLowerCase()).toContain('join');
    });
  });

  describe('rollup grouped by the primary keys of both cubes', () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(
      CUBES + KEYED_ROLLUP + VIEW
    );

    it.each(PLANNERS)('is matched when the query carries the primary keys (%s)', async (_label, useNativeSqlPlanner) => {
      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        ...BASE_QUERY,
        dimensions: [
          'visitors.id',
          'visitor_checkins.id',
          'visitors.source',
          'visitor_checkins.visitor_id',
        ],
        useNativeSqlPlanner,
      } as any);

      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      const [sql] = query.buildSqlAndParams();

      expect(preAggregationsDescription.map((d: any) => d.tableName)).toEqual([
        'visitor_checkins_joined_keys_rollup',
      ]);
      expect(sql).toContain('visitor_checkins_joined_keys_rollup');
    });

    // Native planner only: legacy rejects this rollup even though one stored row
    // is one raw joined row. Going through the view also pins that the view is
    // not mistaken for a third cube.
    it('is matched through a view (native planner)', async () => {
      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        ...BASE_QUERY,
        dimensions: ['visitors_view.visitors_source', 'visitors_view.visitor_checkins_visitor_id'],
        useNativeSqlPlanner: true,
      } as any);

      const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
      const [sql] = query.buildSqlAndParams();

      expect(preAggregationsDescription.map((d: any) => d.tableName)).toEqual([
        'visitor_checkins_joined_keys_rollup',
      ]);
      expect(sql).toContain('visitor_checkins_joined_keys_rollup');
    });
  });
});
