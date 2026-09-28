import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// `extends` hands the extending cube the very definitions of the cube it extends,
// so every reference resolved per cube — a multi-stage `grain:`/`filter:`, a view
// default filter, a pre-aggregation's output column names — has to be written into
// an object owned by that cube. Written into the shared one it resolves to whichever
// cube is prepared last, and the other cubes end up carrying member paths of a cube
// that is not theirs.
describe('Multi-stage members of a cube that another cube extends', () => {
  const baseFact = `
  - name: base_fact
    sql: "SELECT 1 AS id, 1 AS dim_id, '2026-01-01'::date AS d, 10 AS v"
    joins:
      - name: dims
        sql: "{CUBE}.dim_id = {dims}.id"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: d
        sql: "{CUBE}.d"
        type: time
      - name: v
        sql: "{CUBE}.v"
        type: number
    measures:
      - name: v_sum
        sql: "{v}"
        type: sum
      - name: daily_v
        multi_stage: true
        sql: "{v_sum}"
        type: number
      - name: linked
        multi_stage: true
        sql: "{daily_v}"
        type: sum
        grain:
          include:
            - d
      - name: combining
        multi_stage: true
        sql: "CASE WHEN {d} IS NOT NULL THEN {daily_v} ELSE 0 END"
        type: max
        grain:
          include:
            - d
      - name: outer_combining
        multi_stage: true
        sql: "{combining}"
        type: number
      - name: v_sum_all_dates
        multi_stage: true
        sql: "{v_sum}"
        type: number
        filter:
          exclude:
            - d
`;

  const dims = `
  - name: dims
    sql: "SELECT 1 AS id, 'a' AS name"
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: name
        sql: "{CUBE}.name"
        type: string
`;

  const childFact = `
  - name: child_fact
    extends: base_fact
    sql: "SELECT 1 AS id, 1 AS dim_id, '2026-01-01'::date AS d, 10 AS v, 'x' AS tag"
    dimensions:
      - name: tag
        sql: "{CUBE}.tag"
        type: string
`;

  const model = (withChild: boolean) => `cubes:${dims}${baseFact}${withChild ? childFact : ''}`;

  const compile = async (withChild: boolean) => {
    const compilers = prepareYamlCompiler(model(withChild));
    await compilers.compiler.compile();
    return compilers;
  };

  const buildSql = async (withChild: boolean, query: any, useNativeSqlPlanner: boolean) => {
    const compilers = await compile(withChild);
    return new PostgresQuery(compilers, {
      timezone: 'UTC',
      useNativeSqlPlanner,
      ...query,
    }).buildSqlAndParams();
  };

  it('resolves grain references against the cube that declares the member', async () => {
    const { cubeEvaluator } = await compile(true);

    expect(cubeEvaluator.evaluatedCubes.base_fact.measures.linked.grain?.includeReferences)
      .toEqual(['base_fact.d']);
    expect(cubeEvaluator.evaluatedCubes.child_fact.measures.linked.grain?.includeReferences)
      .toEqual(['child_fact.d']);
  });

  it('resolves filter references against the cube that declares the member', async () => {
    const { cubeEvaluator } = await compile(true);

    expect(cubeEvaluator.evaluatedCubes.base_fact.measures.v_sum_all_dates.filter?.excludeReferences)
      .toEqual(['base_fact.d']);
    expect(cubeEvaluator.evaluatedCubes.child_fact.measures.v_sum_all_dates.filter?.excludeReferences)
      .toEqual(['child_fact.d']);
  });

  describe.each([
    ['native', true],
    ['legacy', false],
  ])('%s planner', (_name, useNativeSqlPlanner) => {
    // Planning the parent's member must not depend on the extending cube being
    // there at all, so the plan is compared against the same model without it.
    const expectSamePlanWithAndWithoutChild = async (query: any) => {
      const [withoutChild] = await buildSql(false, query, useNativeSqlPlanner);
      const [withChild] = await buildSql(true, query, useNativeSqlPlanner);
      expect(withChild).toEqual(withoutChild);
    };

    it('plans a multi-stage measure with an explicit grain', async () => {
      await expectSamePlanWithAndWithoutChild({ measures: ['base_fact.linked'] });
      await expect(buildSql(true, { measures: ['child_fact.linked'] }, useNativeSqlPlanner)).resolves.toBeDefined();
    });

    it('plans a chained multi-stage measure reading a grain dimension', async () => {
      await expectSamePlanWithAndWithoutChild({ measures: ['base_fact.outer_combining'] });
      await expectSamePlanWithAndWithoutChild({
        measures: ['base_fact.outer_combining'],
        dimensions: ['dims.name'],
      });
    });

    it('plans a multi-stage measure with a filter directive', async () => {
      await expectSamePlanWithAndWithoutChild({
        measures: ['base_fact.v_sum_all_dates'],
        timeDimensions: [{
          dimension: 'base_fact.d',
          granularity: 'month',
          dateRange: ['2026-01-01', '2026-01-31'],
        }],
      });
    });

    it('plans a plain measure of a cube that another cube extends', async () => {
      await expect(buildSql(true, { measures: ['base_fact.v_sum'] }, useNativeSqlPlanner)).resolves.toBeDefined();
    });
  });
});

describe('View default filters of a view that another view extends', () => {
  const model = `
cubes:
  - name: orders
    sql: "SELECT 1 AS id, 'usd' AS currency"
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: currency
        sql: "{CUBE}.currency"
        type: string
    measures:
      - name: count
        type: count

views:
  - name: base_view
    cubes:
      - join_path: orders
        includes:
          - currency
          - count
    default_filters:
      - member: currency
        operator: equals
        values: ["usd"]

  - name: child_view
    extends: base_view
`;

  it('resolves the filter member against the view that declares the filter', async () => {
    const { compiler, cubeEvaluator } = prepareYamlCompiler(model);
    await compiler.compile();

    expect(cubeEvaluator.evaluatedCubes.base_view.defaultFilters?.map(f => f.memberReference))
      .toEqual(['base_view.currency']);
    expect(cubeEvaluator.evaluatedCubes.child_view.defaultFilters?.map(f => f.memberReference))
      .toEqual(['child_view.currency']);
  });
});

describe('Pre-aggregations of a cube that another cube extends', () => {
  const model = `
cubes:
  - name: base
    sql: "SELECT 1 AS id, '2026-01-01'::date AS d"
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: d
        sql: "{CUBE}.d"
        type: time
    measures:
      - name: count
        type: count
    pre_aggregations:
      - name: main
        dimensions:
          - id
        measures:
          - count
        time_dimension: d
        granularity: day
        output_column_types:
          - member: id
            type: integer

  - name: child
    extends: base
    sql: "SELECT 1 AS id, '2026-01-01'::date AS d, 'x' AS tag"
    dimensions:
      - name: tag
        sql: "{CUBE}.tag"
        type: string
`;

  it('resolves output column names against the cube that declares the pre-aggregation', async () => {
    const { compiler, cubeEvaluator } = prepareYamlCompiler(model);
    await compiler.compile();

    const names = (cube: string) => (cubeEvaluator.evaluatedCubes[cube].preAggregations.main as any)
      .outputColumnTypes.map((c: any) => c.name);

    expect(names('base')).toEqual(['base.id']);
    expect(names('child')).toEqual(['child.id']);
  });
});
