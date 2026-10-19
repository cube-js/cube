import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { groupingFilterModel } from '../../unit/fixtures/grouping-filter';
import { dbRunner } from './PostgresDBRunner';

const allDepthsRollup = 'r: {measures:[value,weight,count],dimensions:[root,region,site,depth],external:false}';
const regionOnlyRollup = 'r: {measures:[value,weight,count],dimensions:[region],external:false}';
const queryDefaults = { measures: ['observations.value'], timezone: 'UTC', order: [], useNativeSqlPlanner: true, preAggregationsSchema: '', cubestoreSupportMultistage: true };
const depthFilter = values => ({ member: 'observations.depth', operator: 'equals', values });
const normalizeRows = (rows: any[]) => rows.map(row => Object.values(row).map(v => (typeof v === 'string' && /^-?\d+(\.\d+)?$/.test(v) ? Number(v) : v))).sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
const cases = [
  { name: 'root', options: {}, expected: [[400]], depth: 0 },
  { name: 'region', options: { dimensions: ['observations.region'] }, expected: [['North', 50], ['South', 100]], depth: 1 },
  { name: 'site', options: { dimensions: ['observations.site'] }, expected: [['Elm', 13], ['Oak', 7], ['Pine', 11]], depth: 2 },
  { name: 'alias', options: { dimensions: ['report.area'], measures: ['report.value'] }, expected: [['North', 50], ['South', 100]], depth: 1 },
  { name: 'derived', options: { dimensions: ['report.area'], measures: ['report.average'] }, expected: [['North', 50 / 3], ['South', 100 / 9]], depth: 1 },
  { name: 'multi-stage', options: { dimensions: ['report.area'], measures: ['report.by_site'] }, expected: [['North', 50], ['South', 100]], depth: 1 },
  { name: 'explicit-single', options: { filters: [depthFilter(['1'])] }, expected: [[150]] },
  { name: 'explicit-multiple', options: { filters: [depthFilter(['1', '2'])] }, expected: [[181]] },
  { name: 'explicit-or', options: { filters: [{ or: [depthFilter(['2']), { member: 'observations.region', operator: 'equals', values: ['South'] }] }] }, expected: [[131]] },
  { name: 'inactive-count', options: { measures: ['observations.count'] }, expected: [[6]] },
  { name: 'inactive-list', options: { measures: [], dimensions: ['observations.site'] }, expected: [['Elm'], ['Oak'], ['Pine']] },
  { name: 'mixed-count', options: { dimensions: ['observations.region'], measures: ['observations.value', 'observations.count'] }, expected: [['North', 50, 1], ['South', 100, 1]], depth: 1 },
  { name: 'having', options: { dimensions: ['observations.region'], measures: ['observations.count'], filters: [{ member: 'observations.value', operator: 'gt', values: ['60'] }] }, expected: [['South', 1]], depth: 1 },
];

describe('planner-visible grouping filters', () => {
  jest.setTimeout(200000);
  const shapes: Array<[string, string]> = [
    ['source', ''], ['unrelated', ''], ['region', regionOnlyRollup], ['all-depths', allDepthsRollup],
    ['original-sql', 'raw: {type: "originalSql", external: false}']
  ];

  for (const [shape, rollup] of shapes) {
    describe(shape, () => {
      const compilers = prepareJsCompiler(groupingFilterModel()
        .replace("cube('observations', {", `cube('observations', {preAggregations: { ${rollup} },`)
        + (shape === 'unrelated' ? `cube('unrelated', {sql: 'SELECT 1 AS id', measures: {count: {type: 'count'}}, preAggregations: {total: {measures: [count], external: false}}});` : ''));
      beforeAll(() => compilers.compiler.compile());
      it.each(cases)('$name', async test => {
        const options = { ...queryDefaults, ...test.options };
        const before = JSON.stringify(options);
        const query = new PostgresQuery(compilers, options);
        const sql = query.buildSqlAndParams();
        const descriptions = query.preAggregations.preAggregationsDescription();
        if (shape === 'all-depths' || shape === 'original-sql') {
          expect(descriptions).toHaveLength(1);
          expect(sql[0]).toContain(descriptions[0].tableName);
        }
        if (shape === 'region' && test.depth !== undefined) {
          expect(descriptions).toHaveLength(0);
        }
        const load = descriptions.map(d => [d.loadSql[0].replace('CREATE TABLE', 'CREATE TEMP TABLE').replace(' AS SELECT', ' ON COMMIT DROP AS SELECT'), d.loadSql[1]]);
        expect(normalizeRows(await dbRunner.testQueries([...load, sql], async () => { /* The fixture uses inline rows; no setup tables are needed. */ }))).toEqual(test.expected);
        expect(JSON.stringify(options)).toEqual(before);
      });
    });
  }
});

describe('grouping filter source pushdown', () => {
  const compilers = prepareJsCompiler(groupingFilterModel()
    .replace('WHERE 1 = 1', `WHERE \${FILTER_PARAMS.observations.depth.filter("depth")}`)
    .replace("cube('observations', {", `cube('observations', {preAggregations: { ${allDepthsRollup} },`));
  beforeAll(() => compilers.compiler.compile());
  it.each(cases.slice(0, 3))('preserves $name through FILTER_PARAMS and a rollup', async test => {
    const q = new PostgresQuery(compilers, { ...queryDefaults, ...test.options });
    const sql = q.buildSqlAndParams();
    const descriptions = q.preAggregations.preAggregationsDescription();
    expect(descriptions).toHaveLength(1);
    const load = descriptions.map(d => [d.loadSql[0].replace('CREATE TABLE', 'CREATE TEMP TABLE').replace(' AS SELECT', ' ON COMMIT DROP AS SELECT'), d.loadSql[1]]);
    expect(normalizeRows(await dbRunner.testQueries([...load, sql], async () => { /* The fixture uses inline rows; no setup tables are needed. */ }))).toEqual(test.expected);
  });
});

describe.each([false, true])('partitioned grouping filters (incremental: %s)', incremental => {
  const rule = "groupingFilter: {dimension: 'depth', levels: ['root', 'region', 'site'], measures: ['value', 'weight']},";
  const model = groupingFilterModel()
    .replace('WHERE 1 = 1', `CROSS JOIN (VALUES ('2026-01-01'::timestamp), ('2026-01-02'::timestamp)) AS days(event_day) WHERE 1 = 1`)
    .replace(`sql: "'2026-01-01'::timestamp"`, "sql: 'event_day'")
    .replace("cube('observations', {", `cube('observations', {
      preAggregations: { daily: {
        measures: [value, weight, count], dimensions: [root, region, site, depth],
        timeDimension: created, granularity: 'day', partitionGranularity: 'day', external: false,
        refreshKey: { every: '1 hour', incremental: ${incremental}, updateWindow: '7 day' }
      } },`);
  const compiled = prepareJsCompiler(model);
  const control = prepareJsCompiler(model.replace(rule, ''));
  beforeAll(async () => { await compiled.compiler.compile(); await control.compiler.compile(); });

  it.each(cases.filter(test => !['multi-stage', 'inactive-list'].includes(test.name)))('executes $name across partitions with ordinary-filter parity', async test => {
    const options = {
      ...queryDefaults,
      ...test.options,
      ...(test.name === 'having' ? { filters: [{ member: 'observations.value', operator: 'gt', values: ['120'] }] } : {}),
      timeDimensions: [{ dimension: 'observations.created', dateRange: ['2026-01-01', '2026-01-02'] }],
    };
    const query = new PostgresQuery(compiled, options);
    const baseline = new PostgresQuery(control, {
      ...options,
      filters: [...(options.filters ?? []), ...(test.depth !== undefined ? [depthFilter([String(test.depth)])] : [])],
    });
    const sql = query.buildSqlAndParams();
    expect(sql).toEqual(baseline.buildSqlAndParams());
    const descriptions = query.preAggregations.preAggregationsDescription();
    expect(descriptions).toHaveLength(1);
    expect(descriptions[0].loadSql).toEqual(baseline.preAggregations.preAggregationsDescription()[0].loadSql);
    expect(descriptions[0].invalidateKeyQueries).toEqual(baseline.preAggregations.preAggregationsDescription()[0].invalidateKeyQueries);
    descriptions[0].dateRange = ['2026-01-01T00:00:00.000', '2026-01-02T23:59:59.999'];
    const seed = 2220000 + Number(incremental) * 100 + cases.indexOf(test);
    const loads = dbRunner.tempTablePreAggregations(descriptions, seed);
    const querySql = dbRunner.replaceTableName(sql, descriptions, seed);
    // Both days contain the same independent observations. Ratios do not double.
    const expected = test.expected.map(row => row.map(value => (typeof value === 'number' && test.name !== 'derived' ? value * 2 : value)));
    expect(normalizeRows(await dbRunner.testQueries([...loads, querySql], async () => { /* Inline fixture. */ }))).toEqual(expected);
  });
});
