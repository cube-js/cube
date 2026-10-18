import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareJsCompiler, prepareYamlCompiler } from './PrepareCompiler';
import { groupingDepthModel } from './fixtures/grouping-depth';

const levels = ['observations.root', 'observations.region', 'observations.site'];
const compilers = prepareJsCompiler(groupingDepthModel());
const control = prepareJsCompiler(groupingDepthModel("'1 = 1'"));

beforeAll(async () => {
  await compilers.compiler.compile();
  await control.compiler.compile();
});

const query = (options = {}, compiled = compilers) => new PostgresQuery(compiled, {
  measures: ['observations.value'], timezone: 'UTC', order: [], useNativeSqlPlanner: true, ...options
});

const depth = (options = {}, configuredLevels = levels, override = 'observations.depth') => query(options).groupingDepth(configuredLevels, override);

describe('SQL_UTILS.groupingDepth', () => {
  it.each([
    [[], 0], [['observations.root'], 0], [['observations.region'], 1],
    [['observations.site'], 2], [['observations.site', 'observations.root'], 2],
    [['report.area'], 1], [['report.site', 'report.area'], 2],
  ])('selects the deepest configured ordinal for %j', (dimensions, expected) => {
    expect(depth({ dimensions })).toBe(expected);
    expect(query({ dimensions }).buildSqlAndParams()[0]).toContain(`WHERE depth = ${expected}`);
  });

  it('uses configuration order, not dimension name or query order', () => {
    expect(depth({ dimensions: ['observations.region'] }, [...levels].reverse())).toBe(1);
    expect(depth({ dimensions: ['observations.root'] }, [...levels].reverse())).toBe(2);
  });

  it.each(['equals', 'notEquals', 'gt', 'set', 'notSet'])('defers to an explicit %s depth predicate', (operator) => {
    const filters = [{ member: 'report.depth', operator, ...(['set', 'notSet'].includes(operator) ? {} : { values: ['2'] }) }];
    expect(depth({ dimensions: ['report.area'], filters })).toBeNull();
    expect(query({ dimensions: ['report.area'], filters }).buildSqlAndParams()[0]).toContain('WHERE 1 = 1');
  });

  it('finds overrides nested under AND and OR without narrowing the OR', () => {
    expect(depth({ filters: [{ and: [{ or: [
      { member: 'observations.depth', operator: 'equals', values: ['2'] },
      { member: 'observations.region', operator: 'equals', values: ['North'] }
    ] }] }] })).toBeNull();
  });

  it('does not infer depth from a hierarchy filter', () => {
    expect(depth({ filters: [{ member: 'observations.site', operator: 'equals', values: ['Oak'] }] })).toBe(0);
  });

  it.each(['observations.value', 'observations.average', 'report.average', 'report.by_site'])('activates through %s dependencies', (measure) => {
    expect(query({ measures: [measure], dimensions: ['report.area'] }).buildSqlAndParams()[0]).toContain('WHERE depth = 1');
  });

  it('activates for a measure used only in HAVING', () => {
    const sql = query({
      measures: ['observations.count'],
      dimensions: ['observations.region'],
      filters: [{ member: 'observations.value', operator: 'gt', values: ['50'] }]
    }).buildSqlAndParams()[0];
    expect(sql).toContain('WHERE depth = 1');
    expect(sql).toMatch(/HAVING/i);
  });

  it.each([
    { measures: ['observations.count'] },
    { measures: [], dimensions: ['observations.site'] },
    { measures: ['observations.count'], useNativeSqlPlanner: false },
    { measures: ['observations.count'], dimensions: ['observations.id'], ungrouped: true },
  ])('preserves inactive SQL and parameters for %j', (options) => {
    expect(query(options).buildSqlAndParams()).toEqual(query(options, control).buildSqlAndParams());
  });

  it('keeps answers isolated across queries sharing the compiled model', () => {
    const first = query({ dimensions: ['observations.region'] });
    const second = query({ dimensions: ['observations.site'] });
    expect(first.groupingDepth(levels, 'observations.depth')).toBe(1);
    expect(second.groupingDepth(levels, 'observations.depth')).toBe(2);
    expect(first.groupingDepth(levels, 'observations.depth')).toBe(1);
    expect(query({}).groupingDepth(levels, 'observations.depth')).toBe(0);
  });

  it.each([
    [[], 'observations.depth', /nonempty ordered levels/],
    [['observations.region', 'report.area'], 'observations.depth', /distinct dimensions/],
    [levels, 'observations.region', /separate from hierarchy/],
    [['observations.created'], 'observations.depth', /time dimensions/],
    [['observations.missing'], 'observations.depth', /not found/],
    [['report.observations.region'], 'observations.depth', /join paths/],
  ])('rejects invalid configuration %j / %s', (configuredLevels, override, message) => {
    expect(() => depth({}, configuredLevels as string[], override as string)).toThrow(message as RegExp);
  });

  it.each([
    { useNativeSqlPlanner: false }, { ungrouped: true, dimensions: ['observations.id'] },
    { preAggregationQuery: true }, { totalQuery: true },
    { dimensions: [{ expression: () => 'region', expressionName: 'custom', cubeName: 'observations' }] },
    { dimensions: [{ expression: () => 'region', expressionName: 'region', cubeName: 'observations', groupingSet: { id: 1, groupType: 'rollup' } }] },
  ])('rejects unsupported active query mode %j', (options) => {
    expect(() => depth(options)).toThrow(/groupingDepth/);
  });

  it.each([[], ['observations.missing'], ['report.observations.value']].map(members => [members]))('rejects invalid active measures %j', (activeMeasures) => {
    expect(() => query().groupingDepth(levels, 'observations.depth', activeMeasures)).toThrow();
  });

  it('rejects pre-aggregations even when the rollup is on another cube', async () => {
    const compiled = prepareJsCompiler(`${groupingDepthModel()}
      cube('other', { sql: 'SELECT 1 AS id', measures: { count: { type: 'count' } },
        preAggregations: { total: { measures: [count] } } });
    `);
    await compiled.compiler.compile();
    expect(() => query({}, compiled).buildSqlAndParams()).toThrow(/pre-aggregations/);
  });

  it.each([false, true])('rejects a matching rollup with external=%s', async (external) => {
    const compiled = prepareJsCompiler(groupingDepthModel().replace("cube('observations', {", `cube('observations', {
      preAggregations: { summary: { measures: [value], dimensions: [region], external: ${external} } },`));
    await compiled.compiler.compile();
    expect(() => query({ dimensions: ['observations.region'] }, compiled).buildSqlAndParams()).toThrow(/groupingDepth.*pre-aggregations/);
  });

  it('rejects a default filter on the override dimension', async () => {
    const compiled = prepareJsCompiler(groupingDepthModel().replace("view('report', {", `view('report', {
      defaultFilters: [{ member: 'depth', operator: 'equals', values: ['1'] }],`));
    await compiled.compiler.compile();
    expect(() => query({ measures: ['report.value'] }, compiled).buildSqlAndParams()).toThrow(/default filters/);
  });

  it('supports YAML expressions without depending on JavaScript array syntax', async () => {
    const compiled = prepareYamlCompiler(`
cubes:
  - name: observations
    sql: >
      SELECT 1 AS root, 1 AS region, 1 AS depth, 10 AS value
      WHERE depth = {SQL_UTILS.groupingDepth('observations.root,observations.region'.split(','), 'observations.depth')}
    dimensions:
      - name: root
        sql: root
        type: number
      - name: region
        sql: region
        type: number
      - name: depth
        sql: depth
        type: number
    measures:
      - name: value
        sql: value
        type: sum
`);
    await compiled.compiler.compile();
    expect(query({ dimensions: ['observations.region'] }, compiled).buildSqlAndParams()[0]).toContain('WHERE depth = 1');
  });
});
