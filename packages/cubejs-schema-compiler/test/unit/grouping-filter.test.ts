import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareJsCompiler, prepareYamlCompiler } from './PrepareCompiler';
import { groupingFilterModel } from './fixtures/grouping-filter';

const rule = "groupingFilter: {dimension: 'depth', levels: ['root', 'region', 'site'], measures: ['value', 'weight']},";
const compiled = prepareJsCompiler(groupingFilterModel());
const control = prepareJsCompiler(groupingFilterModel().replace(rule, ''));
const query = (options = {}, c = compiled) => new PostgresQuery(c, { measures: ['observations.value'], timezone: 'UTC', order: [], useNativeSqlPlanner: true, ...options });
beforeAll(async () => { await compiled.compiler.compile(); await control.compiler.compile(); });

describe('grouping filter normalization', () => {
  it('generates exactly the ordinary filter control SQL and parameters', () => {
    for (const [dimensions, depth] of [[[], 0], [['report.area'], 1], [['report.site'], 2]] as Array<[string[], number]>) {
      expect(query({ dimensions }).buildSqlAndParams()).toEqual(query({ dimensions, filters: [{ member: 'observations.depth', operator: 'equals', values: [String(depth)] }] }, control).buildSqlAndParams());
    }
  });

  it('does not mutate caller options, compiled metadata, or repeat filters across calls', () => {
    const options = { dimensions: ['report.area'], filters: [{ member: 'report.site', operator: 'equals', values: ['Oak'] }] };
    const before = JSON.stringify(options);
    const definitions = JSON.stringify(compiled.cubeEvaluator.groupingFilters);
    const first = query(options);
    expect(first.options.filters).toHaveLength(2);
    expect(first.buildSqlAndParams()).toEqual(first.buildSqlAndParams());
    expect(query({ dimensions: ['report.site'] }).options.filters[0].values).toEqual(['2']);
    expect(query(options).options.filters).toEqual(first.options.filters);
    expect(JSON.stringify(options)).toEqual(before);
    expect(JSON.stringify(compiled.cubeEvaluator.groupingFilters)).toEqual(definitions);
  });

  it('does not apply a consumer scope to pre-aggregation builds', () => {
    const options = { preAggregationQuery: true, dimensions: ['observations.site'] };
    expect(query(options).buildSqlAndParams()).toEqual(query(options, control).buildSqlAndParams());
  });

  it.each(['equals', 'notEquals', 'gt', 'set', 'notSet'])('preserves explicit %s overrides', operator => {
    const options = { filters: [{ member: 'report.depth', operator, ...(['set', 'notSet'].includes(operator) ? {} : { values: ['1'] }) }] };
    expect(query(options).buildSqlAndParams()).toEqual(query(options, control).buildSqlAndParams());
  });

  it.each([
    { measures: ['observations.count'] }, { measures: [], dimensions: ['report.site'] },
    { measures: ['observations.count'], useNativeSqlPlanner: false },
  ])('leaves inactive options unchanged: %j', options => {
    expect(query(options).buildSqlAndParams()).toEqual(query(options, control).buildSqlAndParams());
  });

  it.each([
    { useNativeSqlPlanner: false }, { ungrouped: true }, { totalQuery: true },
    { dimensions: [{ expression: () => 'region', expressionName: 'custom', cubeName: 'observations' }] },
  ])('rejects unsupported active query modes: %j', options => {
    expect(() => query(options)).toThrow(/groupingFilter/);
  });

  it.each([
    "{dimension: 'depth', levels: [], measures: ['value']}",
    "{dimension: 'region', levels: ['root'], measures: ['value']}",
    "{dimension: 'depth', levels: ['root', 'depth'], measures: ['value']}",
    "{dimension: 'depth', levels: ['root'], measures: []}",
    "{dimension: 'depth', levels: ['created'], measures: ['value']}",
    "{dimension: 'depth', levels: ['missing'], measures: ['value']}",
  ])('rejects malformed model rules: %s', async replacement => {
    const c = prepareJsCompiler(groupingFilterModel().replace(rule, `groupingFilter: ${replacement},`));
    await expect(c.compiler.compile()).rejects.toThrow();
  });

  it('accepts YAML configuration as the same model rule', async () => {
    const c = prepareYamlCompiler(`
cubes:
  - name: sample
    sql: SELECT 'all' AS root, 0 AS depth, 10 AS value
    grouping_filter:
      dimension: depth
      levels: [root]
      measures: [value]
    dimensions:
      - name: root
        sql: root
        type: string
      - name: depth
        sql: depth
        type: number
    measures:
      - name: value
        sql: value
        type: sum
`);
    await c.compiler.compile();
    expect(query({ measures: ['sample.value'] }, c).options.filters).toEqual([{ member: 'sample.depth', operator: 'equals', values: ['0'] }]);
  });
  it('does not interfere with an unrelated joined cube instance', async () => {
    const unrelated = `
      cube('people', {sql: 'SELECT 1 AS id, 20 AS score', dimensions: {id: {sql: 'id', type: 'number', primaryKey: true}}, measures: {score: {sql: 'score', type: 'sum'}}});
      cube('tickets', {sql: 'SELECT 1 AS id, 1 AS person_id', dimensions: {id: {sql: 'id', type: 'number', primaryKey: true}}, measures: {count: {type: 'count'}}, joins: [{name: 'people', alias: 'owner', relationship: 'many_to_one', sql: \`\${CUBE}.person_id = \${people}.id\`}]});
    `;
    const automatic = prepareJsCompiler(groupingFilterModel() + unrelated);
    const plain = prepareJsCompiler(groupingFilterModel().replace(rule, '') + unrelated);
    await automatic.compiler.compile(); await plain.compiler.compile();
    const options = { measures: ['tickets.owner.score'], dimensions: ['tickets.id'] };
    const baseline = query(options, plain).buildSqlAndParams();
    expect(query(options, automatic).buildSqlAndParams()).toEqual(baseline);
  });

  it('rejects activating a rule through an unsupported joined cube instance', async () => {
    const model = `${groupingFilterModel()}
      cube('links', {sql: 'SELECT 1 AS id, 1 AS observation_id', dimensions: {id: {sql: 'id', type: 'number', primaryKey: true}}, measures: {count: {type: 'count'}}, joins: [{name: 'observations', alias: 'record', relationship: 'many_to_one', sql: \`\${CUBE}.observation_id = \${observations}.id\`}]});
    `;
    const c = prepareJsCompiler(model); await c.compiler.compile();
    expect(() => query({ measures: ['links.record.value'], dimensions: ['links.id'] }, c)).toThrow(/joined cube instances/);
  });
});

describe('grouping filter model boundaries', () => {
  it.each([
    "{dimension: 'depth', levels: ['root', 'root'], measures: ['value']}",
    "{dimension: 'depth', levels: ['root'], measures: ['value', 'value']}",
    "{dimension: 'depth', levels: ['root'], measures: ['missing']}",
    "{dimension: 'depth', levels: ['observations.root'], measures: ['value']}",
    "{dimension: 'observations.depth', levels: ['root'], measures: ['value']}",
  ])('rejects ambiguous or nonlocal configuration: %s', async replacement => {
    const c = prepareJsCompiler(groupingFilterModel().replace(rule, `groupingFilter: ${replacement},`));
    await expect(c.compiler.compile()).rejects.toThrow(/groupingFilter/);
  });

  it('uses the deepest grouped level regardless of requested order or hierarchy filters', () => {
    const options = {
      dimensions: ['report.site', 'report.area'],
      filters: [{ member: 'report.area', operator: 'equals', values: ['North'] }],
    };
    expect(query(options).buildSqlAndParams()).toEqual(query({
      ...options,
      filters: [...options.filters, { member: 'observations.depth', operator: 'equals', values: ['2'] }],
    }, control).buildSqlAndParams());
    expect(query({ filters: options.filters }).options.filters.slice(-1)[0].values).toEqual(['0']);
  });

  it('preserves a nested explicit depth override through a view alias', () => {
    const options = {
      dimensions: ['report.area'],
      filters: [{ and: [
        { member: 'report.area', operator: 'equals', values: ['North'] },
        { or: [
          { member: 'report.depth', operator: 'equals', values: ['1'] },
          { member: 'report.depth', operator: 'equals', values: ['2'] },
        ] },
      ] }],
    };
    expect(query(options).buildSqlAndParams()).toEqual(query(options, control).buildSqlAndParams());
  });

  it('rejects a conflicting canonical default instead of silently intersecting it', async () => {
    const c = prepareJsCompiler(groupingFilterModel().replace("view('report', {", `view('report', {
      defaultFilters: [{ member: 'depth', operator: 'equals', values: ['2'] }],`));
    await c.compiler.compile();
    expect(() => query({ measures: ['report.value'], dimensions: ['report.area'] }, c)).toThrow(/conflicts with a default filter/);
  });

  it('preserves an unrelated view default alongside automatic depth selection', async () => {
    const model = groupingFilterModel().replace("view('report', {", `view('report', {
      defaultFilters: [{ member: 'area', operator: 'equals', values: ['North'] }],`);
    const c = prepareJsCompiler(model);
    const plain = prepareJsCompiler(model.replace(rule, ''));
    await c.compiler.compile();
    await plain.compiler.compile();
    const options = { measures: ['report.value'], dimensions: ['report.area'] };
    expect(query(options, c).buildSqlAndParams()).toEqual(query({
      ...options,
      filters: [{ member: 'observations.depth', operator: 'equals', values: ['1'] }],
    }, plain).buildSqlAndParams());
  });

  it('rejects unsupported custom subquery joins before selecting a scope', () => {
    expect(() => query({ subqueryJoins: [{}] })).toThrow(/supported aggregate/);
  });
});
