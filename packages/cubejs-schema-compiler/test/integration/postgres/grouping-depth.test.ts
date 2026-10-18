import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { groupingDepthModel } from '../../unit/fixtures/grouping-depth';
import { dbRunner } from './PostgresDBRunner';

const compilers = prepareJsCompiler(groupingDepthModel());
const control = prepareJsCompiler(groupingDepthModel("'1 = 1'"));
const regionControl = prepareJsCompiler(groupingDepthModel("'depth = 1'"));

beforeAll(async () => {
  await compilers.compiler.compile();
  await control.compiler.compile();
  await regionControl.compiler.compile();
});

const execute = async (options = {}, compiled = compilers) => {
  const query = new PostgresQuery(compiled, {
    measures: ['observations.value'], timezone: 'UTC', order: [], useNativeSqlPlanner: true, ...options
  });
  const rows = await dbRunner.testQuery(query.buildSqlAndParams(), async () => { /* Inline VALUES need no database fixture. */ });
  const normalize = (value: unknown) => {
    if (typeof value === 'string' && /^-?\d+(\.\d+)?$/.test(value)) {
      return Number(value);
    }
    return value;
  };
  return rows.map(row => Object.values(row).map(normalize))
    .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
};

describe('grouping depth source selection', () => {
  jest.setTimeout(200000);

  it('reads one depth per aggregate branch instead of summing all depths', async () => {
    expect(await execute()).toEqual([[120]]);
    expect(await execute({ dimensions: ['observations.region'] })).toEqual([['North', 45], ['South', 75]]);
    expect(await execute({ dimensions: ['observations.site'] })).toEqual([['Elm', 75], ['Oak', 20], ['Pine', 25]]);
    expect(await execute({}, control)).toEqual([[360]]);
  });

  it('uses the same context through view aliases and derived measures', async () => {
    expect(await execute({ dimensions: ['report.area'], measures: ['report.average'] })).toEqual([['North', 15], ['South', 75 / 9]]);
  });

  it('retains outer grouping depth when a multi-stage measure introduces an inner grain', async () => {
    const options = { dimensions: ['report.area'], measures: ['report.by_site'] };
    const expected = [['North', 45], ['South', 75]];
    expect(await execute(options, regionControl)).toEqual(expected);
    expect(await execute(options)).toEqual(expected);
  });

  it('honors explicit singleton and multi-value depth filters', async () => {
    expect(await execute({ filters: [{ member: 'observations.depth', operator: 'equals', values: ['2'] }] })).toEqual([[120]]);
    expect(await execute({ filters: [{ member: 'observations.depth', operator: 'equals', values: ['1', '2'] }] })).toEqual([[240]]);
  });

  it('preserves an explicit OR predicate rather than intersecting it with a default', async () => {
    expect(await execute({ filters: [{ or: [
      { member: 'observations.depth', operator: 'equals', values: ['2'] },
      { member: 'observations.region', operator: 'equals', values: ['South'] }
    ] }] })).toEqual([[195]]);
  });

  it('preserves inactive counts and dimension-only lists', async () => {
    expect(await execute({ measures: ['observations.count'] })).toEqual([[6]]);
    expect(await execute({ measures: [], dimensions: ['observations.site'] })).toEqual([['Elm'], ['Oak'], ['Pine']]);
  });

  it('applies an activated source predicate to every measure sharing that source', async () => {
    expect(await execute({ dimensions: ['observations.region'], measures: ['observations.value', 'observations.count'] }))
      .toEqual([['North', 45, 1], ['South', 75, 1]]);
  });

  it('activates a measure that appears only in HAVING', async () => {
    expect(await execute({
      dimensions: ['observations.region'],
      measures: ['observations.count'],
      filters: [{ member: 'observations.value', operator: 'gt', values: ['50'] }]
    })).toEqual([['South', 1]]);
  });

  it('does not leak context when queries are interleaved on a shared compiled model', async () => {
    const results = await Promise.all([
      execute(), execute({ dimensions: ['observations.site'] }),
      execute({ measures: ['observations.count'] }), execute()
    ]);
    expect(results).toEqual([[[120]], [['Elm', 75], ['Oak', 20], ['Pine', 25]], [[6]], [[120]]]);
  });
});
