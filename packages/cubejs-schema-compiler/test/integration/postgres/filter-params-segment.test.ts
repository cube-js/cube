import { getEnv } from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// A cube wrapping a large scan pushes predicates into its own `sql` through
// FILTER_PARAMS. A segment can be named there like any other member: its
// binding renders the column it was given whenever the query selects that
// segment, and `1 = 1` when it does not.
describe('FILTER_PARAMS referencing a segment', () => {
  jest.setTimeout(200000);

  const events = `
    SELECT * FROM (
      SELECT 1 as id, 115 as evid, 'load' as action_group, 'us' as region
      union all
      SELECT 2 as id, 115 as evid, 'load' as action_group, 'eu' as region
      union all
      SELECT 3 as id, 200 as evid, 'click' as action_group, 'us' as region
    ) AS t
  `;

  const compilers = prepareJsCompiler(`
    cube('events', {
      sql: \`${events} WHERE \${FILTER_PARAMS.events.start_load.filter("evid = 115 AND action_group = 'load'")}
        AND \${FILTER_PARAMS.events.region.filter('region')}\`,
      measures: {
        count: { type: 'count' },
      },
      dimensions: {
        id: { sql: 'id', type: 'number', primaryKey: true },
        region: { sql: 'region', type: 'string' },
      },
      segments: {
        start_load: { sql: \`\${CUBE}.evid = 115 AND \${CUBE}.action_group = 'load'\` },
        us_only: { sql: \`\${CUBE}.region = 'us'\` },
      },
    });

    cube('grouped_events', {
      sql: \`${events} WHERE \${FILTER_GROUP(
        FILTER_PARAMS.grouped_events.start_load.filter("evid = 115 AND action_group = 'load'"),
        FILTER_PARAMS.grouped_events.region.filter('region')
      )}\`,
      measures: {
        count: { type: 'count' },
      },
      dimensions: {
        id: { sql: 'id', type: 'number', primaryKey: true },
        region: { sql: 'region', type: 'string' },
      },
      segments: {
        start_load: { sql: \`\${CUBE}.evid = 115 AND \${CUBE}.action_group = 'load'\` },
      },
    });

    cube('callback_events', {
      sql: \`${events} WHERE \${FILTER_PARAMS.callback_events.start_load.filter(() => "evid = 115")}
        AND \${FILTER_PARAMS.callback_events.needs_value.filter((v) => 'evid = ' + v)}\`,
      measures: {
        count: { type: 'count' },
      },
      dimensions: {
        id: { sql: 'id', type: 'number', primaryKey: true },
      },
      segments: {
        start_load: { sql: \`\${CUBE}.evid = 115\` },
        needs_value: { sql: \`\${CUBE}.evid = 115\` },
      },
    });
  `);

  // The cube's `sql` is a subquery aliased as the cube, so everything before
  // that alias is what the pushdown produced.
  const baseSql = (sql: string, cube: string) => {
    const alias = sql.indexOf(`AS "${cube}"`);
    // Without the alias the slice would be the whole query, and the negative
    // assertions would silently stop testing the pushed-down part.
    expect(alias).toBeGreaterThan(-1);
    return sql.slice(0, alias);
  };

  const buildSql = async (query: any) => {
    await compilers.compiler.compile();
    return new PostgresQuery(compilers, { timezone: 'UTC', ...query }).buildSqlAndParams()[0];
  };

  if (getEnv('nativeSqlPlanner')) {
    it('pushes the segment predicate into the cube sql when the segment is selected', async () => {
      const sql = await buildSql({
        measures: ['events.count'],
        segments: ['events.start_load'],
      });

      expect(baseSql(sql, 'events')).toMatch(/evid = 115 AND action_group = 'load'/);
    });

    it('leaves the binding always-true when the segment is not selected', async () => {
      const sql = await buildSql({ measures: ['events.count'] });

      expect(baseSql(sql, 'events')).not.toMatch(/evid = 115/);
      expect(baseSql(sql, 'events')).toMatch(/1\s*=\s*1/);
    });

    it('does not activate the binding for a different segment', async () => {
      const sql = await buildSql({
        measures: ['events.count'],
        segments: ['events.us_only'],
      });

      expect(baseSql(sql, 'events')).not.toMatch(/evid = 115/);
    });

    it('pushes the segment down alongside a dimension filter', async () => {
      const sql = await buildSql({
        measures: ['events.count'],
        segments: ['events.start_load'],
        filters: [{ member: 'events.region', operator: 'equals', values: ['us'] }],
      });

      const base = baseSql(sql, 'events');
      expect(base).toMatch(/evid = 115 AND action_group = 'load'/);
      expect(base).toMatch(/region = \$\d/);
      expect(base).not.toMatch(/1\s*=\s*1/);
    });

    it('renders the segment as one member of a FILTER_GROUP', async () => {
      const sql = await buildSql({
        measures: ['grouped_events.count'],
        segments: ['grouped_events.start_load'],
      });

      expect(baseSql(sql, 'grouped_events')).toMatch(/evid = 115 AND action_group = 'load'/);
    });

    it('renders a callback column that takes no filter values', async () => {
      const sql = await buildSql({
        measures: ['callback_events.count'],
        segments: ['callback_events.start_load'],
      });

      // Nothing else in this cube's sql states the predicate, so it can only
      // have come from the callback the binding compiled.
      expect(baseSql(sql, 'callback_events')).toMatch(/WHERE \(evid = 115\)/);
    });

    // A segment supplies no values, so a column that takes one cannot render.
    // Dropping only its restatement is narrower than binding a value the
    // segment never gave — the segment still filters the query on its own.
    it('leaves a value-taking callback column always-true', async () => {
      const sql = await buildSql({
        measures: ['callback_events.count'],
        segments: ['callback_events.needs_value'],
      });

      // The parentheses are what the filter renderer adds around a binding it
      // reached, so they tell an activated-then-dropped column apart from the
      // bare `1 = 1` of a binding whose segment was never selected.
      expect(baseSql(sql, 'callback_events')).toMatch(/AND \(1\s*=\s*1\)/);
      expect(baseSql(sql, 'callback_events')).not.toMatch(/undefined|\{fpv:/);
    });

    // The predicate now applies both inside the cube's sql and in the outer
    // WHERE the segment always produced. Both restrict the same rows, so the
    // result must be what the segment alone selected.
    it('counts the segment rows once', async () => dbRunner.runQueryTest({
      measures: ['events.count'],
      segments: ['events.start_load'],
      timezone: 'UTC',
    }, [
      { events__count: '2' },
    ], compilers));

    it('counts every row when no segment is selected', async () => dbRunner.runQueryTest({
      measures: ['events.count'],
      timezone: 'UTC',
    }, [
      { events__count: '3' },
    ], compilers));
  } else {
    // Segment pushdown is implemented in the Tesseract planner only.
    test.skip('FILTER_PARAMS referencing a segment', () => { expect(1).toBe(1); });
  }
});
