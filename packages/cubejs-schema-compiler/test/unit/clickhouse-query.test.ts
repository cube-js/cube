import { ClickHouseQuery } from '../../src/adapter/ClickHouseQuery';
import { prepareJsCompiler } from './PrepareCompiler';

describe('ClickHouseQuery', () => {
  const compilers = prepareJsCompiler(`
    cube('events', {
      sql: \`SELECT * FROM events\`,
      measures: {
        count: { type: 'count' }
      },
      dimensions: {
        id: { sql: 'id', type: 'number', primaryKey: true },
        createdAt: { sql: 'created_at', type: 'time' }
      }
    });
  `);

  // https://github.com/cube-js/cube/issues/9051
  it.each([
    ['legacy planner', false],
    ['Tesseract', true],
  ])('keeps sub-second precision of a raw time dimension (%s)', async (_planner, useNativeSqlPlanner) => {
    await compilers.compile();

    const query = compilers.createQuery(ClickHouseQuery, {
      dimensions: ['events.createdAt'],
      order: [{ id: 'events.createdAt', desc: true }],
      ungrouped: true,
      allowUngroupedWithoutPrimaryKey: true,
      timezone: 'UTC',
      // Enabled by default in the server (CUBESQL_SQL_PUSH_DOWN)
      convertTzForRawTimeDimension: true,
      useNativeSqlPlanner,
    });
    const [sql] = query.buildSqlAndParams();

    // A DateTime64(3) column must not be truncated to whole seconds
    expect(sql).toContain('created_at');
    expect(sql).not.toContain('toDateTime64(`events`.created_at, 0)');
  });
});
