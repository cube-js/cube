import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';
import { DuckDBDriver } from '../../src';
import { DuckDBQuery } from '../../src/DuckDBQuery';

const prepareCompiler = (content: string) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'main.js', content }
  ])
}, { adapter: 'postgres' });

describe('DuckDBQuery SQL templates', () => {
  // DuckDB has no default LIKE escape character - the `default_escape` gate on
  // its `expressions.like`/`ilike` templates is the repo's own record of that.
  // The native planner escapes `%`, `_` and `\` in the filter value (BaseQuery's
  // `like_escape_char`), so the statement has to carry the clause that
  // interprets that escaping; without one a user searching for a literal `%`
  // matches nothing instead of the rows containing a percent sign.
  it.each([['legacy', false], ['tesseract', true]])(
    'escapes LIKE wildcards in filter values on the %s planner',
    async (_name, useNativeSqlPlanner) => {
      const compilers = prepareCompiler(`
        cube('orders', {
          sql_table: 'orders',

          measures: {
            count: {
              type: 'count',
            },
          },

          dimensions: {
            id: {
              sql: 'id',
              type: 'number',
              primary_key: true,
            },
            status: {
              sql: 'status',
              type: 'string',
            },
          },
        });
      `);

      await compilers.compile();

      const query = compilers.createQuery(DuckDBQuery, {
        measures: ['orders.count'],
        filters: [{ member: 'orders.status', operator: 'contains', values: ['%'] }],
        useNativeSqlPlanner,
      });

      const [sql, params] = query.buildSqlAndParams();

      expect(params).toEqual(['\\%']);

      // Only the native planner emits the clause: the legacy path relies on
      // DuckDB reading a bare backslash as the escape character, which is the
      // behaviour it has always had here.
      if (useNativeSqlPlanner) {
        // eslint-disable-next-line quotes -- double quotes keep the SQL readable
        expect(sql).toContain("ESCAPE '\\'");
      } else {
        expect(sql).not.toContain('ESCAPE');
      }
    }
  );

  it('counts unit boundaries crossed in DATEDIFF', async () => {
    const compilers = prepareCompiler(`
      cube('orders', {
        sql_table: 'orders',
        measures: { count: { type: 'count' } },
      });
    `);
    await compilers.compile();

    const template = compilers.createQuery(DuckDBQuery, { measures: ['orders.count'] })
      .sqlTemplates().functions.DATEDIFF;
    const datediff = (part: string, start: string, end: string) => template
      .replaceAll('{{ date_part }}', part)
      .replace('{{ args[1] }}', `TIMESTAMP '${start}'`)
      .replace('{{ args[2] }}', `TIMESTAMP '${end}'`);

    const driver = new DuckDBDriver({});

    try {
      const [row] = await driver.query(`SELECT
        ${datediff('day', '2024-01-01 23:59:00', '2024-01-02 00:01:00')} AS day,
        ${datediff('week', '2024-01-07 23:00:00', '2024-01-08 01:00:00')} AS week,
        ${datediff('week', '2024-01-08 00:00:00', '2024-01-07 23:00:00')} AS week_back,
        ${datediff('month', '2024-01-04 00:00:00', '2024-02-02 00:00:00')} AS month
      `, []);

      expect(row).toEqual({ day: '1', week: '1', week_back: '-1', month: '1' });
    } finally {
      await driver.release();
    }
  });
});
