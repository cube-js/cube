import { MssqlQuery } from '../../../src/adapter/MssqlQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './MSSqlDbRunner';

describe('MSSQL offset without order', () => {
  jest.setTimeout(200000);

  const compilers = prepareJsCompiler(`
    cube(\`visitors\`, {
      sql: \`select * from ##visitors\`,

      measures: {
        count: { type: 'count' }
      },

      dimensions: {
        id: { type: 'number', sql: 'id', primaryKey: true },
        source: { type: 'string', sql: 'source' }
      }
    })
  `);

  async function run(query: Record<string, unknown>) {
    await compilers.compile();
    const [sql, params] = compilers.createQuery(MssqlQuery, query).buildSqlAndParams();
    return { sql, rows: await dbRunner.testQuery([sql, params]) as Record<string, unknown>[] };
  }

  it('skips the only row of a measures-only query', async () => {
    const { sql, rows } = await run({
      measures: ['visitors.count'],
      rowLimit: 10000,
      offset: 1,
    });
    expect(sql).toMatch(/ORDER BY[\s\S]*OFFSET 1 ROWS/);
    expect(rows).toEqual([]);
  });

  it('skips rows of a grouped query with an explicitly empty order', async () => {
    const all = await run({
      measures: ['visitors.count'],
      dimensions: ['visitors.source'],
      order: [],
      rowLimit: 10000,
    });
    const { sql, rows } = await run({
      measures: ['visitors.count'],
      dimensions: ['visitors.source'],
      order: [],
      rowLimit: 10000,
      offset: 1,
    });
    expect(sql).toMatch(/ORDER BY[\s\S]*OFFSET 1 ROWS/);
    expect(all.rows).toHaveLength(3);
    expect(rows).toHaveLength(2);
    expect(all.rows).toEqual(expect.arrayContaining(rows));
  });

  it('keeps TOP when there is no offset', async () => {
    const { sql, rows } = await run({
      measures: ['visitors.count'],
      rowLimit: 10000,
    });
    expect(sql).toMatch(/SELECT TOP 10000/);
    expect(sql).not.toMatch(/ORDER BY/);
    expect(rows).toEqual([{ visitors__count: '6' }]);
  });
});
