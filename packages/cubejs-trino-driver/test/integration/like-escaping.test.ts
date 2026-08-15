/* eslint-disable no-restricted-syntax, quotes */
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { prepareCompiler, PrestodbQuery } from '@cubejs-backend/schema-compiler';
import { TrinoDriver } from '../../src/TrinoDriver';

/**
 * End-to-end check that LIKE-based filters (`contains`, `notContains`,
 * `startsWith`, `endsWith`) treat `%`, `_` and `\` in the user-supplied value
 * as literals rather than as LIKE wildcards.
 *
 * Trino, unlike Postgres, has NO default escape character for LIKE: the schema
 * compiler escapes the value (`%` -> `\%`) AND has to emit an explicit
 * `ESCAPE '\'` clause, otherwise `\%` degrades into "a backslash followed by
 * anything" and the filter silently matches the wrong rows. The driver then
 * interpolates the parameter as an ANSI string literal, where a backslash must
 * NOT be doubled. This test exercises that whole chain against a real Trino,
 * for both the legacy and the Tesseract (native) planner.
 */
describe('Trino LIKE filter escaping', () => {
  jest.setTimeout(10 * 60 * 1000);

  // id -> name. Chosen so a leaked wildcard yields a visibly different result
  // set from a correctly escaped literal.
  const ROWS: [number, string][] = [
    [1, '50% off'],
    [2, 'discount 50 percent'],
    [3, 'no discount'],
    [4, 'a_b'],
    [5, 'axb'],
    [6, 'back\\slash'],
    [7, '100%'],
  ];

  // The cube's `sql` lives inside a template literal in the data model source,
  // so backslashes must be doubled to survive that layer. Trino string literals
  // themselves do not process backslash escapes, so what reaches the database is
  // a single backslash.
  const schemaSql = ROWS
    .map(([id, name]) => `SELECT ${id} AS id, '${name.replace(/\\/g, '\\\\')}' AS name`)
    .join(' UNION ALL ');

  const compilers = prepareCompiler({
    localPath: () => __dirname,
    dataSchemaFiles: () => Promise.resolve([{
      fileName: 'Names.js',
      content: `
        cube('Names', {
          sql: \`${schemaSql}\`,
          measures: {
            count: {
              type: 'count'
            }
          },
          dimensions: {
            id: {
              sql: 'id',
              type: 'number',
              primaryKey: true
            },
            name: {
              sql: 'name',
              type: 'string'
            }
          }
        });
      `,
    }]),
  }, { adapter: 'prestodb' });

  let container: StartedTestContainer | null = null;
  let driver: TrinoDriver;

  beforeAll(async () => {
    await compilers.compiler.compile();

    let host: string;
    let port: string;

    if (process.env.TEST_TRINO_HOST) {
      host = process.env.TEST_TRINO_HOST;
      port = process.env.TEST_TRINO_PORT || '8080';
    } else {
      container = await new GenericContainer(`trinodb/trino:${process.env.TEST_TRINO_VERSION || '476'}`)
        .withExposedPorts(8080)
        .withWaitStrategy(Wait.forLogMessage('======== SERVER STARTED ========'))
        .withStartupTimeout(5 * 60 * 1000)
        .start();
      host = container.getHost();
      port = `${container.getMappedPort(8080)}`;
    }

    driver = new TrinoDriver({
      host,
      port,
      catalog: 'memory',
      schema: 'default',
      user: 'test',
      dataSource: 'default',
    });

    await driver.testConnection();
  });

  afterAll(async () => {
    await driver?.release();

    if (container) {
      await container.stop();
    }
  });

  describe.each([
    ['legacy planner', false],
    ['tesseract planner', true],
  ])('%s', (_name, useNativeSqlPlanner) => {
    /**
     * Builds the query with the real schema compiler and runs it through the
     * real driver, returning the ids of the matched rows.
     */
    const idsMatching = async (operator: string, values: string[]) => {
      const query = new PrestodbQuery(compilers, {
        dimensions: ['Names.id'],
        filters: [{ member: 'Names.name', operator, values }],
        order: [{ id: 'Names.id', desc: false }],
        useNativeSqlPlanner,
      });

      const [sql, params] = query.buildSqlAndParams();

      // Trino has no implicit LIKE escape character - without this clause the
      // escaping the compiler applies to the value is actively harmful.
      expect(sql).toContain(`ESCAPE '\\'`);

      const rows = await driver.query(sql, params);

      return rows
        .map((row: any) => Number(row[Object.keys(row)[0]]))
        .sort((a: number, b: number) => a - b);
    };

    it('contains treats % as a literal, not a wildcard', async () => {
      // Not [1..7]: a leaked `%` wildcard would match every row.
      expect(await idsMatching('contains', ['%'])).toEqual([1, 7]);
    });

    it('contains treats _ as a literal, not a single-char wildcard', async () => {
      // Not [4, 5]: a leaked `_` wildcard would also match `axb`.
      expect(await idsMatching('contains', ['_'])).toEqual([4]);
    });

    it('contains treats a backslash as a literal', async () => {
      expect(await idsMatching('contains', ['\\'])).toEqual([6]);
    });

    it('contains matches a literal % inside a longer value', async () => {
      expect(await idsMatching('contains', ['50%'])).toEqual([1]);
    });

    it('notContains treats % as a literal, not a wildcard', async () => {
      // Not []: a leaked `%` wildcard would exclude every row.
      expect(await idsMatching('notContains', ['%'])).toEqual([2, 3, 4, 5, 6]);
    });

    it('startsWith treats % as a literal, not a wildcard', async () => {
      expect(await idsMatching('startsWith', ['100%'])).toEqual([7]);
    });

    it('endsWith treats % as a literal, not a wildcard', async () => {
      expect(await idsMatching('endsWith', ['% off'])).toEqual([1]);
    });

    it('contains still matches values with no special characters', async () => {
      expect(await idsMatching('contains', ['discount'])).toEqual([2, 3]);
    });
  });
});
