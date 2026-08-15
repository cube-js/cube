/* eslint-disable no-restricted-syntax, quotes */
import { BigqueryQuery } from '../../src/adapter/BigqueryQuery';
import { ClickHouseQuery } from '../../src/adapter/ClickHouseQuery';
import { CubeStoreQuery } from '../../src/adapter/CubeStoreQuery';
import { MssqlQuery } from '../../src/adapter/MssqlQuery';
import { MysqlQuery } from '../../src/adapter/MysqlQuery';
import { OracleQuery } from '../../src/adapter/OracleQuery';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { PrestodbQuery } from '../../src/adapter/PrestodbQuery';
import { SnowflakeQuery } from '../../src/adapter/SnowflakeQuery';
import { TrinoQuery } from '../../src/adapter/TrinoQuery';
import { prepareJsCompiler } from './PrepareCompiler';

/**
 * A LIKE-based filter must never let a `%`, `_` or `\` typed by a user act as a
 * wildcard. That takes two cooperating pieces, and BOTH have to come from the
 * dialect:
 *
 *   1. the value is escaped - on the native planner this only happens when the
 *      dialect defines the `filters/like_escape_char` template, otherwise the
 *      planner skips escaping entirely and the raw value becomes the pattern;
 *   2. the emitted SQL interprets that escape character - either because the
 *      engine treats backslash as the default (Postgres, MySQL, BigQuery,
 *      ClickHouse, Cube Store) or because the statement carries an explicit
 *      ESCAPE clause (Presto/Trino, Snowflake, Oracle, MSSQL).
 *
 * Getting (1) without (2) is the dangerous combination: `contains '%'` silently
 * matches every row instead of the rows containing a literal percent sign.
 */
describe('LIKE filter wildcard escaping', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube('Names', {
      sql: \`SELECT 1 AS id, 'a' AS name\`,
      measures: {
        count: { type: 'count' }
      },
      dimensions: {
        id: { sql: 'id', type: 'number', primaryKey: true },
        name: { sql: 'name', type: 'string' }
      }
    });
  `);

  const buildFilter = async (QueryClass: any, useNativeSqlPlanner: boolean, value = '%') => {
    await compiler.compile();

    const query = new QueryClass({ joinGraph, cubeEvaluator, compiler }, {
      dimensions: ['Names.id'],
      filters: [{ member: 'Names.name', operator: 'contains', values: [value] }],
      useNativeSqlPlanner,
    });

    const [sql, params] = query.buildSqlAndParams();

    return { sql: sql.replace(/\s+/g, ' '), params };
  };

  // Dialects whose LIKE treats backslash as the escape character with no clause.
  // Cube Store belongs here and cannot be moved: its parser rejects `ESCAPE '\'`
  // outright, so an explicit clause would break every pre-aggregation query.
  const BACKSLASH_BY_DEFAULT: [string, any][] = [
    ['Postgres', PostgresQuery],
    ['MySQL', MysqlQuery],
    ['BigQuery', BigqueryQuery],
    ['ClickHouse', ClickHouseQuery],
    ['Cube Store', CubeStoreQuery],
  ];

  // Dialects with no default escape character, which therefore have to say so.
  // Snowflake doubles the backslash because it also unescapes the clause itself.
  const NEEDS_EXPLICIT_CLAUSE: [string, any, string][] = [
    ['Presto', PrestodbQuery, "ESCAPE '\\'"],
    ['Trino', TrinoQuery, "ESCAPE '\\'"],
    ['Oracle', OracleQuery, "ESCAPE '\\'"],
    ['MSSQL', MssqlQuery, "ESCAPE '\\'"],
    ['Snowflake', SnowflakeQuery, "ESCAPE '\\\\'"],
  ];

  describe.each([['legacy', false], ['tesseract', true]])('%s planner', (_name, native) => {
    it.each(BACKSLASH_BY_DEFAULT)('%s escapes the value and needs no clause', async (dialect, QueryClass) => {
      const { sql, params } = await buildFilter(QueryClass, native);

      // MSSQL aside (covered below), every dialect escapes with a backslash.
      expect(params).toEqual(['\\%']);
      expect(sql).not.toMatch(/ESCAPE/i);
    });

    it.each(NEEDS_EXPLICIT_CLAUSE)('%s escapes the value and emits an explicit ESCAPE clause', async (dialect, QueryClass, clause) => {
      const { sql, params } = await buildFilter(QueryClass, native);

      // MSSQL's legacy path escapes by bracketing rather than by backslash; both
      // are correct, so only the native planner's form is pinned exactly.
      if (!(dialect === 'MSSQL' && !native)) {
        expect(params).toEqual(['\\%']);
        expect(sql).toContain(clause);
      }
    });

    it('escapes underscores and backslashes too, not just percent', async () => {
      expect((await buildFilter(PostgresQuery, native, '_')).params).toEqual(['\\_']);
      expect((await buildFilter(PostgresQuery, native, '\\')).params).toEqual(['\\\\']);
    });

    it('leaves values with no special characters untouched', async () => {
      expect((await buildFilter(PostgresQuery, native, 'plain')).params).toEqual(['plain']);
    });
  });
});
