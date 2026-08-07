/* eslint-disable no-restricted-syntax, quotes */
import { PrestodbQuery } from '../../src/adapter/PrestodbQuery';
import { TrinoQuery } from '../../src/adapter/TrinoQuery';
import { prepareJsCompiler } from './PrepareCompiler';

// Trino/Presto reject timezone arithmetic over DATE:
// "Type of value must be a time or timestamp with/without time zone (actual date)".
// Scaffolding maps DATE columns to `type: time` without a cast, so `convertTz`
// has to promote the field to a timestamp itself. The promotion must not be a
// plain `CAST(... AS TIMESTAMP)`: that strips the zone off a
// `timestamp with time zone` column and shifts the converted value.
describe('Trino/Presto time dimensions over DATE columns', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube('events', {
      sql: \`
        SELECT
          1 AS id,
          CAST('2024-01-15' AS DATE) AS d,
          CAST('2024-01-15 10:20:30' AS TIMESTAMP) AS ts,
          CAST('2024-01-15 10:20:30 UTC' AS TIMESTAMP WITH TIME ZONE) AS tstz
      \`,
      dimensions: {
        id: {
          sql: 'id',
          type: 'number',
          primaryKey: true
        },
        d: {
          sql: 'd',
          type: 'time'
        },
        ts: {
          sql: 'ts',
          type: 'time'
        },
        tstz: {
          sql: 'tstz',
          type: 'time'
        }
      },
      measures: {
        count: {
          type: 'count'
        }
      }
    });
  `);

  const timezone = 'America/Los_Angeles';

  const buildSql = (QueryClass: any, column: string) => {
    const query = new QueryClass({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['events.count'],
      timeDimensions: [{
        dimension: `events.${column}`,
        granularity: 'day'
      }],
      timezone
    });

    return query.buildSqlAndParams()[0];
  };

  const promoted = (column: string) => `COALESCE("events".${column}, CAST(NULL AS TIMESTAMP))`;

  const dialects = [
    {
      name: 'TrinoQuery',
      QueryClass: TrinoQuery,
      convertTz: (column: string) => `CAST((${promoted(column)} AT TIME ZONE '${timezone}') AS TIMESTAMP)`
    },
    {
      name: 'PrestodbQuery',
      QueryClass: PrestodbQuery,
      convertTz: (column: string) => {
        const atTimezone = `${promoted(column)} AT TIME ZONE '${timezone}'`;
        return `CAST(date_add('minute', timezone_minute(${atTimezone}), ` +
          `date_add('hour', timezone_hour(${atTimezone}), ${promoted(column)})) AS TIMESTAMP)`;
      }
    }
  ] as const;

  for (const { name, QueryClass, convertTz } of dialects) {
    describe(name, () => {
      // `d` is the column that used to fail outright; `ts`/`tstz` guard the
      // promotion against changing what already worked.
      for (const column of ['d', 'ts', 'tstz']) {
        it(`promotes the ${column} column instead of feeding it to AT TIME ZONE`, async () => {
          await compiler.compile();

          const sql = buildSql(QueryClass, column);

          expect(sql).not.toMatch(new RegExp(`"events"\\.${column} AT TIME ZONE`));
          expect(sql).toContain(convertTz(column));
        });
      }
    });
  }
});
