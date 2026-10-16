import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';
import { TrinoQuery } from '../../src/TrinoQuery';

const prepareCompiler = (content: string) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'main.js', content }
  ])
}, { adapter: 'postgres' });

// Trino rejects AT TIME ZONE over DATE, so `convertTz` promotes the field first. The promotion
// must not be a plain `CAST(... AS TIMESTAMP)`: that strips the zone off a
// `timestamp with time zone` column and shifts the converted value.
describe('TrinoQuery time dimensions', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(`
    cube('events', {
      sql: \`
        SELECT
          1 AS id,
          CAST('2024-01-15' AS DATE) AS d,
          CAST('2024-01-15 10:20:30' AS TIMESTAMP) AS ts,
          CAST('2024-01-15 10:20:30 UTC' AS TIMESTAMP WITH TIME ZONE) AS tstz
      \`,
      dimensions: {
        id: { sql: 'id', type: 'number', primaryKey: true },
        d: { sql: 'd', type: 'time' },
        ts: { sql: 'ts', type: 'time' },
        tstz: { sql: 'tstz', type: 'time' }
      },
      measures: {
        count: { type: 'count' }
      }
    });
  `);

  const timezone = 'America/Los_Angeles';

  it.each(['d', 'ts', 'tstz'])('converts the %s column with AT TIME ZONE over the promoted field', async (column) => {
    await compiler.compile();

    const query = new TrinoQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['events.count'],
      timeDimensions: [{ dimension: `events.${column}`, granularity: 'day' }],
      timezone
    });
    const [sql] = query.buildSqlAndParams();

    expect(sql).toContain(
      `CAST((COALESCE("events".${column}, CAST(NULL AS TIMESTAMP)) AT TIME ZONE '${timezone}') AS TIMESTAMP)`
    );
  });

  it('leaves the field untouched without a timezone', async () => {
    await compiler.compile();

    const query = new TrinoQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['events.count'],
      timezone: null
    });

    expect(query.convertTz('"events".d')).toBe('"events".d');
  });
});
