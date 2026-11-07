import { MssqlQuery } from '../../../src/adapter/MssqlQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './MSSqlDbRunner';

describe('MSSQL week granularity', () => {
  jest.setTimeout(200000);

  // Sat/Sun pairs share a week with the preceding Monday, including before the 1900-01-01 day 0.
  const compilers = prepareYamlCompiler(`
  cubes:
    - name: events
      sql: >
        SELECT id, CAST(created_at AS DATETIME2) AS created_at
        FROM (VALUES
          (0, '1899-12-30T10:00:00'),
          (5, '1899-12-31T10:00:00'),
          (1, '2024-01-06T10:00:00'),
          (2, '2024-01-07T23:30:00'),
          (3, '2024-01-08T00:00:00'),
          (4, '2024-01-14T12:00:00')
        ) AS t (id, created_at)

      dimensions:
        - name: id
          sql: id
          type: number
          primary_key: true

        - name: createdAt
          sql: created_at
          type: time

      measures:
        - name: count
          type: count
  `);

  it.each([1, 7])('buckets Sundays into the week starting on the preceding Monday (DATEFIRST %i)', async (dateFirst) => {
    await compilers.compile();
    const query = compilers.createQuery(MssqlQuery, {
      measures: ['events.count'],
      timeDimensions: [{
        dimension: 'events.createdAt',
        granularity: 'week',
        dateRange: ['1899-12-01', '2024-01-31'],
      }],
      order: [{ id: 'events.createdAt' }],
      timezone: 'UTC',
    });
    const [sql, params] = query.buildSqlAndParams();

    const res = await dbRunner.testQuery([`SET DATEFIRST ${dateFirst}; ${sql}`, params]);
    expect(res).toEqual([
      { events__count: '2', events__created_at_week: '1899-12-25T00:00:00.000Z' },
      { events__count: '2', events__created_at_week: '2024-01-01T00:00:00.000Z' },
      { events__count: '2', events__created_at_week: '2024-01-08T00:00:00.000Z' },
    ]);
  });
});
