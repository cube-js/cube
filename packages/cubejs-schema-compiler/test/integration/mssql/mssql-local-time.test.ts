import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { MssqlQuery } from '../../../src/adapter/MssqlQuery';
import { MSSqlDbRunner } from './MSSqlDbRunner';

describe('MSSQL Local Time Dimensions', () => {
  jest.setTimeout(200000);

  const dbRunner = new MSSqlDbRunner();

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(
    `
    cubes:
      - name: orders
        sql: >
          SELECT order_id,
                 created_at,
                 local_timestamp
          FROM (
            SELECT num + 1 AS order_id,
                   DATEADD(HOUR, num * 6, CAST('2024-01-01T00:00:00' AS DATETIME2)) AS created_at,
                   DATEADD(HOUR, num * 6, CAST('2024-01-01T23:00:00' AS DATETIME2)) AS local_timestamp
            FROM (VALUES (0),(1),(2),(3),(4)) AS t(num)
          ) AS subquery

        dimensions:
          - name: order_id
            sql: order_id
            type: number
            primary_key: true

          - name: createdAt
            sql: created_at
            type: time

          - name: localTimestamp
            sql: local_timestamp
            type: time
            local_time: true

        measures:
          - name: count
            type: count

          - name: minLocalTimestamp
            type: min
            sql: local_timestamp
    `,
    MssqlQuery
  );

  it('localTime dimension filters on local time value ignoring query timezone', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.count'],
      timeDimensions: [{
        dimension: 'orders.localTimestamp',
        dateRange: ['2024-01-01', '2024-01-01']
      }],
      timezone: 'Europe/Athens',  // +02:00 - would make 2024-01-01T23:00:00Z appear as 2024-01-02T01:00:00
      order: [{ id: 'orders.localTimestamp', desc: false }],
    },
    [
      { orders__count: 1 }
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  it('localTime dimension with day granularity returns local date', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.count'],
      timeDimensions: [
        {
          dimension: 'orders.createdAt',
          granularity: 'day'
        },
        {
          dimension: 'orders.localTimestamp',
          granularity: 'day'
        }
      ],
      filters: [{
        member: 'orders.createdAt',
        operator: 'equals',
        values: ['2024-01-01']
      }],
      timezone: 'Europe/Athens',  // +02:00
      order: [{ id: 'orders.createdAt', desc: false }],
    },
    [
      {
        orders__count: 1,
        orders__created_at_day: '2024-01-01T00:00:00.000Z',
        orders__local_timestamp_day: '2024-01-01T00:00:00.000Z',  // Local date, not Athens date
      }
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));

  it('localTime dimension comparison shows it stays in local time while regular dimension converts', async () => dbRunner.runQueryTest(
    {
      measures: ['orders.count', 'orders.minLocalTimestamp'],
      timeDimensions: [
        {
          dimension: 'orders.createdAt',
          granularity: 'day',
          dateRange: ['2024-01-01', '2024-01-01']
        }
      ],
      timezone: 'Europe/Athens',  // +02:00
      order: [{ id: 'orders.createdAt', desc: false }],
    },
    [
      {
        orders__count: 4,  // Only 4 records on 2024-01-01 in Athens timezone
        orders__created_at_day: '2024-01-01T00:00:00.000Z',
        orders__min_local_timestamp: '2024-01-01T23:00:00.000Z',  // Returns UTC representation of local time
      }
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));
});

