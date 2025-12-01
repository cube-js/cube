import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { ClickHouseQuery } from '../../../src/adapter/ClickHouseQuery';
import { ClickHouseDbRunner } from './ClickHouseDbRunner';

describe('ClickHouse Local Time Dimensions', () => {
  jest.setTimeout(200000);

  const dbRunner = new ClickHouseDbRunner();

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(
    `
    cubes:
      - name: orders
        sql: >
          SELECT order_id,
                 created_at,
                 local_timestamp
          FROM (
            SELECT
              num + 1 AS order_id,
              toDateTime('2024-01-01 00:00:00', 'UTC') + INTERVAL (num * 6) HOUR AS created_at,
              toDateTime('2024-01-01 23:00:00', 'UTC') + INTERVAL (num * 6) HOUR AS local_timestamp
            FROM (
              SELECT number AS num
              FROM system.numbers
              LIMIT 5
            )
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
    ClickHouseQuery
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
      { orders__count: '1' }
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
        orders__count: '1',
        orders__created_at_day: '2024-01-01T00:00:00.000',
        orders__local_timestamp_day: '2024-01-01T00:00:00.000',  // Local date, not Athens date
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
        orders__count: '4',  // Only 4 records on 2024-01-01 in Athens timezone
        orders__created_at_day: '2024-01-01T00:00:00.000',
        orders__min_local_timestamp: '2024-01-01T23:00:00.000',  // Returns local time without timezone conversion
      }
    ],
    { joinGraph, cubeEvaluator, compiler }
  ));
});

