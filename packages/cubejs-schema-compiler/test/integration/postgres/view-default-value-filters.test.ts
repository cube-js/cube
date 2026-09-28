import { getEnv } from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('View default value filters', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: >
      SELECT * FROM (VALUES
        (1, 'US', 100, 92),
        (2, 'CA', 50, 46),
        (3, 'DE', 80, 75),
        (4, 'FR', 30, 28),
        (5, 'GB', 60, 56)
      ) AS t(id, country, amount_usd, amount_eur)

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
        public: true

      - name: country
        sql: country
        type: string

      - name: currency
        type: switch
        values:
          - USD
          - EUR
          - GBP

    measures:
      - name: count
        type: count

      - name: total_amount_usd
        type: sum
        sql: amount_usd

      - name: total_amount_eur
        type: sum
        sql: amount_eur

views:
  - name: orders_view_real_unconditional
    cubes:
      - join_path: orders
        includes: "*"
    default_filters:
      - member: country
        operator: equals
        values:
          - US

  - name: orders_view_real_with_unless
    cubes:
      - join_path: orders
        includes: "*"
    default_filters:
      - member: country
        operator: equals
        values:
          - US
        unless:
          - country

  - name: orders_view_switch_unconditional
    cubes:
      - join_path: orders
        includes: "*"
    default_filters:
      - member: currency
        operator: equals
        values:
          - USD

  - name: orders_view_switch_with_unless
    cubes:
      - join_path: orders
        includes: "*"
    default_filters:
      - member: currency
        operator: equals
        values:
          - USD
        unless:
          - currency
  `);

  async function runQueryTest(q: any, expectedResult: any) {
    if (!getEnv('nativeSqlPlanner')) {
      return;
    }

    await compiler.compile();
    const query = new PostgresQuery(
      { joinGraph, cubeEvaluator, compiler },
      { ...q, timezone: 'UTC', preAggregationsSchema: '' }
    );

    const qp = query.buildSqlAndParams();
    const res = await dbRunner.testQuery(qp);

    expect(res).toEqual(expectedResult);
  }

  describe('Real dimension default filter', () => {
    it('applies when no `unless` and no relevant projection', async () => runQueryTest(
      {
        measures: ['orders_view_real_unconditional.count'],
      },
      [
        {
          orders_view_real_unconditional__count: '1',
        },
      ]
    ));

    it('keeps applying when `unless: [country]` and country is only in projection', async () => runQueryTest(
      {
        measures: ['orders_view_real_with_unless.count'],
        dimensions: ['orders_view_real_with_unless.country'],
        order: [{ id: 'orders_view_real_with_unless.country' }],
      },
      [
        {
          orders_view_real_with_unless__country: 'US',
          orders_view_real_with_unless__count: '1',
        },
      ]
    ));

    it('is released when `unless: [country]` and explicit filter on country', async () => runQueryTest(
      {
        measures: ['orders_view_real_with_unless.count'],
        filters: [
          {
            member: 'orders_view_real_with_unless.country',
            operator: 'equals',
            values: ['FR'],
          },
        ],
      },
      [
        {
          orders_view_real_with_unless__count: '1',
        },
      ]
    ));
  });

  describe('Virtual switch dimension default filter', () => {
    it('collapses the switch union when no `unless`', async () => runQueryTest(
      {
        measures: ['orders_view_switch_unconditional.count'],
        dimensions: ['orders_view_switch_unconditional.currency'],
        order: [{ id: 'orders_view_switch_unconditional.currency' }],
      },
      [
        {
          orders_view_switch_unconditional__currency: 'USD',
          orders_view_switch_unconditional__count: '5',
        },
      ]
    ));

    it('keeps the union pinned when `unless: [currency]` and currency is only in projection', async () => runQueryTest(
      {
        measures: ['orders_view_switch_with_unless.count'],
        dimensions: ['orders_view_switch_with_unless.currency'],
        order: [{ id: 'orders_view_switch_with_unless.currency' }],
      },
      [
        {
          orders_view_switch_with_unless__currency: 'USD',
          orders_view_switch_with_unless__count: '5',
        },
      ]
    ));

    it('is released when `unless: [currency]` and explicit filter on currency', async () => runQueryTest(
      {
        measures: ['orders_view_switch_with_unless.count'],
        dimensions: ['orders_view_switch_with_unless.currency'],
        filters: [
          {
            member: 'orders_view_switch_with_unless.currency',
            operator: 'equals',
            values: ['EUR'],
          },
        ],
        order: [{ id: 'orders_view_switch_with_unless.currency' }],
      },
      [
        {
          orders_view_switch_with_unless__currency: 'EUR',
          orders_view_switch_with_unless__count: '5',
        },
      ]
    ));
  });
});
