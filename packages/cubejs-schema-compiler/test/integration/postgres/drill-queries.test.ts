import { PostgresQuery } from '../../../src/adapter';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Drill queries', () => {
  jest.setTimeout(200000);

  const compilers = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: >
      SELECT * FROM (VALUES
        (1, 1, 'completed', 100),
        (2, 1, 'cancelled', 40),
        (3, 2, 'completed', 70),
        (4, 3, 'completed', 30),
        (5, 3, 'completed', 25)
      ) AS t(id, user_id, status, amount)
    joins:
      - name: users
        relationship: many_to_one
        sql: "{CUBE}.user_id = {users.id}"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string
    measures:
      - name: total_amount
        sql: amount
        type: sum
        drill_queries:
          - name: by_city
            members: [users.city, total_amount]
            order: { total_amount: desc }
            limit: 2
            filters: [{ member: status, operator: equals, values: [completed] }]

  - name: users
    sql: >
      SELECT * FROM (VALUES
        (1, 'Alice', 'Berlin'),
        (2, 'Bob', 'Paris'),
        (3, 'Carol', 'Rome')
      ) AS t(id, name, city)
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: name
        sql: name
        type: string
      - name: city
        sql: city
        type: string

views:
  - name: customer_details
    member_sets:
      - name: contact
        members: [name]
    cubes:
      - join_path: users
        includes: [name]

  - name: orders_overview
    cubes:
      - join_path: orders
        includes:
          - status
          - name: total_amount
            drill_queries:
              - name: top_customers
                members: [customer_details.contact, users.city, total_amount]
                order: { total_amount: desc }
                limit: 2
`);

  // The query `ResultSet.drillDown()` builds for a drill query, without a clicked cell
  async function runDrillQuery(measure: string, drillQueryName: string) {
    await compilers.compile();

    const [cubeName] = measure.split('.');
    const measureConfig = compilers.metaTransformer.cubes
      .find((c) => c.config.name === cubeName)!.config.measures
      .find((m) => m.name === measure)!;
    const drillQuery = measureConfig.drillQueries!.find((q) => q.name === drillQueryName)!;

    const query = compilers.createQuery(PostgresQuery, {
      ...drillQuery.membersGrouped,
      order: Object.entries(drillQuery.order || {}).map(([id, direction]) => ({ id, desc: direction === 'desc' })),
      // The API's `limit` is `rowLimit` once the query reaches the compiler
      rowLimit: drillQuery.limit,
      filters: [...(drillQuery.filters || []), { member: measure, operator: 'measureFilter' }],
      timezone: 'UTC',
      preAggregationsSchema: '',
    });

    return dbRunner.testQuery(query.buildSqlAndParams());
  }

  it('runs a cube drill query through a join', async () => {
    const res = await runDrillQuery('orders.total_amount', 'by_city');

    expect(res).toEqual([
      { users__city: 'Berlin', orders__total_amount: '100' },
      { users__city: 'Paris', orders__total_amount: '70' },
    ]);
  });

  it('runs a view drill query naming another view and a cube', async () => {
    const res = await runDrillQuery('orders_overview.total_amount', 'top_customers');

    expect(res).toEqual([
      { customer_details__name: 'Alice', users__city: 'Berlin', orders_overview__total_amount: '140' },
      { customer_details__name: 'Bob', users__city: 'Paris', orders_overview__total_amount: '70' },
    ]);
  });
});
