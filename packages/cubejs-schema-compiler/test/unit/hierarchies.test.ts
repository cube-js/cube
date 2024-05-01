import { prepareYamlCompiler } from './PrepareCompiler';

describe('Cube hierarchies', () => {
  it.only('includes cube hierarchies', async () => {
    const { compiler, metaTransformer } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: SELECT * FROM orders
    joins:
      - name: users
        sql: "{CUBE}.order_id = {orders}.id"
        relationship: many_to_one
    measures:
      - name: xxx
        sql: xxx
        type: number
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: number
        sql: number
        type: number

      - name: status
        sql: status
        type: string

      - name: city
        sql: city
        type: string
    hierarchies:
      - name: orders_hierarchy
        levels:
          - orders.status
          - users.state
          - city
      - name: Some other hierarchy
        levels:
          - users.state
          - users.city
  - name: users
    sql: SELECT * FROM users
    hierarchies:
      - name: Users hierarchy
        levels:
          - users.age
          - city
    dimensions:
      - name: age
        sql: age
        type: number
      - name: state
        sql: state
        type: string
      - name: city
        sql: city
        type: string

views:
  - name: orders_view
    cubes:
      - join_path: orders
        includes: "*"
      - join_path: users
        includes:
          - age
          - state
  - name: test_view
    hierarchies: []
    cubes:
      - join_path: orders
        includes: "*"
      - join_path: users
        includes:
          - age
          - state
  - name: empty_view
    hierarchies: []
    cubes:
      - join_path: users.orders
        includes:
          - number
      `);

    await compiler.compile();

    const ordersView = metaTransformer.cubes.find(it => it.config.name === 'orders_view');
    expect(ordersView.config.hierarchies).toEqual([
      {
        name: 'orders_hierarchy',
        levels: [
          'orders.status',
          'users.state',
          'orders.city',
        ]
      },
      {
        name: 'Some other hierarchy',
        levels: [
          'users.state'
        ]
      },
      {
        name: 'Users hierarchy',
        levels: [
          'users.age'
        ]
      }
    ]);

    const testView = metaTransformer.cubes.find(it => it.config.name === 'test_view');
    expect(testView.config.hierarchies.length).toBe(0);

    const emptyView = metaTransformer.cubes.find(it => it.config.name === 'empty_view');
    expect(emptyView.config.hierarchies.length).toBe(0);
  });
});
