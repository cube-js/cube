import { prepareYamlCompiler } from './PrepareCompiler';

describe('Cube hierarchies', () => {
  it('includes cube hierarchies', async () => {
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

    const orders = metaTransformer.cubes.find(it => it.config.name === 'orders');
    expect(orders.config.hierarchies).toEqual([
      {
        name: 'orders_hierarchy',
        levels: [
          'orders.status',
          'users.state',
          'orders.city'
        ]
      },
      {
        name: 'Some other hierarchy',
        levels: [
          'users.state',
          'users.city'
        ]
      }
    ]);

    const ordersView = metaTransformer.cubes.find(it => it.config.name === 'orders_view');
    expect(ordersView.config.hierarchies).toEqual([
      {
        name: 'orders_hierarchy',
        levels: [
          'orders_view.status',
          'orders_view.state',
          'orders_view.city',
        ]
      },
      {
        name: 'Some other hierarchy',
        levels: [
          'orders_view.state'
        ]
      },
      {
        name: 'Users hierarchy',
        levels: [
          'orders_view.age'
        ]
      }
    ]);

    const testView = metaTransformer.cubes.find(it => it.config.name === 'test_view');
    expect(testView.config.hierarchies.length).toBe(0);

    const emptyView = metaTransformer.cubes.find(it => it.config.name === 'empty_view');
    expect(emptyView.config.hierarchies.length).toBe(0);
  });

  it('hierarchies defined on a view only', async () => {
    const { compiler, metaTransformer } = prepareYamlCompiler(`
views:
  - name: orders_view
    cubes:
      - join_path: orders
        includes: "*"
    hierarchies:
    - name: hello
      levels:
        - orders.status
cubes:
  - name: orders
    sql: SELECT * FROM orders
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: status
        sql: status
        type: string

      - name: city
        sql: city
        type: string
      `);

    await compiler.compile();

    const ordersView = metaTransformer.cubes.find(it => it.config.name === 'orders_view');
    
    expect(ordersView.config.hierarchies).toEqual([
      {
        name: 'hello',
        levels: [
          'orders_view.status',
        ]
      },
    ]);
  });

  it('views with prefix and aliased members', async () => {
    const { compiler, metaTransformer } = prepareYamlCompiler(`
views:
  - name: orders_view
    cubes:
      - join_path: orders
        prefix: true
        includes: "*"
      - join_path: users
        prefix: false
        includes:
          - count
          - name: gender
            alias: hello_world
    hierarchies:
    - name: hello
      levels:
        - users.count
        - users.gender
        - orders.count
        - orders.status
cubes:
  - name: orders
    sql: SELECT * FROM orders
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: status
        sql: status
        type: string

  - name: users
    sql: SELECT * FROM users
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: gender
        sql: gender
        type: string

      - name: city
        sql: city
        type: string

      - name: status
        sql: status
        type: string
      `);

    await compiler.compile();

    const ordersView = metaTransformer.cubes.find(it => it.config.name === 'orders_view');
    
    expect(ordersView.config.hierarchies).toEqual([
      {
        name: 'hello',
        levels: [
          'orders_view.count',
          'orders_view.hello_world',
          'orders_view.orders_count',
          'orders_view.orders_status'
        ]
      },
    ]);
  });
});
