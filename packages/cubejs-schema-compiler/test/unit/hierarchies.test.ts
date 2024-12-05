import fs from 'fs';
import path from 'path';

import { prepareYamlCompiler } from './PrepareCompiler';

describe('Cube hierarchies', () => {
  it('hierarchies defined on a view only', async () => {
    const modelContent = fs.readFileSync(
      path.join(process.cwd(), '/test/unit/fixtures/hierarchies2.yml'),
      'utf8'
    );
    const { compiler, metaTransformer } = prepareYamlCompiler(modelContent);

    await compiler.compile();

    const ordersView = metaTransformer.cubes.find(
      (it) => it.config.name === 'orders_users_view'
    );

    expect(ordersView.config.hierarchies.length).toBe(2);
    expect(ordersView.config.hierarchies).toEqual([
      {
        name: 'orders_users_view.orders_hierarchy',
        levels: [
          'orders_users_view.status',
          'orders_users_view.number'
        ],
      },
      {
        name: 'orders_users_view.some_other_hierarchy',
        title: 'Some other hierarchy',
        levels: ['orders_users_view.state']
      }
    ]);

    const ordersIncludesExcludesView = metaTransformer.cubes.find(
      (it) => it.config.name === 'orders_includes_excludes_view'
    );
    expect(ordersIncludesExcludesView.config.hierarchies.length).toBe(1);

    const emptyView = metaTransformer.cubes.find(
      (it) => it.config.name === 'empty_view'
    );
    expect(emptyView.config.hierarchies.length).toBe(0);

    const allHierarchyView = metaTransformer.cubes.find(
      (it) => it.config.name === 'all_hierarchy_view'
    );
    expect(allHierarchyView.config.hierarchies.length).toBe(3);
  });
  //     const { compiler, metaTransformer } = prepareYamlCompiler(`
  // views:
  //   - name: orders_view
  //     cubes:
  //       - join_path: orders
  //         prefix: true
  //         includes: "*"
  //       - join_path: users
  //         prefix: false
  //         includes:
  //           - count
  //           - name: gender
  //             alias: hello_world
  //     hierarchies:
  //     - name: hello
  //       levels:
  //         - users.count
  //         - users.gender
  //         - orders.count
  //         - orders.status
  // cubes:
  //   - name: orders
  //     sql: SELECT * FROM orders
  //     measures:
  //       - name: count
  //         type: count
  //     dimensions:
  //       - name: id
  //         sql: id
  //         type: number
  //         primary_key: true

  //       - name: status
  //         sql: status
  //         type: string

  //   - name: users
  //     sql: SELECT * FROM users
  //     measures:
  //       - name: count
  //         type: count
  //     dimensions:
  //       - name: id
  //         sql: id
  //         type: number
  //         primary_key: true

  //       - name: gender
  //         sql: gender
  //         type: string

  //       - name: city
  //         sql: city
  //         type: string

  //       - name: status
  //         sql: status
  //         type: string
  //       `);

  //     await compiler.compile();

  //     const ordersView = metaTransformer.cubes.find(it => it.config.name === 'orders_view');

  //     expect(ordersView.config.hierarchies).toEqual([
  //       {
  //         name: 'hello',
  //         levels: [
  //           'orders_view.count',
  //           'orders_view.hello_world',
  //           'orders_view.orders_count',
  //           'orders_view.orders_status'
  //         ]
  //       },
  //     ]);
  //   });
});
