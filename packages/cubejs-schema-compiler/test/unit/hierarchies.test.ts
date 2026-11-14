import fs from 'fs';
import path from 'path';

import { prepareJsCompiler, prepareYamlCompiler } from './PrepareCompiler';

describe('Cube hierarchies', () => {
  it('base cases', async () => {
    const modelContent = fs.readFileSync(
      path.join(process.cwd(), '/test/unit/fixtures/hierarchies.yml'),
      'utf8'
    );
    const compilers = prepareYamlCompiler(modelContent);

    await compilers.compile();

    const ordersView = compilers.metaTransformer.cubes.find(
      (it) => it.config.name === 'orders_users_view'
    );

    expect(ordersView?.config.hierarchies.length).toBe(2);
    expect(ordersView?.config.hierarchies).toEqual([
      {
        name: 'orders_users_view.orders_hierarchy',
        title: 'Hello Hierarchy',
        public: true,
        aliasMember: 'orders.orders_hierarchy',
        levels: [
          'orders_users_view.status',
          'orders_users_view.number',
          'orders_users_view.user_city'
        ],
      },
      {
        name: 'orders_users_view.some_other_hierarchy',
        public: true,
        aliasMember: 'orders.some_other_hierarchy',
        title: 'Some other hierarchy',
        levels: ['orders_users_view.state', 'orders_users_view.user_city']
      }
    ]);

    const ordersIncludesExcludesView = compilers.metaTransformer.cubes.find(
      (it) => it.config.name === 'orders_includes_excludes_view'
    );
    expect(ordersIncludesExcludesView?.config.hierarchies.length).toBe(1);

    const emptyView = compilers.metaTransformer.cubes.find(
      (it) => it.config.name === 'empty_view'
    );
    expect(emptyView?.config.hierarchies.length).toBe(0);

    const allHierarchyView = compilers.metaTransformer.cubes.find(
      (it) => it.config.name === 'all_hierarchy_view'
    );
    expect(allHierarchyView?.config.hierarchies.length).toBe(3);

    const prefixedHierarchy = allHierarchyView?.config.hierarchies.find((it) => it.name === 'all_hierarchy_view.users_users_hierarchy');
    expect(prefixedHierarchy).toBeTruthy();
    expect(prefixedHierarchy?.aliasMember).toEqual('users.users_hierarchy');
    expect(prefixedHierarchy?.levels).toEqual(['all_hierarchy_view.users_age', 'all_hierarchy_view.users_city']);
  });

  it('auto include hierarchy members', async () => {
    const modelContent = fs.readFileSync(
      path.join(process.cwd(), '/test/unit/fixtures/hierarchies.yml'),
      'utf8'
    );
    const compilers = prepareYamlCompiler(modelContent);

    await compilers.compile();

    const view1 = compilers.metaTransformer.cubes.find(
      (it) => it.config.name === 'only_hierarchy_included_view'
    );

    expect(view1?.config.dimensions).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: 'only_hierarchy_included_view.status' }),
        expect.objectContaining({ name: 'only_hierarchy_included_view.number' }),
        expect.objectContaining({ name: 'only_hierarchy_included_view.city' })
      ])
    );

    // Members from the `users` cube are not included as `users` is not selected (not joined)
    const view2 = compilers.metaTransformer.cubes.find(
      (it) => it.config.name === 'auto_include_view'
    );
    expect(view2?.config.dimensions.length).toEqual(2);
    expect(view2?.config.dimensions).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: 'auto_include_view.status' }),
        expect.objectContaining({ name: 'auto_include_view.number' }),
      ])
    );
  });

  it(('hierarchy with measure'), async () => {
    const modelContent = fs.readFileSync(
      path.join(process.cwd(), '/test/unit/fixtures/hierarchy-with-measure.yml'),
      'utf8'
    );
    const compilers = prepareYamlCompiler(modelContent);

    await expect(compilers.compile()).rejects.toThrow('Only dimensions can be part of a hierarchy. Please remove the \'count\' member from the \'orders_hierarchy\' hierarchy.');
  });

  //     await expect(compiler.compile()).rejects.toThrow('with value "hello wrong name" fails to match the identifier pattern');
  //   });

  // it(('duplicated hierarchy'), async () => {
  //   const { compiler } = prepareYamlCompiler(`cubes:
  //     - name: orders
  //       sql_table: orders
  //       dimensions:
  //         - name: id
  //           sql: id
  //           type: number
  //           primary_key: true

  //         - name: id
  //           sql: id
  //           type: number
  //           primary_key: true

  //       hierarchies:
  //         - name: test_hierarchy
  //           levels:
  //             - id
  //         - name: test_hierarchy
  //           levels:
  //             - id
  //   `);

  //   await expect(compiler.compile()).rejects.toThrow('Duplicate hierarchy name \'test_hierarchy\' in cube \'orders\'');
  // });

  it(('hierarchies on extended cubes'), async () => {
    const modelContent = fs.readFileSync(
      path.join(process.cwd(), '/test/unit/fixtures/hierarchies-extended-cubes.yml'),
      'utf8'
    );
    const compilers = prepareYamlCompiler(modelContent);

    await compilers.compile();

    const testView = compilers.metaTransformer.cubes.find(
      (it) => it.config.name === 'test_view'
    );

    expect(testView?.config.hierarchies).toEqual([
      {
        aliasMember: 'orders.base_orders_hierarchy',
        name: 'test_view.base_orders_hierarchy',
        title: 'Hello Hierarchy',
        levels: ['test_view.status', 'test_view.number'],
        public: true
      },
      {
        aliasMember: 'orders.orders_hierarchy',
        name: 'test_view.orders_hierarchy',
        levels: ['test_view.state', 'test_view.city'],
        public: true
      }
    ]);
  });

  it('js model base cases', async () => {
    const modelContent = fs.readFileSync(
      path.join(process.cwd(), '/test/unit/fixtures/orders.js'),
      'utf8'
    );
    const compilers = prepareJsCompiler(modelContent);

    await compilers.compile();

    const ordersCube = compilers.metaTransformer.cubes.find(
      (it) => it.config.name === 'orders'
    );

    expect(ordersCube?.config.hierarchies).toEqual([
      {
        aliasMember: undefined,
        name: 'orders.hello',
        title: 'World',
        levels: ['orders.status'],
        public: true
      }
    ]);
  });
});
