import { prepareJsCompiler, prepareYamlCompiler } from './PrepareCompiler';

const CUBES = `
cubes:
  - name: orders
    sql_table: orders
    member_sets:
      - name: order_details
        members: [id, created_at, status]
      - name: with_city
        members: [order_details, users.city]
    joins:
      - name: users
        relationship: many_to_one
        sql: "{CUBE}.user_id = {users.id}"
      - name: line_items
        relationship: one_to_many
        sql: "{CUBE}.id = {line_items.order_id}"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: created_at
        sql: created_at
        type: time
      - name: status
        sql: status
        type: string
    measures:
      - name: count
        type: count
      - name: total_amount
        sql: amount
        type: sum
        drill_members: [id, status]
        drill_queries:
          - name: recent_orders
            title: Recent orders
            icon: clock
            members: [order_details]
            order: { created_at: desc }
            limit: 50
          - name: completed_only
            members: [order_details, total_amount]
            filters: [{ member: status, operator: equals, values: [completed] }]
          - name: by_city
            members: [users.city, total_amount]
            order: { users.city: asc }
          - name: by_product
            members: [line_items.products.name, total_amount]

  - name: users
    sql_table: users
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: city
        sql: city
        type: string
      - name: name
        sql: name
        type: string
      - name: email
        sql: email
        type: string
    measures:
      - name: count
        type: count

  - name: line_items
    sql_table: line_items
    joins:
      - name: products
        relationship: many_to_one
        sql: "{CUBE}.product_id = {products.id}"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: count
        type: count

  - name: products
    sql_table: products
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: name
        sql: name
        type: string

  - name: unrelated
    sql_table: unrelated
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
`;

const measureMeta = (compilers: any, cube: string, measure: string) => compilers.metaTransformer.cubes
  .find((c: any) => c.config.name === cube).config.measures
  .find((m: any) => m.name === `${cube}.${measure}`);

describe('Drill queries', () => {
  it('lists cube drill queries in meta, member sets expanded', async () => {
    const compilers = prepareYamlCompiler(CUBES);
    await compilers.compile();

    expect(measureMeta(compilers, 'orders', 'total_amount').drillQueries).toEqual([
      {
        name: 'recent_orders',
        title: 'Recent orders',
        icon: 'clock',
        members: ['orders.id', 'orders.created_at', 'orders.status'],
        membersGrouped: { dimensions: ['orders.id', 'orders.created_at', 'orders.status'], measures: [] },
        order: { 'orders.created_at': 'desc' },
        limit: 50,
      },
      {
        name: 'completed_only',
        title: 'Completed Only',
        members: ['orders.id', 'orders.created_at', 'orders.status', 'orders.total_amount'],
        membersGrouped: {
          dimensions: ['orders.id', 'orders.created_at', 'orders.status'],
          measures: ['orders.total_amount'],
        },
        filters: [{ member: 'orders.status', operator: 'equals', values: ['completed'] }],
      },
      {
        name: 'by_city',
        title: 'By City',
        members: ['users.city', 'orders.total_amount'],
        membersGrouped: { dimensions: ['users.city'], measures: ['orders.total_amount'] },
        order: { 'users.city': 'asc' },
      },
      {
        name: 'by_product',
        title: 'By Product',
        members: ['products.name', 'orders.total_amount'],
        membersGrouped: { dimensions: ['products.name'], measures: ['orders.total_amount'] },
      },
    ]);
  });

  it('leaves the meta of measures without drill queries unchanged', async () => {
    const compilers = prepareYamlCompiler(CUBES);
    await compilers.compile();

    expect(measureMeta(compilers, 'orders', 'count')).not.toHaveProperty('drillQueries');
    expect(measureMeta(compilers, 'orders', 'total_amount').drillMembers).toEqual(['orders.id', 'orders.status']);
  });

  it('compiles drill queries and member sets written in JS as references', async () => {
    const compilers = prepareJsCompiler(`
      cube('users', {
        sql_table: 'users',
        dimensions: {
          id: { sql: 'id', type: 'number', primaryKey: true },
          city: { sql: 'city', type: 'string' },
        },
      });

      cube('orders', {
        sql_table: 'orders',
        memberSets: [
          { name: 'details', members: [id, CUBE.status] },
        ],
        joins: {
          users: { relationship: 'many_to_one', sql: \`\${CUBE}.user_id = \${users.id}\` },
        },
        dimensions: {
          id: { sql: 'id', type: 'number', primaryKey: true },
          status: { sql: 'status', type: 'string' },
        },
        measures: {
          total: {
            sql: 'amount',
            type: 'sum',
            drillQueries: [{
              name: 'by_city',
              members: [details, users.city, total],
              order: [[total, 'desc']],
              limit: 10,
              filters: [{ member: status, operator: 'notEquals', values: ['cancelled'] }],
            }],
          },
        },
      });
    `);
    await compilers.compile();

    expect(measureMeta(compilers, 'orders', 'total').drillQueries).toEqual([{
      name: 'by_city',
      title: 'By City',
      members: ['orders.id', 'orders.status', 'users.city', 'orders.total'],
      membersGrouped: { dimensions: ['orders.id', 'orders.status', 'users.city'], measures: ['orders.total'] },
      order: { 'orders.total': 'desc' },
      limit: 10,
      filters: [{ member: 'orders.status', operator: 'notEquals', values: ['cancelled'] }],
    }]);
  });

  describe('views', () => {
    const VIEWS = `${CUBES}
views:
  - name: customer_details
    member_sets:
      - name: contact
        members: [name, email, city]
    cubes:
      - join_path: users
        includes: [name, email, city]

  - name: orders_overview
    cubes:
      - join_path: orders
        includes:
          - status
          - created_at
          - name: total_amount
            drill_queries:
              - name: recent_orders
                members: [created_at, status, total_amount]
                order: { created_at: desc }
                limit: 50
              - name: top_customers
                icon: users
                members: [customer_details.contact, total_amount]
                order: { total_amount: desc }
                limit: 10
`;

    it('inherits, overrides and defines drill queries', async () => {
      const compilers = prepareYamlCompiler(VIEWS);
      await compilers.compile();

      const { drillQueries } = measureMeta(compilers, 'orders_overview', 'total_amount');
      // `by_city` and `by_product` keep only the measure: the view includes neither city nor product name
      expect(drillQueries.map((q: any) => q.name))
        .toEqual(['recent_orders', 'completed_only', 'by_city', 'by_product', 'top_customers']);

      // Overridden by the view
      expect(drillQueries[0]).toEqual({
        name: 'recent_orders',
        title: 'Recent Orders',
        members: ['orders_overview.created_at', 'orders_overview.status', 'orders_overview.total_amount'],
        membersGrouped: {
          dimensions: ['orders_overview.created_at', 'orders_overview.status'],
          measures: ['orders_overview.total_amount'],
        },
        order: { 'orders_overview.created_at': 'desc' },
        limit: 50,
      });

      // Inherited without `id`, which the view doesn't include
      expect(drillQueries[1]).toEqual({
        name: 'completed_only',
        title: 'Completed Only',
        members: ['orders_overview.created_at', 'orders_overview.status', 'orders_overview.total_amount'],
        membersGrouped: {
          dimensions: ['orders_overview.created_at', 'orders_overview.status'],
          measures: ['orders_overview.total_amount'],
        },
        filters: [{ member: 'orders_overview.status', operator: 'equals', values: ['completed'] }],
      });

      // Defined in the view, naming a member set of another view
      expect(drillQueries[4]).toEqual({
        name: 'top_customers',
        title: 'Top Customers',
        icon: 'users',
        members: ['customer_details.name', 'customer_details.email', 'customer_details.city', 'orders_overview.total_amount'],
        membersGrouped: {
          dimensions: ['customer_details.name', 'customer_details.email', 'customer_details.city'],
          measures: ['orders_overview.total_amount'],
        },
        order: { 'orders_overview.total_amount': 'desc' },
        limit: 10,
      });
    });

    it('maps inherited members by exact join path', async () => {
      const compilers = prepareYamlCompiler(`${CUBES}
views:
  - name: by_path
    cubes:
      - join_path: orders
        includes: [total_amount, status]
      - join_path: orders.users
        prefix: true
        includes: [city]
      - join_path: orders.line_items.products
        prefix: true
        includes: [name]

  - name: other_path
    cubes:
      - join_path: orders
        includes: [total_amount, status]
      - join_path: orders.line_items.products
        includes: [name]
      - join_path: products
        prefix: true
        includes: [id]
`);
      await compilers.compile();

      const names = (view: string) => measureMeta(compilers, view, 'total_amount').drillQueries
        .map((q: any) => [q.name, q.members]);

      expect(names('by_path')).toEqual([
        ['completed_only', ['by_path.status', 'by_path.total_amount']],
        ['by_city', ['by_path.users_city', 'by_path.total_amount']],
        ['by_product', ['by_path.products_name', 'by_path.total_amount']],
      ]);
      // `users.city` isn't included at all, `recent_orders` loses its sort while
      // it has a limit, and `by_city` has only the measure left
      expect(names('other_path')).toEqual([
        ['completed_only', ['other_path.status', 'other_path.total_amount']],
        ['by_city', ['other_path.total_amount']],
        ['by_product', ['other_path.name', 'other_path.total_amount']],
      ]);
    });

    it('skips an inherited drill query whose filter member is missing', async () => {
      const compilers = prepareYamlCompiler(`${CUBES}
views:
  - name: no_status
    cubes:
      - join_path: orders
        includes: [total_amount, id, created_at]
`);
      await compilers.compile();

      expect(measureMeta(compilers, 'no_status', 'total_amount').drillQueries.map((q: any) => q.name))
        .toEqual(['recent_orders', 'by_city', 'by_product']);
    });

    it('fails on a drill query naming members that cannot be joined', async () => {
      const compilers = prepareYamlCompiler(`${CUBES}
views:
  - name: lonely
    cubes:
      - join_path: unrelated
        includes: [id]

  - name: orders_overview
    cubes:
      - join_path: orders
        includes:
          - name: total_amount
            drill_queries:
              - name: unrelated_rows
                members: [lonely.id, total_amount]
`);
      await expect(compilers.compile()).rejects.toThrow(/names members that can't be joined \('orders', 'unrelated'\): Can't find join path/);
    });
  });

  describe('aliased joins', () => {
    const ALIASED = `
cubes:
  - name: orders
    sql_table: orders
    member_sets:
      - name: parties
        members: [buyer.city, seller.city]
    joins:
      - name: users
        alias: buyer
        relationship: many_to_one
        sql: "{CUBE}.buyer_id = {buyer.id}"
      - name: users
        alias: seller
        relationship: many_to_one
        sql: "{CUBE}.seller_id = {seller.id}"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: total_amount
        sql: amount
        type: sum
        drill_queries:
          - name: by_parties
            members: [parties, total_amount]
          - name: by_id
            members: [id, total_amount]

  - name: users
    sql_table: users
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: city
        sql: city
        type: string
`;

    it('offers a drill query through an alias only in views, mapped through the alias', async () => {
      const compilers = prepareYamlCompiler(`${ALIASED}
views:
  - name: deals
    cubes:
      - join_path: orders
        includes: [total_amount]
      - join_path: orders.buyer
        prefix: true
        includes: [city]
      - join_path: orders.seller
        prefix: true
        includes: [city]
`);
      await compilers.compile();

      // The cube can't name the aliased instances in meta
      expect(measureMeta(compilers, 'orders', 'total_amount').drillQueries.map((q: any) => q.name))
        .toEqual(['by_id']);
      expect(measureMeta(compilers, 'deals', 'total_amount').drillQueries.map((q: any) => [q.name, q.members]))
        .toEqual([
          ['by_parties', ['deals.buyer_city', 'deals.seller_city', 'deals.total_amount']],
          ['by_id', ['deals.total_amount']],
        ]);
    });

    it('fails on a view drill query naming a cube member set through an alias', async () => {
      const compilers = prepareYamlCompiler(`${ALIASED}
views:
  - name: deals
    cubes:
      - join_path: orders
        includes:
          - name: total_amount
            drill_queries:
              - name: own_parties
                members: [orders.parties, total_amount]
`);
      await expect(compilers.compile()).rejects.toThrow(/reaches 'buyer.city' through a join alias/);
    });
  });

  it('fails on a view drill query naming a cube member set with join paths', async () => {
    const compilers = prepareYamlCompiler(`${CUBES}
views:
  - name: orders_overview
    cubes:
      - join_path: orders
        includes:
          - name: total_amount
            drill_queries:
              - name: with_city
                members: [orders.with_city, total_amount]
`);
    await expect(compilers.compile()).rejects.toThrow(/reaches 'users.city' through a join path of cube 'orders'/);
  });

  describe('validation', () => {
    const withDrillQueries = (drillQueries: string, extra = '') => `
cubes:
  - name: orders
    sql_table: orders
${extra}
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
      - name: created_at
        sql: created_at
        type: time
    measures:
      - name: total
        sql: amount
        type: sum
        drill_queries:
${drillQueries}

  - name: users
    sql_table: users
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

  - name: other
    sql_table: other
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

views:
  - name: users_view
    cubes:
      - join_path: users
        includes: [id]
`;

    it.each([
      ['a duplicate name', `
          - name: q
            members: [id]
          - name: q
            members: [status]`, /more than one drill query named 'q'/],
      ['no members', `
          - name: q`, /drillQueries\[0\]\.members\) is required/],
      ['an unknown key', `
          - name: q
            members: [id]
            sort: { id: asc }`, /drillQueries\[0\]\.sort = \[object Object\]\) is not allowed/],
      ['a bad limit', `
          - name: q
            members: [id]
            limit: 0`, /drillQueries\[0\]\.limit = 0\) must be a positive number/],
      ['a bad icon', `
          - name: q
            members: [id]
            icon: Bad Icon`, /Tabler icon name/],
      ['an unknown member', `
          - name: q
            members: [nope]`, /'nope' is not a member or member set of cube 'orders'/],
      ['a view', `
          - name: q
            members: [users_view.id]`, /names view 'users_view'/],
      ['an unreachable cube', `
          - name: q
            members: [other.id]`, /can't be reached by joins: cube 'orders' has no join 'other'/],
      ['an order member outside members', `
          - name: q
            members: [id]
            order: { status: asc }`, /orders by 'status', which is not in its members/],
      ['an operator that does not fit the type', `
          - name: q
            members: [id]
            filters: [{ member: created_at, operator: contains, values: [x] }]`, /operator 'contains' doesn't fit 'orders.created_at' of type time/],
      ['an unknown operator', `
          - name: q
            members: [id]
            filters: [{ member: status, operator: unknownOp, values: [x] }]`, /unknown filter operator 'unknownOp'/],
    ])('fails on %s', async (_title, drillQueries, error) => {
      const compilers = prepareYamlCompiler(withDrillQueries(drillQueries));
      await expect(compilers.compile()).rejects.toThrow(error);
    });

    it.each([
      ['a cycle', `
      - name: a
        members: [b]
      - name: b
        members: [a]`, /includes itself/],
      ['a member name', `
      - name: status
        members: [id]`, /has the name of a member of 'orders'/],
      ['an unknown member', `
      - name: a
        members: [nope]`, /Member set 'orders.a': 'nope' is not a member or member set/],
    ])('fails on a member set with %s', async (_title, sets, error) => {
      const compilers = prepareYamlCompiler(withDrillQueries(`
          - name: q
            members: [id]`, `    member_sets:${sets}`));
      await expect(compilers.compile()).rejects.toThrow(error);
    });
  });
});
