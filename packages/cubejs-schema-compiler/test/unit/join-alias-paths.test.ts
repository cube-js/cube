import { PostgresQuery } from '../../src';
import { prepareJsCompiler, prepareYamlCompiler } from './PrepareCompiler';

const model = `
cubes:
  - name: orders
    sql_table: orders
    joins:
      - name: users
        alias: customer
        sql: "{CUBE}.customer_id = {users}.id"
        relationship: many_to_one
      - name: users
        alias: manager
        sql: "{CUBE}.manager_id = {users}.id"
        relationship: many_to_one
      - name: products
        sql: "{CUBE}.product_id = {products}.id"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: count
        type: count

  - name: users
    sql_table: users
    joins:
      - name: departments
        sql: "{CUBE}.department_id = {departments}.id"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: city
        sql: city
        type: string
      - name: created_at
        sql: created_at
        type: time
    measures:
      - name: count
        type: count
    segments:
      - name: berliners
        sql: "{CUBE}.city = 'Berlin'"

  - name: departments
    sql_table: departments
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: name
        sql: name
        type: string

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
`;

describe('Member paths through join aliases', () => {
  async function evaluator() {
    const { compiler, cubeEvaluator } = prepareYamlCompiler(model);
    await compiler.compile();
    return cubeEvaluator;
  }

  it('resolves a member through an alias to its cube instance', async () => {
    const resolved = (await evaluator()).resolveMemberPath('orders.customer.city');
    expect(resolved).toEqual({
      fullPath: 'orders.customer.city',
      instancePath: 'orders.customer',
      targetCube: 'users',
      member: 'city',
      targetPath: 'users.city',
      granularity: undefined,
      aliased: true,
    });
  });

  it('keeps two aliases of one cube apart', async () => {
    const cubeEvaluator = await evaluator();
    expect(cubeEvaluator.resolveMemberPath('orders.manager.count')?.instancePath).toBe('orders.manager');
    expect(cubeEvaluator.resolveMemberPath('orders.customer.berliners')?.instancePath).toBe('orders.customer');
  });

  it('makes every join below an alias an instance of its own', async () => {
    expect((await evaluator()).resolveMemberPath('orders.manager.departments.name')).toMatchObject({
      instancePath: 'orders.manager.departments',
      targetCube: 'departments',
      targetPath: 'departments.name',
      aliased: true,
    });
  });

  it('splits the granularity off a time dimension of an instance', async () => {
    expect((await evaluator()).resolveMemberPath('orders.customer.created_at.month')).toMatchObject({
      fullPath: 'orders.customer.created_at',
      targetPath: 'users.created_at',
      granularity: 'month',
      aliased: true,
    });
  });

  it('reads the joined cube name inside an instance as the instance', async () => {
    expect((await evaluator()).resolveMemberPath('orders.customer.users.city')).toMatchObject({
      instancePath: 'orders.customer',
      targetPath: 'users.city',
    });
  });

  it('resolves a path through no alias to the last cube it names', async () => {
    const cubeEvaluator = await evaluator();
    expect(cubeEvaluator.resolveMemberPath('orders.products.name')).toMatchObject({
      instancePath: 'products',
      targetPath: 'products.name',
      aliased: false,
    });
    expect(cubeEvaluator.resolveMemberPath('users.city')).toMatchObject({
      instancePath: 'users',
      aliased: false,
    });
  });

  it('names no member for unknown, bare-alias or unjoined paths', async () => {
    const cubeEvaluator = await evaluator();
    expect(cubeEvaluator.resolveMemberPath('customer.city')).toBeNull();
    expect(cubeEvaluator.resolveMemberPath('orders.customer')).toBeNull();
    expect(cubeEvaluator.resolveMemberPath('orders.customer.products.name')).toBeNull();
    expect(cubeEvaluator.resolveMemberPath('orders.customer.missing')).toBeNull();
  });
});

const modelWithAliasReferences = `${model.replace(
  `    measures:
      - name: count
        type: count

  - name: users`,
  `      - name: customer_city
        sql: "{customer.city}"
        type: string
    measures:
      - name: count
        type: count

  - name: users`
)}
views:
  - name: orders_view
    cubes:
      - join_path: orders
        includes:
          - count
      - join_path: orders.customer
        prefix: true
        includes:
          - city
      - join_path: orders.manager.departments
        prefix: true
        includes:
          - name
      - join_path: orders.customer.departments
        alias: customer_departments
        prefix: true
        includes:
          - name
`;

describe('Join aliases in the data model', () => {
  async function compile(schema: string) {
    const compilers = prepareYamlCompiler(schema);
    await compilers.compiler.compile();
    return compilers;
  }

  it('resolves an alias in a member of the declaring cube', async () => {
    const compilers = await compile(modelWithAliasReferences);
    const dimension = compilers.cubeEvaluator.dimensionByPath('orders.customer_city');
    expect((dimension as { aliasMember?: string }).aliasMember).toBeUndefined();
    expect(compilers.cubeEvaluator.evaluateReferences('orders', dimension.sql as any, { collectJoinHints: true }))
      .toBe('orders.customer.city');

    const sql = new PostgresQuery(compilers, {
      measures: ['orders.count'],
      dimensions: ['orders.customer_city'],
      timezone: 'UTC',
    }).buildSqlAndParams()[0];
    expect(sql).toContain('AS "orders__customer" ON "orders".customer_id = "orders__customer".id');
    expect(sql).toContain('"orders__customer".city "orders__customer_city"');
  });

  it('includes members into a view through an alias', async () => {
    const compilers = await compile(modelWithAliasReferences);
    const view = compilers.cubeEvaluator.cubeFromPath('orders_view');
    expect(Object.keys(view.dimensions).sort()).toEqual([
      'customer_city',
      'customer_departments_name',
      'departments_name',
    ]);
    expect(compilers.cubeEvaluator.evaluateReferences(
      'orders_view',
      view.dimensions.customer_city.sql as any,
      { collectJoinHints: true }
    )).toBe('orders.customer.city');
    expect(view.includedMembers?.find(m => m.name === 'customer_city')?.memberPath).toBe('users.city');

    const sql = new PostgresQuery(compilers, {
      measures: ['orders_view.count'],
      dimensions: ['orders_view.customer_city', 'orders_view.departments_name', 'orders_view.customer_departments_name'],
      timezone: 'UTC',
    }).buildSqlAndParams()[0];
    expect(sql).toContain('AS "orders__customer" ON "orders".customer_id = "orders__customer".id');
    expect(sql).toContain('AS "orders__manager__departments" ON "orders__manager".department_id = "orders__manager__departments".id');
    expect(sql).toContain('AS "orders__customer__departments" ON "orders__customer".department_id = "orders__customer__departments".id');
  });

  it('drills a view measure into members of its own instance', async () => {
    const compilers = await compile(`${model.replace(
      '      - name: count\n        type: count\n    segments:',
      '      - name: count\n        type: count\n        drill_members:\n          - city\n    segments:'
    )}
views:
  - name: staff_view
    cubes:
      - join_path: orders.customer
        prefix: true
        includes:
          - city
      - join_path: orders.manager
        prefix: true
        includes:
          - city
          - count
`);
    const view = compilers.cubeEvaluator.cubeFromPath('staff_view');
    const { drillMembers } = view.measures.manager_count as { drillMembers?: () => string[] };
    expect(drillMembers?.()).toEqual(['staff_view.manager_city']);
  });

  it('accepts an alias in the JS model', async () => {
    const compilers = prepareJsCompiler(`
      cube('orders', {
        sql_table: 'orders',
        joins: [
          { name: 'users', alias: 'buyer', sql: \`\${CUBE}.buyer_id = \${buyer}.id\`, relationship: 'many_to_one' },
        ],
        dimensions: {
          id: { sql: 'id', type: 'number', primary_key: true },
          buyer_city: { sql: \`\${buyer.city}\`, type: 'string' },
        },
        measures: { count: { type: 'count' } },
      });

      cube('users', {
        sql_table: 'users',
        dimensions: {
          id: { sql: 'id', type: 'number', primary_key: true },
          city: { sql: 'city', type: 'string' },
        },
      });
    `);
    await compilers.compiler.compile();

    const sql = new PostgresQuery(compilers, {
      measures: ['orders.count'],
      dimensions: ['orders.buyer_city'],
      timezone: 'UTC',
    }).buildSqlAndParams()[0];
    expect(sql).toContain('AS "orders__buyer" ON "orders".buyer_id = "orders__buyer".id');
  });

  it('rejects an alias named like a member of the declaring cube', async () => {
    const { compiler } = prepareYamlCompiler(model.replace('alias: manager', 'alias: count'));
    await expect(compiler.compile()).rejects.toThrow(
      /alias 'count', which is the name of a member of 'orders'/
    );
  });
});

describe('Queries over join aliases', () => {
  it('serves everything the SQL request reads', async () => {
    const compilers = prepareYamlCompiler(modelWithAliasReferences);
    await compilers.compiler.compile();
    const query = new PostgresQuery(compilers, {
      measures: ['orders.count', 'orders.manager.count'],
      dimensions: ['orders.customer.city', 'orders.manager.departments.name'],
      timeDimensions: [{ dimension: 'orders.customer.created_at', granularity: 'month' }],
      segments: ['orders.customer.berliners'],
      filters: [{ member: 'orders.manager.city', operator: 'equals', values: ['Paris'] }],
      timezone: 'UTC',
    });

    const result = compilers.compiler.withQuery(query, () => ({
      external: query.externalPreAggregationQuery(),
      sql: query.buildSqlAndParams()[0],
      lambdaQueries: query.buildLambdaQuery(),
      timeDimensionAlias: query.timeDimensions[0]?.unescapedAliasName(),
      cacheKeyQueries: query.cacheKeyQueries(),
      preAggregations: query.preAggregations.preAggregationsDescription(),
      dataSource: query.dataSource,
      aliasNameToMember: query.aliasNameToMember,
      canUseTransformedQuery: query.preAggregations.canUseTransformedQuery(),
      memberNames: query.collectAllMemberNames(),
    }));

    expect(result.sql).toContain('"orders__customer".city "orders__customer__city"');
    expect(result.sql).toContain('"orders__manager__departments".name "orders__manager__departments__name"');
    expect(result.sql).toContain('"orders__customer__created_at_month"');
    expect(result.dataSource).toBe('default');
    expect(result.timeDimensionAlias).toBe('orders__customer__created_at_month');
    expect(result.aliasNameToMember).toEqual({
      orders__count: 'orders.count',
      orders__manager__count: 'orders.manager.count',
      orders__customer__city: 'orders.customer.city',
      orders__manager__departments__name: 'orders.manager.departments.name',
      orders__customer__created_at_month: 'orders.customer.created_at.month',
    });
    expect(result.memberNames).toEqual(expect.arrayContaining([
      'orders.count',
      'orders.manager.count',
      'orders.customer.city',
      'orders.manager.departments.name',
      'orders.customer.created_at',
      'orders.customer.berliners',
      'orders.manager.city',
    ]));
    expect(result.memberNames).not.toContain('users.city');
  });

  it('filters on a measure of an instance', async () => {
    const compilers = prepareYamlCompiler(modelWithAliasReferences);
    await compilers.compiler.compile();
    const query = new PostgresQuery(compilers, {
      measures: ['orders.count'],
      dimensions: ['orders.customer.city'],
      filters: [{ member: 'orders.manager.count', operator: 'gt', values: ['1'] }],
      timezone: 'UTC',
    });

    const { sql, memberNames } = compilers.compiler.withQuery(query, () => ({
      sql: query.buildSqlAndParams()[0],
      memberNames: query.collectAllMemberNames(),
    }));
    expect(query.measureFilters.map(f => f.measure)).toEqual(['users.count']);
    expect(sql).toMatch(/HAVING [^\n]*"orders__manager"\.id[^\n]*> \$1/);
    expect(memberNames).toContain('orders.manager.count');
    expect(memberNames).not.toContain('users.count');
  });
});

describe('Join alias limits', () => {
  it('requires the Tesseract planner', async () => {
    const compilers = prepareYamlCompiler(model);
    await compilers.compiler.compile();
    expect(() => new PostgresQuery(compilers, {
      measures: ['orders.count'],
      dimensions: ['orders.customer.city'],
      timezone: 'UTC',
      useNativeSqlPlanner: false,
    })).toThrow(/join aliases require the Tesseract SQL planner/);
  });

  it.each([
    ['contains a double underscore', 'alias: customer\n', 'alias: buyer__vip\n', /contains '__'/],
    ['reads like another after snake-casing', 'alias: manager\n', 'alias: Customer\n', /renders under the same name as the alias 'customer'/],
  ])('rejects an alias that %s', async (_, from, to, expected) => {
    const { compiler } = prepareYamlCompiler(model.replace(from, to));
    await expect(compiler.compile()).rejects.toThrow(expected);
  });

  it('rejects an alias rendering under the name of a cube', async () => {
    const { compiler } = prepareYamlCompiler(`${model}
  - name: orders__customer
    sql_table: orders_customer
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
`);
    await expect(compiler.compile()).rejects.toThrow(/renders under the same name as the cube 'orders__customer'/);
  });

  it('compares the names cubes render under, sql_alias included', async () => {
    const { compiler } = prepareYamlCompiler(`${model.replace('    sql_table: orders\n', '    sql_table: orders\n    sql_alias: o\n')}
  - name: legacy_customers
    sql_alias: o__customer
    sql_table: legacy_customers
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
`);
    await expect(compiler.compile()).rejects.toThrow(/renders under the same name as the cube 'legacy_customers'/);
  });

  it('rejects an alias named like another cube', async () => {
    const { compiler } = prepareYamlCompiler(model.replace('alias: manager\n', 'alias: departments\n'));
    await expect(compiler.compile()).rejects.toThrow(
      /alias 'departments', which is the name of the cube 'departments'/
    );
  });
});
