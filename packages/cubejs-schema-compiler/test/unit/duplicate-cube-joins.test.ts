import { PostgresQuery } from '../../src';
import { prepareJsCompiler, prepareYamlCompiler } from './PrepareCompiler';

const usersCube = `
  - name: users
    sql_table: users_tbl
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: name
        sql: name
        type: string
`;

const managersCube = `
  - name: managers
    sql_table: managers_tbl
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: name
        sql: name
        type: string
`;

const ordersMembers = `
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
`;

async function compileError(schema: string): Promise<string> {
  const { compiler } = prepareYamlCompiler(schema);
  try {
    await compiler.compile();
  } catch (e: any) {
    return e.message;
  }
  return '';
}

describe('Multiple joins to the same cube', () => {
  it('rejects two joins to the same cube', async () => {
    const message = await compileError(`
cubes:
  - name: orders
    sql_table: orders_tbl
    joins:
      - name: users
        sql: "{CUBE}.user_id = {users}.id"
        relationship: many_to_one
      - name: users
        sql: "{CUBE}.manager_id = {users}.id"
        relationship: many_to_one
${ordersMembers}
${usersCube}
`);

    expect(message).toContain('Cube \'orders\' declares 2 joins to \'users\' (joins[0], joins[1])');
  });

  it('reports every conflicting declaration', async () => {
    const message = await compileError(`
cubes:
  - name: orders
    sql_table: orders_tbl
    joins:
      - name: users
        sql: "{CUBE}.user_id = {users}.id"
        relationship: many_to_one
      - name: managers
        sql: "{CUBE}.manager_id = {managers}.id"
        relationship: many_to_one
      - name: users
        sql: "{CUBE}.approver_id = {users}.id"
        relationship: many_to_one
      - name: users
        sql: "{CUBE}.reporter_id = {users}.id"
        relationship: many_to_one
${ordersMembers}
${usersCube}
${managersCube}
`);

    expect(message).toContain('Cube \'orders\' declares 3 joins to \'users\' (joins[0], joins[2], joins[3])');
  });

  it('reports every cube that declares duplicates', async () => {
    const message = await compileError(`
cubes:
  - name: orders
    sql_table: orders_tbl
    joins:
      - name: users
        sql: "{CUBE}.user_id = {users}.id"
        relationship: many_to_one
      - name: users
        sql: "{CUBE}.manager_id = {users}.id"
        relationship: many_to_one
${ordersMembers}
  - name: tickets
    sql_table: tickets_tbl
    joins:
      - name: managers
        sql: "{CUBE}.owner_id = {managers}.id"
        relationship: many_to_one
      - name: managers
        sql: "{CUBE}.assignee_id = {managers}.id"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
${usersCube}
${managersCube}
`);

    expect(message).toContain('Cube \'orders\' declares 2 joins to \'users\'');
    expect(message).toContain('Cube \'tickets\' declares 2 joins to \'managers\'');
  });

  it('allows joins to different cubes', async () => {
    const compilers = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders_tbl
    joins:
      - name: users
        sql: "{CUBE}.user_id = {users}.id"
        relationship: many_to_one
      - name: managers
        sql: "{CUBE}.manager_id = {managers}.id"
        relationship: many_to_one
${ordersMembers}
${usersCube}
${managersCube}
`);

    await compilers.compiler.compile();

    const sql = new PostgresQuery(compilers, {
      measures: ['orders.count'],
      dimensions: ['users.name', 'managers.name'],
      timezone: 'UTC',
    }).buildSqlAndParams()[0];

    expect(sql).toContain('"orders".user_id = "users".id');
    expect(sql).toContain('"orders".manager_id = "managers".id');
  });

  it('allows the same pair of cubes joined from both sides', async () => {
    const compilers = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders_tbl
    joins:
      - name: users
        sql: "{CUBE}.user_id = {users}.id"
        relationship: many_to_one
${ordersMembers}
  - name: users
    sql_table: users_tbl
    joins:
      - name: orders
        sql: "{CUBE}.id = {orders}.user_id"
        relationship: one_to_many
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: name
        sql: name
        type: string
`);

    await compilers.compiler.compile();

    const sql = new PostgresQuery(compilers, {
      measures: ['users.count'],
      dimensions: ['orders.id'],
      timezone: 'UTC',
    }).buildSqlAndParams()[0];

    expect(sql).toContain('orders_tbl');
    expect(sql).toContain('users_tbl');
  });

  it('allows a transitive join path reaching a cube through another one', async () => {
    const compilers = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders_tbl
    joins:
      - name: users
        sql: "{CUBE}.user_id = {users}.id"
        relationship: many_to_one
${ordersMembers}
  - name: users
    sql_table: users_tbl
    joins:
      - name: managers
        sql: "{CUBE}.manager_id = {managers}.id"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: name
        sql: name
        type: string
${managersCube}
`);

    await compilers.compiler.compile();

    const sql = new PostgresQuery(compilers, {
      measures: ['orders.count'],
      dimensions: ['managers.name'],
      timezone: 'UTC',
    }).buildSqlAndParams()[0];

    expect(sql).toContain('"orders".user_id = "users".id');
    expect(sql).toContain('"users".manager_id = "managers".id');
  });

  it('lets a child cube override a join inherited through extends', async () => {
    const compilers = prepareYamlCompiler(`
cubes:
  - name: orders_base
    sql_table: orders_tbl
    joins:
      - name: users
        sql: "{CUBE}.user_id = {users}.id"
        relationship: many_to_one
${ordersMembers}
  - name: orders
    extends: orders_base
    joins:
      - name: users
        sql: "{CUBE}.manager_id = {users}.id"
        relationship: many_to_one
${usersCube}
`);

    await compilers.compiler.compile();

    const sql = new PostgresQuery(compilers, {
      measures: ['orders.count'],
      dimensions: ['users.name'],
      timezone: 'UTC',
    }).buildSqlAndParams()[0];

    expect(sql).toContain('"orders".manager_id = "users".id');
    expect(sql).not.toContain('"orders".user_id = "users".id');
  });

  // With errors omitted the model compiles anyway, so none of the conflicting
  // declarations may reach the join graph: the cube keeps working on its own,
  // only the ambiguous join is gone.
  it('drops the conflicting joins instead of picking one when compile errors are omitted', async () => {
    const compilers = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders_tbl
    joins:
      - name: users
        sql: "{CUBE}.user_id = {users}.id"
        relationship: many_to_one
      - name: users
        sql: "{CUBE}.manager_id = {users}.id"
        relationship: many_to_one
${ordersMembers}
${usersCube}
`, { omitErrors: true });

    await compilers.compiler.compile();

    const ownSql = new PostgresQuery(compilers, {
      measures: ['orders.count'],
      timezone: 'UTC',
    }).buildSqlAndParams()[0];
    expect(ownSql).toContain('orders_tbl');

    expect(() => new PostgresQuery(compilers, {
      measures: ['orders.count'],
      dimensions: ['users.name'],
      timezone: 'UTC',
    }).buildSqlAndParams()).toThrow(/Can't find join path to join/);
  });

  // Invalidating the cube would make every other cube joining to it report
  // `Cube orders doesn't exist`, which is both false and unrelated to the defect.
  it('does not make cubes joining to it report that it does not exist', async () => {
    const message = await compileError(`
cubes:
  - name: orders
    sql_table: orders_tbl
    joins:
      - name: users
        sql: "{CUBE}.user_id = {users}.id"
        relationship: many_to_one
      - name: users
        sql: "{CUBE}.manager_id = {users}.id"
        relationship: many_to_one
${ordersMembers}
  - name: users
    sql_table: users_tbl
    joins:
      - name: orders
        sql: "{CUBE}.id = {orders}.user_id"
        relationship: one_to_many
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: name
        sql: name
        type: string
`);

    expect(message).toContain('Cube \'orders\' declares 2 joins to \'users\'');
    expect(message).not.toContain('doesn\'t exist');
  });

  it('blames the cube that declares the duplicates, not the one inheriting them', async () => {
    const message = await compileError(`
cubes:
  - name: orders_base
    sql_table: orders_tbl
    joins:
      - name: users
        sql: "{CUBE}.user_id = {users}.id"
        relationship: many_to_one
      - name: users
        sql: "{CUBE}.manager_id = {users}.id"
        relationship: many_to_one
${ordersMembers}
  - name: orders
    extends: orders_base
${usersCube}
`);

    expect(message).toContain('Cube \'orders_base\' declares 2 joins to \'users\'');
    expect(message).not.toContain('Cube \'orders\' declares');
  });
});

describe('Join aliases', () => {
  const aliasedOrders = `
cubes:
  - name: orders
    sql_table: orders_tbl
    joins:
      - name: users
        alias: customer
        sql: "{CUBE}.customer_id = {users}.id"
        relationship: many_to_one
      - name: users
        alias: manager
        sql: "{CUBE}.manager_id = {users}.id"
        relationship: many_to_one
      - name: managers
        sql: "{CUBE}.approver_id = {managers}.id"
        relationship: many_to_one
${ordersMembers}
${usersCube}
${managersCube}
`;

  it('allows several joins to the same cube when each is aliased', async () => {
    const compilers = prepareYamlCompiler(aliasedOrders);
    await compilers.compiler.compile();

    const { joins } = compilers.cubeEvaluator.cubeFromPath('orders');
    expect(joins.map(j => [j.name, j.alias])).toEqual([
      ['users', 'customer'],
      ['users', 'manager'],
      ['managers', undefined],
    ]);
  });

  it('keeps unaliased joins of the cube working', async () => {
    const compilers = prepareYamlCompiler(aliasedOrders);
    await compilers.compiler.compile();

    const sql = new PostgresQuery(compilers, {
      measures: ['orders.count'],
      dimensions: ['managers.name'],
      timezone: 'UTC',
    }).buildSqlAndParams()[0];

    expect(sql).toContain('"orders".approver_id = "managers".id');
    expect(sql).not.toContain('users_tbl');
  });

  it('does not reach an aliased-only cube by its own name', async () => {
    const compilers = prepareYamlCompiler(aliasedOrders);
    await compilers.compiler.compile();

    expect(() => new PostgresQuery(compilers, {
      measures: ['orders.count'],
      dimensions: ['users.name'],
      timezone: 'UTC',
    }).buildSqlAndParams()).toThrow(/Can't find join path to join/);
  });

  it('rejects an aliased and an unaliased join to the same cube', async () => {
    const message = await compileError(`
cubes:
  - name: orders
    sql_table: orders_tbl
    joins:
      - name: users
        alias: customer
        sql: "{CUBE}.customer_id = {users}.id"
        relationship: many_to_one
      - name: users
        sql: "{CUBE}.manager_id = {users}.id"
        relationship: many_to_one
${ordersMembers}
${usersCube}
`);

    expect(message).toContain('Cube \'orders\' declares 2 joins to \'users\' (joins[0], joins[1])');
    expect(message).toContain('must each declare a distinct alias');
  });

  it('rejects a duplicate alias', async () => {
    const message = await compileError(`
cubes:
  - name: orders
    sql_table: orders_tbl
    joins:
      - name: users
        alias: person
        sql: "{CUBE}.customer_id = {users}.id"
        relationship: many_to_one
      - name: managers
        alias: person
        sql: "{CUBE}.manager_id = {managers}.id"
        relationship: many_to_one
${ordersMembers}
${usersCube}
${managersCube}
`);

    expect(message).toContain('Cube \'orders\' declares 2 joins named \'person\' (joins[0], joins[1])');
  });

  it('rejects an alias clashing with an unaliased join', async () => {
    const message = await compileError(`
cubes:
  - name: orders
    sql_table: orders_tbl
    joins:
      - name: users
        alias: managers
        sql: "{CUBE}.customer_id = {users}.id"
        relationship: many_to_one
      - name: managers
        sql: "{CUBE}.manager_id = {managers}.id"
        relationship: many_to_one
${ordersMembers}
${usersCube}
${managersCube}
`);

    expect(message).toContain('Cube \'orders\' declares 2 joins named \'managers\'');
  });

  it('rejects an alias equal to the joined cube name', async () => {
    const message = await compileError(`
cubes:
  - name: orders
    sql_table: orders_tbl
    joins:
      - name: users
        alias: users
        sql: "{CUBE}.customer_id = {users}.id"
        relationship: many_to_one
${ordersMembers}
${usersCube}
`);

    expect(message).toContain('Cube \'orders\' declares a join to \'users\' (joins[0]) with the alias \'users\', which is the name of the joined cube');
  });

  // Self-joins are the case the alias exists for
  it('allows an aliased self-join', async () => {
    const compilers = prepareYamlCompiler(`
cubes:
  - name: users
    sql_table: users_tbl
    joins:
      - name: users
        alias: supervisor
        sql: "{CUBE}.manager_id = {users}.id"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
`);

    await compilers.compiler.compile();
  });

  it('rejects an alias that is not an identifier', async () => {
    const message = await compileError(`
cubes:
  - name: orders
    sql_table: orders_tbl
    joins:
      - name: users
        alias: "not an identifier"
        sql: "{CUBE}.customer_id = {users}.id"
        relationship: many_to_one
${ordersMembers}
${usersCube}
`);

    expect(message).toContain('alias');
  });

  // The map form is turned into the array form at transpile time, and it can
  // hold a single join per cube anyway
  it('accepts an alias on the map form of joins', async () => {
    const compilers = prepareJsCompiler(`
      cube('orders', {
        sql_table: 'orders_tbl',
        joins: {
          users: {
            alias: 'customer',
            sql: \`\${CUBE}.customer_id = \${users}.id\`,
            relationship: 'many_to_one',
          },
        },
        dimensions: {
          id: { sql: 'id', type: 'number', primary_key: true },
        },
      });

      cube('users', {
        sql_table: 'users_tbl',
        dimensions: {
          id: { sql: 'id', type: 'number', primary_key: true },
        },
      });
    `);

    await compilers.compiler.compile();

    const { joins } = compilers.cubeEvaluator.cubeFromPath('orders');
    expect(joins.map(j => [j.name, j.alias])).toEqual([['users', 'customer']]);
  });

  it('rejects a child cube redeclaring an inherited alias', async () => {
    const compilers = prepareYamlCompiler(`
cubes:
  - name: orders_base
    sql_table: orders_tbl
    joins:
      - name: users
        alias: customer
        sql: "{CUBE}.customer_id = {users}.id"
        relationship: many_to_one
      - name: users
        alias: manager
        sql: "{CUBE}.manager_id = {users}.id"
        relationship: many_to_one
${ordersMembers}
  - name: orders
    extends: orders_base
    joins:
      - name: users
        alias: customer
        sql: "{CUBE}.buyer_id = {users}.id"
        relationship: many_to_one
${usersCube}
`);

    await expect(compilers.compiler.compile()).rejects.toThrow(
      /Cube 'orders' declares a join named 'customer' \(joins\[0\]\), which redeclares an aliased join of the cube it extends/
    );
  });
});
