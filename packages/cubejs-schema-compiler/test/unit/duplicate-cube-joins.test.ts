import { PostgresQuery } from '../../src';
import { prepareYamlCompiler } from './PrepareCompiler';

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

  // With errors omitted the cube compiles anyway, so the check has to also mark
  // it invalid, or the collapsed join graph would keep serving the wrong path.
  it('keeps the cube out of the join graph when compile errors are omitted', async () => {
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

    expect(() => new PostgresQuery(compilers, {
      measures: ['orders.count'],
      dimensions: ['users.name'],
      timezone: 'UTC',
    }).buildSqlAndParams()).toThrow(/orders/);
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
