import { SchemaFileRepository } from '@cubejs-backend/shared';
import { CompilerApi } from '../../src/core/CompilerApi';
import { DbTypeInternalFn } from '../../src/core/types';

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
      - name: email
        sql: email
        type: string
        mask: "'***'"
      - name: secret
        sql: secret
        type: string
    measures:
      - name: count
        type: count
    access_policy:
      - group: "*"
        member_level:
          includes:
            - id
            - city
            - count
        member_masking:
          includes:
            - email
        row_level:
          filters:
            - member: "{CUBE}.city"
              operator: equals
              values: ["Berlin"]

  - name: departments
    sql_table: departments
    joins:
      - name: users
        alias: head
        sql: "{CUBE}.head_id = {users}.id"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: name
        sql: name
        type: string
      - name: head_city
        sql: "{head.city}"
        type: string

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
`;

describe('CompilerApi row level security through join aliases', () => {
  let compilerApi: CompilerApi;

  const repository: SchemaFileRepository = {
    localPath: () => '/mock/path',
    dataSchemaFiles: () => Promise.resolve([{ fileName: 'model.yml', content: model }]),
  };
  const dbType: DbTypeInternalFn = async () => 'postgres';
  const context: any = { securityContext: {}, requestId: 'test' };

  beforeEach(() => {
    compilerApi = new CompilerApi(repository, dbType, {
      logger: () => {}, // eslint-disable-line @typescript-eslint/no-empty-function
      contextToGroups: async () => [],
    });
  });

  afterEach(() => compilerApi.dispose());

  const applyRls = async (query: any) => {
    const normalized = { timezone: 'UTC', filters: [], ...query };
    return compilerApi.applyRowLevelSecurity(normalized, normalized, context);
  };

  test('constrains each instance of the cube by its own row filter', async () => {
    const { query, denied } = await applyRls({
      measures: ['orders.count'],
      dimensions: ['orders.customer.city', 'orders.manager.city'],
    });
    expect(denied).toBe(false);
    const rendered = JSON.stringify(query.filters);
    expect(rendered).toContain('"member":"orders.customer.city","operator":"equals","values":["Berlin"]');
    expect(rendered).toContain('"member":"orders.manager.city","operator":"equals","values":["Berlin"]');
    expect(rendered).not.toContain('"users.city"');
  });

  test('masks a member of an instance under its path', async () => {
    const { query, denied } = await applyRls({
      dimensions: ['orders.customer.email'],
    });
    expect(denied).toBe(false);
    expect((query as any).maskedMembers).toEqual([{ member: 'orders.customer.email', filter: undefined }]);
  });

  test('denies a member the policy hides, through an alias too', async () => {
    const { denied } = await applyRls({
      dimensions: ['orders.customer.secret'],
    });
    expect(denied).toBe(true);
  });

  test('applies nothing to an instance the query only passes through', async () => {
    const { query, denied } = await applyRls({
      measures: ['orders.count'],
      dimensions: ['orders.manager.departments.name'],
    });
    expect(denied).toBe(false);
    expect(query.filters).toEqual([]);
  });

  test('renders the instance constraints into the query SQL', async () => {
    const { query } = await applyRls({
      measures: ['orders.count'],
      dimensions: ['orders.customer.city', 'orders.manager.email'],
    });
    const { sql } = await compilerApi.getSql(query as any);
    const [text, params] = sql;
    expect(text).toContain('"orders__customer".city = $');
    expect(text).toContain('"orders__manager".city = $');
    expect(text).toContain("'***'");
    expect(params).toEqual(['Berlin', 'Berlin']);
  });

  test('constrains the instance a view member reaches through an alias', async () => {
    const { query, denied } = await applyRls({
      measures: ['orders_view.count'],
      dimensions: ['orders_view.customer_city'],
    });
    expect(denied).toBe(false);
    const rendered = JSON.stringify(query.filters);
    expect(rendered).toContain('"member":"orders.customer.city","operator":"equals","values":["Berlin"]');
    expect(rendered).not.toContain('"users.city"');

    const { sql, dataSource } = await compilerApi.getSql(query as any);
    expect(dataSource).toBe('default');
    expect(sql[0]).toContain('"orders__customer".city = $');
  });

  test('constrains an instance reached through an alias inside another instance', async () => {
    const { query } = await applyRls({
      measures: ['orders.count'],
      dimensions: ['orders.manager.departments.head_city'],
    });
    const rendered = JSON.stringify(query.filters);
    expect(rendered).toContain('"member":"orders.manager.departments.head.city","operator":"equals","values":["Berlin"]');
    expect(rendered).not.toContain('"users.city"');

    const { sql } = await compilerApi.getSql(query as any);
    expect(sql[0]).toContain('"orders__manager__departments__head".city = $');
  });
});
