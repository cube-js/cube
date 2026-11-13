import { SchemaFileRepository } from '@cubejs-backend/shared';
import { CompilerApi } from '../../src/core/CompilerApi';
import { DbTypeInternalFn } from '../../src/core/types';

const model = `
cubes:
  - name: orders
    sql_table: orders
    joins:
      - name: customers
        sql: "{CUBE}.customer_id = {customers.id}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string
    measures:
      - name: count
        type: count
    access_policy:
      - group: "*"
        member_level:
          includes:
            - id
            - count

  - name: customers
    sql_table: customers
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: name
        sql: name
        type: string
      - name: email
        sql: email
        type: string
      - name: city
        sql: city
        type: string
    access_policy:
      - group: "*"
        member_level:
          includes:
            - name
        row_level:
          filters:
            - member: "{CUBE}.city"
              operator: equals
              values: ["Berlin"]

  - name: accounts
    sql_table: accounts
    joins:
      - name: customers
        sql: "{CUBE}.customer_id = {customers.id}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: count
        type: count

views:
  - name: orders_view
    cubes:
      - join_path: orders
        includes:
          - count
          - status
      - join_path: orders.customers
        prefix: true
        includes:
          - name
          - email
`;

describe('CompilerApi member level access through views', () => {
  const repository: SchemaFileRepository = {
    localPath: () => '/mock/path',
    dataSchemaFiles: () => Promise.resolve([{ fileName: 'model.yml', content: model }]),
  };
  const dbType: DbTypeInternalFn = async () => 'postgres';
  const context: any = { securityContext: {}, requestId: 'test' };
  const customersRowFilter = '"member":"customers.city","operator":"equals","values":["Berlin"]';

  let compilerApi: CompilerApi;

  const createCompilerApi = (sqlCache = false) => new CompilerApi(repository, dbType, {
    logger: () => {}, // eslint-disable-line @typescript-eslint/no-empty-function
    contextToGroups: async () => [],
    sqlCache,
  });

  beforeEach(() => {
    compilerApi = createCompilerApi();
  });

  afterEach(() => compilerApi.dispose());

  const applyRls = async (query: any) => {
    const normalized = { timezone: 'UTC', filters: [], ...query };
    return compilerApi.applyRowLevelSecurity(normalized, normalized, context);
  };

  test('exposes hidden members and join keys of a cube read only through a view', async () => {
    const { query, denied } = await applyRls({
      measures: ['orders_view.count'],
      dimensions: ['orders_view.status', 'orders_view.customers_email'],
    });
    expect(denied).toBe(false);
    expect(JSON.stringify(query.filters)).toContain(customersRowFilter);
  });

  test('denies a hidden member read directly', async () => {
    const { denied } = await applyRls({ dimensions: ['customers.email'] });
    expect(denied).toBe(true);
  });

  test('denies a hidden member read directly next to a view over its cube', async () => {
    const { denied } = await applyRls({
      measures: ['orders_view.count'],
      dimensions: ['customers.email'],
    });
    expect(denied).toBe(true);
  });

  test('denies a hidden member read directly in a filter next to a view over its cube', async () => {
    const { denied } = await applyRls({
      measures: ['orders_view.count'],
      filters: [{ member: 'customers.email', operator: 'equals', values: ['a@example.com'] }],
    });
    expect(denied).toBe(true);
  });

  test('denies a hidden member read both directly and through a view', async () => {
    const { denied } = await applyRls({
      dimensions: ['customers.email', 'orders_view.customers_email'],
    });
    expect(denied).toBe(true);
  });

  test('denies a hidden member of the other cube of a view', async () => {
    const { denied } = await applyRls({
      dimensions: ['orders.status', 'orders_view.customers_name'],
    });
    expect(denied).toBe(true);
  });

  test('allows a granted member read directly next to a hidden one read through a view', async () => {
    const { denied } = await applyRls({
      measures: ['orders.count'],
      dimensions: ['orders_view.status'],
    });
    expect(denied).toBe(false);
  });

  test('requires a grant on the join key of a cube read directly', async () => {
    const direct = await applyRls({
      measures: ['accounts.count'],
      dimensions: ['customers.name'],
    });
    expect(direct.denied).toBe(true);

    const mixed = await applyRls({
      dimensions: ['customers.name', 'orders_view.status'],
    });
    expect(mixed.denied).toBe(true);
  });

  test('exposes the join key of a cube read through a view next to another cube read directly', async () => {
    const { query, denied } = await applyRls({
      measures: ['orders.count'],
      dimensions: ['orders_view.customers_email'],
    });
    expect(denied).toBe(false);
    expect(JSON.stringify(query.filters)).toContain(customersRowFilter);
  });

  test('a cached view-only query does not expose members to a later mixed query', async () => {
    compilerApi.dispose();
    compilerApi = createCompilerApi(true);

    const viewOnly = await applyRls({ dimensions: ['orders_view.customers_email'] });
    expect(viewOnly.denied).toBe(false);

    const mixed = await applyRls({ dimensions: ['customers.email', 'orders_view.customers_email'] });
    expect(mixed.denied).toBe(true);

    const viewOnlyAgain = await applyRls({ dimensions: ['orders_view.customers_email'] });
    expect(viewOnlyAgain.denied).toBe(false);
  });
});
