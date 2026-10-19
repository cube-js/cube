import { resolveQueryDataSource } from '../../src/adapter/QueryDataSource';
import { prepareJsCompiler } from './PrepareCompiler';

const model = `
cube('pg_orders', {
  sql: 'SELECT * FROM orders',
  measures: { count: { type: 'count' } },
  dimensions: {
    id: { sql: 'id', type: 'number', primaryKey: true },
    status: { sql: 'status', type: 'string' },
  },
});

cube('bq_customers', {
  sql: 'SELECT * FROM customers',
  dataSource: 'analytics',
  measures: { count: { type: 'count' } },
  dimensions: {
    id: { sql: 'id', type: 'number', primaryKey: true },
    name: { sql: 'name', type: 'string' },
  },
});

cube('bq_orders', {
  sql: 'SELECT * FROM orders',
  dataSource: 'analytics',
  measures: {
    count: { type: 'count' },
    named_count: { type: 'count', filters: [{ sql: \`\${bq_customers.name} IS NOT NULL\` }] },
  },
  dimensions: {
    id: { sql: 'id', type: 'number', primaryKey: true },
    customer_id: { sql: 'customer_id', type: 'number' },
    created_at: { sql: 'created_at', type: 'time' },
    customer_name: { sql: \`\${bq_customers.name}\`, type: 'string' },
  },
  segments: {
    named: { sql: \`\${CUBE}.customer_id IS NOT NULL\` },
  },
  joins: {
    bq_customers: { relationship: 'many_to_one', sql: \`\${CUBE}.customer_id = \${bq_customers.id}\` },
  },
});

cube('mixed', {
  sql: 'SELECT * FROM mixed',
  dataSource: 'analytics',
  dimensions: {
    id: { sql: 'id', type: 'number', primaryKey: true },
    // References a cube in another data source
    status: { sql: \`\${pg_orders.status}\`, type: 'string' },
  },
});

view('bq_view', {
  cubes: [
    { join_path: bq_orders, includes: ['count', 'created_at'] },
    { join_path: bq_orders.bq_customers, prefix: true, includes: ['name'] },
  ],
});
`;

describe('resolveQueryDataSource', () => {
  const compilers = prepareJsCompiler(model);

  beforeAll(() => compilers.compiler.compile());

  const resolve = (query: Record<string, unknown>) => resolveQueryDataSource(compilers, query);

  it('takes the data source of the cubes of the members', () => {
    expect(resolve({ measures: ['pg_orders.count'] })).toEqual('default');
    expect(resolve({ measures: ['bq_orders.count'], dimensions: ['bq_customers.name'] })).toEqual('analytics');
  });

  it('takes the data source of the cubes behind a view', () => {
    expect(resolve({ measures: ['bq_view.count'], dimensions: ['bq_view.bq_customers_name'] })).toEqual('analytics');
  });

  it('collects time dimensions, segments and nested filters', () => {
    expect(resolve({
      timeDimensions: [{ dimension: 'bq_view.created_at', granularity: 'day' }],
      segments: ['bq_orders.named'],
      filters: [{ and: [{ or: [{ member: 'bq_customers.name', operator: 'set' }] }] }],
    })).toEqual('analytics');
  });

  it('collects the cubes member SQL references', () => {
    expect(resolve({ dimensions: ['mixed.status'] })).toBeUndefined();
    expect(resolve({ dimensions: ['mixed.id'] })).toEqual('analytics');
    expect(resolve({ measures: ['bq_orders.named_count'] })).toEqual('analytics');
  });

  it('collects the cubes a member expression references', () => {
    expect(resolve({
      measures: [{
        cubeName: 'bq_view',
        name: 'expr',
        // Named after the cubes they reference, as the API gateway builds them
        expression: (bq_view: any) => `COUNT(${bq_view.bq_customers_name})`,
      }],
    })).toEqual('analytics');
  });

  it('resolves nothing across data sources', () => {
    expect(resolve({ measures: ['pg_orders.count'], dimensions: ['bq_customers.name'] })).toBeUndefined();
    expect(resolve({
      measures: ['bq_orders.count'],
      filters: [{ member: 'pg_orders.status', operator: 'set' }],
    })).toBeUndefined();
  });

  it('resolves nothing without members', () => {
    expect(resolve({})).toBeUndefined();
  });

  it('resolves nothing for an unknown member', () => {
    expect(resolve({ measures: ['bq_orders.unknown'] })).toBeUndefined();
    expect(resolve({ measures: ['unknown.count'] })).toBeUndefined();
  });
});
