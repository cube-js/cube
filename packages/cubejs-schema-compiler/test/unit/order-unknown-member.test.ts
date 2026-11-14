import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { UserError } from '../../src/compiler/UserError';
import { prepareYamlCompiler } from './PrepareCompiler';

// Regression tests for https://github.com/cube-js/cube/issues/12753
//
// An `order` entry referencing a member that is not part of the query
// (not in dimensions, measures or timeDimensions with a granularity) used to
// be silently dropped: by BaseQuery.orderBy() in the legacy planner and by
// the native (Tesseract) planner alike. When every entry was dropped the
// generated SQL had no ORDER BY at all and the default order was not
// applied either, so limit/offset pagination could return overlapping
// pages and silently lose rows. BaseQuery.buildSqlAndParams now rejects
// such queries with a UserError naming the offending member, before the
// query is handed to either planner.
describe('Order by a member that is not part of the query', () => {
  const schema = `
cubes:
  - name: orders
    sql: >
      select 1 as id, 'new' as status, 100 as amount, timestamp '2026-01-01' as created_at
      union all select 2, 'new', 200, timestamp '2026-01-02'
      union all select 3, 'processed', 300, timestamp '2026-01-03'
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
        public: true
      - name: status
        sql: status
        type: string
      - name: amount
        sql: amount
        type: number
      - name: created_at
        sql: created_at
        type: time
    measures:
      - name: count
        type: count
      - name: total_amount
        sql: amount
        type: sum
`;

  const buildQuery = async (queryOptions: Record<string, unknown>) => {
    const compilers = prepareYamlCompiler(schema);
    await compilers.compile();
    return compilers.createQuery(PostgresQuery, {
      timezone: 'UTC',
      ...queryOptions,
    });
  };

  it('rejects the query when the only order member is not in the query', async () => {
    const query = await buildQuery({
      dimensions: ['orders.status', 'orders.amount'],
      order: [{ id: 'orders.id', desc: true }],
      rowLimit: 2,
      offset: 1,
    });

    expect(() => query.buildSqlAndParams()).toThrowError(UserError);
    expect(() => query.buildSqlAndParams()).toThrowError(/Order member 'orders\.id' is not part of the query/);
  });

  it('rejects the query on the native planner as well', async () => {
    const query = await buildQuery({
      dimensions: ['orders.status', 'orders.amount'],
      order: [{ id: 'orders.id', desc: true }],
      useNativeSqlPlanner: true,
    });

    expect(() => query.buildSqlAndParams()).toThrowError(UserError);
    expect(() => query.buildSqlAndParams()).toThrowError(/not part of the query/);
  });

  it('rejects the query when one of several order members is not in the query', async () => {
    const query = await buildQuery({
      dimensions: ['orders.status'],
      measures: ['orders.count'],
      order: [
        { id: 'orders.status', desc: false },
        { id: 'orders.id', desc: true },
      ],
    });

    expect(() => query.buildSqlAndParams()).toThrowError(/orders\.id/);
  });

  it('names every offending member when more than one is not in the query', async () => {
    const query = await buildQuery({
      dimensions: ['orders.status'],
      order: [
        { id: 'orders.id', desc: true },
        { id: 'orders.amount', desc: false },
      ],
    });

    expect(() => query.buildSqlAndParams()).toThrowError(/Order members 'orders\.id', 'orders\.amount' are not part of the query/);
  });

  it('orders by a dimension that is part of the query', async () => {
    const query = await buildQuery({
      dimensions: ['orders.id', 'orders.status', 'orders.amount'],
      order: [{ id: 'orders.id', desc: true }],
      rowLimit: 2,
      offset: 1,
    });

    const [sql] = query.buildSqlAndParams();
    expect(sql).toMatch(/ORDER BY\s+1\s+DESC/);
  });

  it('orders by a measure that is part of the query', async () => {
    const query = await buildQuery({
      dimensions: ['orders.status'],
      measures: ['orders.total_amount'],
      order: [{ id: 'orders.total_amount', desc: true }],
    });

    const [sql] = query.buildSqlAndParams();
    expect(sql).toMatch(/ORDER BY/);
  });

  it('orders by a time dimension with a granularity', async () => {
    const query = await buildQuery({
      timeDimensions: [{ dimension: 'orders.created_at', granularity: 'day' }],
      measures: ['orders.count'],
      order: [{ id: 'orders.created_at', desc: false }],
    });

    const [sql] = query.buildSqlAndParams();
    expect(sql).toMatch(/ORDER BY/);
  });

  it('explains the granularity problem for a time dimension used only as a date range', async () => {
    const query = await buildQuery({
      timeDimensions: [{ dimension: 'orders.created_at', dateRange: ['2026-01-01', '2026-01-31'] }],
      measures: ['orders.count'],
      order: [{ id: 'orders.created_at', desc: false }],
    });

    expect(() => query.buildSqlAndParams()).toThrowError(UserError);
    expect(() => query.buildSqlAndParams()).toThrowError(/'orders\.created_at' is a time dimension without a granularity/);
  });

  it('orders by a member that is part of the query on the native planner', async () => {
    const query = await buildQuery({
      dimensions: ['orders.id', 'orders.status'],
      order: [{ id: 'orders.id', desc: true }],
      useNativeSqlPlanner: true,
    });

    const [sql] = query.buildSqlAndParams();
    expect(sql).toMatch(/ORDER BY/);
  });

  it('still applies the default order when no order is provided', async () => {
    const query = await buildQuery({
      dimensions: ['orders.status'],
      measures: ['orders.count'],
    });

    const [sql] = query.buildSqlAndParams();
    expect(sql).toContain('ORDER BY');
  });
});
