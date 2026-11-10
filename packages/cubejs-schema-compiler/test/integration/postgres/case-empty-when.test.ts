import { PostgresQuery } from '../../../src/adapter';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Case dimension without when branches', () => {
  jest.setTimeout(200000);

  const compilers = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: >
      SELECT * FROM (VALUES (1, 'a'), (2, 'a'), (3, 'b')) AS t(id, status)

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: status
        sql: status
        type: string

      - name: status_bucket
        type: string
        case:
          when: []
          else:
            label: Unknown

    measures:
      - name: count
        type: count
  `);

  async function runQuery(q: any) {
    await compilers.compile();
    const query = compilers.createQuery(PostgresQuery, {
      ...q,
      timezone: 'UTC',
      preAggregationsSchema: '',
    });
    return dbRunner.testQuery(query.buildSqlAndParams());
  }

  it('renders the else label as the dimension value', async () => {
    const res = await runQuery({
      measures: ['orders.count'],
      dimensions: ['orders.status_bucket', 'orders.status'],
      order: [{ id: 'orders.status' }],
    });
    expect(res).toEqual([
      { orders__status_bucket: 'Unknown', orders__status: 'a', orders__count: '2' },
      { orders__status_bucket: 'Unknown', orders__status: 'b', orders__count: '1' },
    ]);
  });

  it('filters by the else label', async () => {
    const res = await runQuery({
      measures: ['orders.count'],
      filters: [{ member: 'orders.status_bucket', operator: 'equals', values: ['Unknown'] }],
    });
    expect(res).toEqual([{ orders__count: '3' }]);
  });
});
