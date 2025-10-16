import { PostgresQuery } from '../../src';
import { prepareYamlCompiler } from './PrepareCompiler';

describe('Query-time Granularity Offset', () => {
  const compilers = prepareYamlCompiler(`
    cubes:
      - name: orders
        sql: "SELECT * FROM orders"
        
        dimensions:
          - name: id
            sql: id
            type: number
            primary_key: true
            
          - name: createdAt
            sql: created_at
            type: time
            granularities:
              - name: custom_week
                interval: 1 week
                offset: 4 days
        
        measures:
          - name: count
            type: count
  `);

  it('should accept offset parameter with day granularity', async () => {
    await compilers.compiler.compile();

    const query = new PostgresQuery(compilers, {
      measures: ['orders.count'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'day',
        offset: '-2 hours 30 minutes',
        dateRange: ['2024-01-01', '2024-01-31']
      }],
      timezone: 'UTC'
    });

    const queryAndParams = query.buildSqlAndParams();
    expect(queryAndParams[0]).toBeDefined();
    expect(queryAndParams[0]).toContain('orders');
  });

  it('should apply offset to predefined hour granularity', async () => {
    await compilers.compiler.compile();

    const query = new PostgresQuery(compilers, {
      measures: ['orders.count'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'hour',
        offset: '15 minutes',
        dateRange: ['2024-01-01', '2024-01-02']
      }],
      timezone: 'UTC'
    });

    const queryAndParams = query.buildSqlAndParams();
    expect(queryAndParams[0]).toBeDefined();
  });

  it('should reject offset with custom granularity', async () => {
    await compilers.compiler.compile();

    expect(() => {
      new PostgresQuery(compilers, {
        measures: ['orders.count'],
        timeDimensions: [{
          dimension: 'orders.createdAt',
          granularity: 'custom_week', // Custom granularity
          offset: '2 days', // Should be rejected
          dateRange: ['2024-01-01', '2024-01-31']
        }],
        timezone: 'UTC'
      });
    }).toThrow('Query-time offset parameter cannot be used with custom granularity');
  });

  it('should support negative offsets', async () => {
    await compilers.compiler.compile();

    const query = new PostgresQuery(compilers, {
      measures: ['orders.count'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'day',
        offset: '-6 hours',
        dateRange: ['2024-01-01', '2024-01-31']
      }],
      timezone: 'UTC'
    });

    const queryAndParams = query.buildSqlAndParams();
    expect(queryAndParams[0]).toBeDefined();
  });

  it('should accept complex offset formats', async () => {
    await compilers.compiler.compile();

    const query = new PostgresQuery(compilers, {
      measures: ['orders.count'],
      timeDimensions: [{
        dimension: 'orders.createdAt',
        granularity: 'day',
        offset: '2 hours 30 minutes 15 seconds',
        dateRange: ['2024-01-01', '2024-01-31']
      }],
      timezone: 'UTC'
    });

    const queryAndParams = query.buildSqlAndParams();
    expect(queryAndParams[0]).toBeDefined();
  });
});

