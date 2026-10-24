import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// https://github.com/cube-js/cube/issues/12375
// A rolling count that turns into COUNT(DISTINCT ...) across a one_to_many join gets a
// time_series CTE in a partitioned rollup's build SQL. The partition bounds there must be
// bound params (like the WHERE bounds), not '__FROM_PARTITION_RANGE' string literals.
describe('PreAggregations partitioned non-additive rolling window', () => {
  jest.setTimeout(200000);

  const compilers = prepareJsCompiler(`
  cube('rpl_events', {
    sql: \`
      SELECT 1 AS id, 's1' AS site_id, 'new' AS status, TIMESTAMP '2017-01-05 10:00:00' AS created_at
      UNION ALL
      SELECT 2 AS id, 's1' AS site_id, 'done' AS status, TIMESTAMP '2017-01-06 09:30:00' AS created_at
    \`,
    joins: {
      rpl_sites: {
        relationship: 'many_to_one',
        sql: \`\${CUBE}.site_id = \${rpl_sites}.id\`,
      },
    },
    measures: {
      count: {
        type: 'count',
      },
      runningCount: {
        type: 'count',
        sql: \`\${CUBE}.status <> 'new'\`,
        rollingWindow: {
          trailing: 'unbounded',
        },
      },
    },
    dimensions: {
      id: {
        sql: 'id',
        type: 'number',
        primaryKey: true,
      },
      status: {
        sql: 'status',
        type: 'string',
      },
      createdAt: {
        sql: 'created_at',
        type: 'time',
      },
    },
    preAggregations: {
      main: {
        measures: [CUBE.count, CUBE.runningCount],
        dimensions: [CUBE.status, rpl_projects.id],
        timeDimension: CUBE.createdAt,
        granularity: 'day',
        partitionGranularity: 'year',
      },
    },
  });

  cube('rpl_sites', {
    sql: \`SELECT 's1' AS id\`,
    joins: {
      rpl_projects: {
        relationship: 'one_to_many',
        sql: \`\${CUBE}.id = \${rpl_projects}.site_id\`,
      },
    },
    dimensions: {
      id: {
        sql: 'id',
        type: 'string',
        primaryKey: true,
      },
    },
  });

  cube('rpl_projects', {
    sql: \`
      SELECT 'p1' AS id, 's1' AS site_id
      UNION ALL
      SELECT 'p2' AS id, 's1' AS site_id
    \`,
    dimensions: {
      id: {
        sql: 'id',
        type: 'string',
        primaryKey: true,
      },
    },
  });
  `);

  it('builds the partitions without literal partition placeholders', async () => {
    await compilers.compile();

    const query = compilers.createQuery(PostgresQuery, {
      measures: ['rpl_events.runningCount'],
      dimensions: ['rpl_events.status', 'rpl_projects.id'],
      timeDimensions: [{
        dimension: 'rpl_events.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-05', '2017-01-06'],
      }],
      timezone: 'UTC',
      preAggregationsSchema: '',
    });

    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    expect(preAggregationsDescription[0].tableName).toEqual('rpl_events_main');
    expect(preAggregationsDescription[0].loadSql[0]).not.toContain('__FROM_PARTITION_RANGE');
    expect(preAggregationsDescription[0].loadSql[0]).not.toContain('__TO_PARTITION_RANGE');

    const res = await dbRunner.evaluateQueryWithPreAggregations(query);
    expect(res.length).toBeGreaterThan(0);
  });
});
