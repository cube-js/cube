import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';

// When a cube's access policy denies the queried members, RBAC
// (CompilerApi.applyRowLevelSecurity) appends a member-expression segment
// `{ expression: () => '1 = 0', cubeName, name: 'rlsAccessDenied' }`. The rollup
// must still be selected (the `1 = 0` is just a constant filter on top of it).
// This guards that the segment doesn't disqualify pre-aggregation matching.
describe('PreAggregations access-denied segment', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
  cube('rls_visitors', {
    sql: 'select * from visitors',
    sqlAlias: 'rlsv',

    measures: {
      count: {
        type: 'count'
      }
    },

    dimensions: {
      id: {
        type: 'number',
        sql: 'id',
        primaryKey: true
      },
      status: {
        type: 'number',
        sql: 'status'
      }
    },

    preAggregations: {
      statusRollup: {
        type: 'rollup',
        measures: [CUBE.count],
        dimensions: [CUBE.status],
      }
    }
  });

  view('rls_visitors_view', {
    cubes: [
      {
        join_path: 'rls_visitors',
        includes: '*',
      },
    ]
  });
  `);

  it('selects the rollup despite the access-denied segment', async () => {
    await compiler.compile();

    const query = new PostgresQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['rls_visitors_view.count'],
        // Byte-for-byte the segment CompilerApi.applyRowLevelSecurity injects on denial.
        segments: [
          {
            expression: () => '1 = 0',
            cubeName: 'rls_visitors',
            name: 'rlsAccessDenied',
          },
        ],
        timezone: 'America/Los_Angeles',
        preAggregationsSchema: '',
      }
    );

    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    const [sql] = query.buildSqlAndParams();

    expect(preAggregationsDescription[0].tableName).toEqual('rlsv_status_rollup');
    expect(sql).toContain('rlsv_status_rollup');
    expect(sql).toContain('1 = 0');
  });
});
