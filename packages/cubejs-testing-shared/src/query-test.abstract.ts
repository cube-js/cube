import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import { BaseQuery, prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';
import { StartedTestContainer } from 'testcontainers';
import { TO_PARTITION_RANGE } from '@cubejs-backend/shared';

import { createCubeSchema } from './utils';

export const prepareCompiler = (content: any, options?: any) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'main.js', content }
  ])
}, { adapter: 'postgres', ...options });

export abstract class QueryTestAbstract<T extends BaseDriver> {
  abstract getQueryClass(): any;

  protected getQuery(a: any, b: any): BaseQuery {
    const QueryClass = this.getQueryClass();

    return new QueryClass(a, b);
  }

  public async testRefreshKeyEveryDay(connection: T) {
    const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(
      createCubeSchema({
        name: 'cards',
        preAggregations: `
          countCreatedAt: {
              type: 'rollup',
              external: true,
              measureReferences: [count],
              timeDimensionReference: createdAt,
              granularity: \`day\`,
              partitionGranularity: \`month\`,
              scheduledRefresh: true,
              refreshKey: {
                every: \`1 day\`,
              },
          },
        `
      })
    );
    await compiler.compile();

    const query = this.getQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'cards.count'
      ],
      timeDimensions: [{
        dimension: 'cards.createdAt',
        granularity: 'day',
        dateRange: ['2016-12-30', '2017-01-05']
      }],
      filters: [],
      timezone: 'America/Los_Angeles',
    });

    const preAggregations: any = query.newPreAggregations().preAggregationsDescription();

    const [sql, params] = preAggregations[0].invalidateKeyQueries[0];

    console.log('Executing ', [sql, params]);

    await connection.query(sql, params, {});
  }

  public async testRefreshKeyIncrementalWithUpdateWindow(connection: T) {
    const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(
      createCubeSchema({
        name: 'cards',
        preAggregations: `
          countCreatedAt: {
              type: 'rollup',
              external: true,
              measureReferences: [count],
              timeDimensionReference: createdAt,
              granularity: \`day\`,
              partitionGranularity: \`month\`,
              scheduledRefresh: true,
              refreshKey: {
                every: \`1 day\`,
                incremental: true,
                updateWindow: \`7 day\`,
              },
          },
        `
      })
    );
    await compiler.compile();

    const query = this.getQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'cards.count'
      ],
      timeDimensions: [{
        dimension: 'cards.createdAt',
        granularity: 'day',
        dateRange: ['2016-12-30', '2017-01-05']
      }],
      filters: [],
      timezone: 'America/Los_Angeles',
    });

    const preAggregations: any = query.newPreAggregations().preAggregationsDescription();

    // eslint-disable-next-line prefer-const
    let [sql, params] = preAggregations[0].invalidateKeyQueries[0];
    // TODO Introduce full cycle testing through BaseDbRunner / QueryOrchestrator.
    // TODO Internal structures shouldn't be never accessed in tests.
    params = params.map((p: any) => (p === TO_PARTITION_RANGE ? '2017-01-05T00:00:00' : p));

    console.log('Executing ', [sql, params]);

    const res = await connection.query(sql, params, {});
    console.log(res);
  }

  public async testCreateIndexes(connection: T) {
    const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(
      createCubeSchema({
        name: 'cards',
        preAggregations: `
          countCreatedAt: {
              type: 'rollup',
              external: true,
              measures: [count, sum, max],
              dimensions: [id, filter],
              timeDimension: createdAt,
              granularity: \`day\`,
              indexes: {
                reg_default: {
                    columns: [id]
                },
                reg: {
                    type: \`regular\`,
                    columns: [id]
                },
                aggr: {
                    type: \`aggregate\`,
                    columns: [id]
                }
              }
          },
        `
      })
    );
    await compiler.compile();

    const query = this.getQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'cards.count'
      ],
      timeDimensions: [{
        dimension: 'cards.createdAt',
        granularity: 'day',
        dateRange: ['2016-12-30', '2017-01-05']
      }],
      filters: [],
      timezone: 'America/Los_Angeles',
    });

    const preAggregations: any = query.newPreAggregations().preAggregationsDescription();
    
    const preAggregation = preAggregations[0];

    const columns = [
      { name: 'cards__id', type: 'int' },
      { name: 'cards__created_at', type: 'Timestamp' },
      { name: 'cards__filter', type: 'int' },
      { name: 'cards__count', type: 'int' },
      { name: 'cards__sum', type: 'int' },
      { name: 'cards__max', type: 'int' },

    ];
    await connection.query(`CREATE SCHEMA ${preAggregation.preAggregationsSchema}`, [], {});
    await connection.uploadTableWithIndexes(
      preAggregation.tableName,
      columns,
      { rows: [] }, preAggregation.indexesSql, preAggregation.uniqueKeyColumns, {}, preAggregation.aggregationsColumns, preAggregation.createTableIndexes
    );

    // eslint-disable-next-line camelcase
    const tables = await connection.query<{ aggregate_columns: string }>('select * from system.tables', [], {});
    expect(tables).toHaveLength(1);
    const table = tables[0];
    expect(table).toHaveProperty('aggregate_columns');
    expect(table.aggregate_columns).toMatch('{ column: Column { name: "cards__count", column_type: Int, column_index: 3 }, function: SUM }');
    expect(table.aggregate_columns).toMatch('{ column: Column { name: "cards__max", column_type: Int, column_index: 5 }, function: MAX }');
    expect(table.aggregate_columns).toMatch('{ column: Column { name: "cards__sum", column_type: Int, column_index: 4 }, function: SUM }');

    // eslint-disable-next-line camelcase
    const indexes = await connection.query<{name: string; index_type: string}>('select * from system.indexes', [], {});
    expect(indexes).toHaveLength(4);
    const indexesMap: {[key: string]: any} = {
      cards_count_created_at_reg_default: { type: 'Regular', seen: false },
      cards_count_created_at_reg: { type: 'Regular', seen: false },
      cards_count_created_at_aggr: { type: 'Aggregate', seen: false },
      default: { type: 'Regular', seen: false },
    };
    // indexes.forEach(ind => expect(expected_indexes[ind.name]).type).equal(ind.index_type);
    indexes.forEach(ind => {
      expect(indexesMap[ind.name].type).toEqual(ind.index_type);
      indexesMap[ind.name].seen = true;
    });
    for (const [_, ind] of Object.entries(indexesMap)) {
      expect(ind.seen).toEqual(true);
    }

    // eslint-disable-next-line prefer-const
    /* let [sql, params] = preAggregations[0].invalidateKeyQueries[0];
    // TODO Introduce full cycle testing through BaseDbRunner / QueryOrchestrator.
    // TODO Internal structures shouldn't be never accessed in tests.
    params = params.map((p: any) => (p === TO_PARTITION_RANGE ? '2017-01-05T00:00:00' : p));

    await connection.query(sql, params, {}); */
  }
}

export interface QueryTestCaseOptions {
  name: string,
  connectionFactory: (container: StartedTestContainer) => BaseDriver,
  DbRunnerClass: any,
}

export function createQueryTestCase(test: QueryTestAbstract<any>, opts: QueryTestCaseOptions) {
  describe(`${opts.name}Query`, () => {
    jest.setTimeout(60 * 1000);

    let container: StartedTestContainer;
    let connection: BaseDriver;

    beforeAll(async () => {
      container = await opts.DbRunnerClass.startContainer({});
      connection = opts.connectionFactory(container);
    });

    afterAll(async () => {
      if (connection) {
        await connection.release();
      }

      if (container) {
        await container.stop();
      }
    });

    it('test refreshKey every day', async () => test.testRefreshKeyEveryDay(connection));
    it('test refreshKey incremental with update window', async () => test.testRefreshKeyIncrementalWithUpdateWindow(connection));
    it('test create indexes', async () => test.testCreateIndexes(connection));
  });
}
