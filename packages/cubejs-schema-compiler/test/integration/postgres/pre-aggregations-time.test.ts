/* eslint-disable no-restricted-syntax */
import R from 'ramda';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { PostgresDBRunner } from './PostgresDBRunner';

const SCHEMA_VARIANTS = [
  // with references postfix
  `
    cube(\`visitors\`, {
      sql: \`
      select * from visitors
      \`,

      measures: {
        count: {
          type: 'count'
        }
      },

      dimensions: {
        createdAt: {
          type: 'time',
          sql: 'created_at'
        },
      },

      preAggregations: {
        month: {
          type: 'rollup',
          measureReferences: [count],
          timeDimensionReference: createdAt,
          granularity: 'month',
        },
        day: {
          type: 'rollup',
          measureReferences: [count],
          timeDimensionReference: createdAt,
          granularity: 'day',
        },
      }
    })
  `,
  // without references postfix
  `
    cube(\`visitors\`, {
      sql: \`
      select * from visitors
      \`,

      measures: {
        count: {
          type: 'count'
        }
      },

      dimensions: {
        createdAt: {
          type: 'time',
          sql: 'created_at'
        },
      },

      preAggregations: {
        month: {
          type: 'rollup',
          measures: [count],
          timeDimension: createdAt,
          granularity: 'month',
        },
        day: {
          type: 'rollup',
          measures: [count],
          timeDimension: createdAt,
          granularity: 'day',
        },
      }
    })
  `,
];

for (const [index, schema] of Object.entries(SCHEMA_VARIANTS)) {
  // eslint-disable-next-line no-loop-func
  describe(`PreAggregations in time hierarchy (schema #${index})`, () => {
    jest.setTimeout(200000);

    const dbRunner = new PostgresDBRunner();

    afterAll(async () => {
      await dbRunner.tearDown();
    });

    const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(schema);

    function replaceTableName(query, preAggregation, suffix) {
      const [toReplace, params] = query;
      console.log(toReplace);
      preAggregation = Array.isArray(preAggregation) ? preAggregation : [preAggregation];
      return [
        preAggregation.reduce((replacedQuery, desc) => replacedQuery.replace(new RegExp(desc.tableName, 'g'), `${desc.tableName}_${suffix}`), toReplace),
        params
      ];
    }

    function tempTablePreAggregations(preAggregationsDescriptions) {
      return R.unnest(preAggregationsDescriptions.map(desc => desc.invalidateKeyQueries.concat([
        [desc.loadSql[0].replace('CREATE TABLE', 'CREATE TEMP TABLE'), desc.loadSql[1]]
      ])));
    }

    it('query on year match to pre-agg on month', () => compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.count'
        ],
        dimensions: [],
        timezone: 'America/Los_Angeles',
        timeDimensions: [{
          dimension: 'visitors.createdAt',
          granularity: 'year',
          dateRange: ['2016-12-01', '2018-12-31']
        }],
        preAggregationsSchema: '',
        order: [],
      });

      const queryAndParams = query.buildSqlAndParams();

      expect((<any>query).preAggregations.preAggregationForQuery.preAggregation.granularity).toEqual('month');

      console.log(queryAndParams);
      const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
      console.log(preAggregationsDescription);

      const queries = tempTablePreAggregations(preAggregationsDescription);

      console.log(JSON.stringify(queries.concat(queryAndParams)));

      return dbRunner.testQueries(
        queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 1))
      ).then(res => {
        console.log(JSON.stringify(res));
        expect(res).toEqual(
          [
            {
              visitors__count: '5',
              visitors__created_at_year: '2017-01-01T00:00:00.000Z'
            },
          ]
        );
      });
    }));

    it('query on week match to pre-agg on day', () => compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.count'
        ],
        dimensions: [],
        timezone: 'America/Los_Angeles',
        timeDimensions: [{
          dimension: 'visitors.createdAt',
          granularity: 'week',
          dateRange: ['2017-01-02', '2019-02-08']
        }],
        preAggregationsSchema: '',
        order: [],
        filters: [
          {
            or: [{
              dimension: 'visitors.count',
              operator: 'equals',
              values: [
                '5'
              ]
            }, {
              dimension: 'visitors.count',
              operator: 'equals',
              values: [
                '2'
              ]
            },
            ]
          }]
      });

      const queryAndParams = query.buildSqlAndParams();

      expect((<any>query).preAggregations.preAggregationForQuery.preAggregation.granularity).toEqual('day');

      console.log(queryAndParams);
      const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
      console.log(preAggregationsDescription);

      const queries = tempTablePreAggregations(preAggregationsDescription);

      console.log(JSON.stringify(queries.concat(queryAndParams)));

      return dbRunner.testQueries(
        queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 1))
      ).then(res => {
        console.log(JSON.stringify(res));
        expect(res).toEqual(
          [
            {
              visitors__count: '5',
              visitors__created_at_week: '2017-01-02T00:00:00.000Z'
            },
          ]
        );
      });
    }));
  });
}
