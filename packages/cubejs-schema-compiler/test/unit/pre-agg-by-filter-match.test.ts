import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareCube } from './PrepareCompiler';
import { PreAggregations } from '../../src/adapter/PreAggregations';
import { PreAggregationReferences } from '../../src/compiler/CubeEvaluator';

describe('Pre Aggregation by filter match tests', () => {
  function getCube(cube) {
    cube.sql = 'select * from table';
    cube.dataSource = 'default';
    cube.measures = {
      uniqueField: {
        type: 'countDistinct',
        sql: 'field',
      },
    };

    cube.dimensions.created = {
      sql: 'created',
      type: 'time'
    };

    return prepareCube('cube', cube);
  }

  async function testPreAggregationMatch(
    expecting: boolean,
    cubedimensions: Array<String>,
    preAggdimensions: Array<String>,
    querydimensions: Array<String>,
    filters: Array<any>,
    preAggSegments: Array<String> | undefined = undefined,
    querySegments: Array<String> | undefined = undefined
  ) {
    const testPreAgg = {
      type: 'rollup',
      measures: ['cube.uniqueField'],
      dimensions: preAggdimensions.map(d => `cube.${d}`),
      timeDimension: 'cube.created',
      segments: preAggSegments?.map(s => `cube.${s}`),
      granularity: 'day',
      partitionGranularity: 'month',
    };

    const cube = {
      segments: {
        qqq: { sql: 'id > 10000' }
      },
      dimensions: Object.fromEntries(cubedimensions.map(d => [d, { sql: d, type: 'string' }])),
      preAggregations: { testPreAgg }
    };

    const { compiler, joinGraph, cubeEvaluator } = getCube(cube);

    const refs: PreAggregationReferences = {
      dimensions: testPreAgg.segments ? testPreAgg.dimensions.concat(testPreAgg.segments) : testPreAgg.dimensions,
      measures: testPreAgg.measures,
      timeDimensions: [{
        dimension: testPreAgg.timeDimension,
        granularity: testPreAgg.granularity,
      }],
      rollups: [],
      fullNameDimensions: [],
      fullNameMeasures: [],
      fullNameTimeDimensions: [],
    };

    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      dimensions: querydimensions.map(d => `cube.${d}`),
      measures: ['cube.uniqueField'],
      timeDimensions: [{
        dimension: 'cube.created',
        granularity: 'day',
        dateRange: { from: '2017-01-01', to: '2017-01-30' }
      }],
      timezone: 'America/Los_Angeles',
      filters,
      segments: querySegments?.map(s => `cube.${s}`),
    });

    const usePreAggregation = PreAggregations.canUsePreAggregationForTransformedQueryFn(
      PreAggregations.transformQueryToCanUseForm(query),
      refs
    );

    expect(usePreAggregation).toEqual(expecting);
  }

  it('1 Dimension, 1 Filter', () => testPreAggregationMatch(
    true,
    ['type'],
    ['type'],
    [],
    [
      {
        member: 'cube.type',
        operator: 'equals',
        values: ['aa']
      },
    ]
  ));

  it('1 Dimension, 1 Filter, and', () => testPreAggregationMatch(
    true,
    ['type'],
    ['type'],
    [],
    [
      {
        member: 'cube.type',
        operator: 'equals',
        values: ['aa']
      },
      {
        and: [
          {
            member: 'cube.type',
            operator: 'equals',
            values: ['aa', 'bbb']
          },
          {
            and: [
              {
                member: 'cube.type',
                operator: 'equals',
                values: ['aa', 'bbb', 'ccc']
              }
            ]
          }
        ]
      }
    ]
  ));

  it('2 cube dims, 2 pre-agg dims, 1 query dim, 2 Filters', () => testPreAggregationMatch(
    true,
    ['type', 'dim2'],
    ['type', 'dim2'],
    ['type'],
    [
      {
        member: 'cube.type',
        operator: 'equals',
        values: ['a', 'b']
      },
      {
        member: 'cube.dim2',
        operator: 'equals',
        values: ['a']
      },
    ]
  ));

  it('1 Dimension, 1 Filter, gt', () => testPreAggregationMatch(
    false,
    ['type'],
    ['type'],
    [],
    [
      {
        member: 'cube.type',
        operator: 'gt',
        values: ['aa']
      },
    ]
  ));

  it('1 Dimension, 1 Filter by another Dimension', () => testPreAggregationMatch(
    false,
    ['type', 'dim2'],
    ['type'],
    [],
    [
      {
        member: 'cube.dim2',
        operator: 'equals',
        values: ['aa']
      },
    ]
  ));

  it('2 Dimensions, 2 Filters', () => testPreAggregationMatch(
    true,
    ['type', 'dim2'],
    ['type', 'dim2'],
    [],
    [
      {
        member: 'cube.type',
        operator: 'equals',
        values: ['aa']
      },
      {
        member: 'cube.dim2',
        operator: 'equals',
        values: ['bbb']
      },
    ]
  ));

  it('2 Dimensions, 2 Filters, not eq', () => testPreAggregationMatch(
    false,
    ['type', 'dim2'],
    ['type', 'dim2'],
    [],
    [
      {
        member: 'cube.type',
        operator: 'equals',
        values: ['aa']
      },
      {
        member: 'cube.dim2',
        operator: 'gt',
        values: ['bbb']
      },
    ]
  ));

  it('2 Dimensions, 1 Filter', () => testPreAggregationMatch(
    false,
    ['type', 'dim2'],
    ['type', 'dim2'],
    [],
    [
      {
        member: 'cube.type',
        operator: 'equals',
        values: ['aa']
      },
    ]
  ));

  it('2 Dimensions, 1 Filter, 2 Query Dims', () => testPreAggregationMatch(
    true,
    ['type', 'dim2'],
    ['type', 'dim2'],
    ['type', 'dim2'],
    [
      {
        member: 'cube.type',
        operator: 'equals',
        values: ['aa']
      },
    ]
  ));

  it('1 Dimensions, 2 Filter', () => testPreAggregationMatch(
    false,
    ['type', 'dim2'],
    ['type'],
    [],
    [
      {
        member: 'cube.type',
        operator: 'equals',
        values: ['aa']
      },
      {
        member: 'cube.dim2',
        operator: 'equals',
        values: ['aa']
      },
    ]
  ));

  it('1 Dimensions, 1 Filter, 2 Query Dims', () => testPreAggregationMatch(
    false,
    ['type', 'dim2'],
    ['type'],
    [],
    [
      {
        member: 'cube.type',
        operator: 'equals',
        values: ['aa']
      },
      {
        member: 'cube.dim2',
        operator: 'equals',
        values: ['aa']
      },
    ]
  ));

  it('1 Dimensions, 1 Filter, 2 Query Dims', () => testPreAggregationMatch(
    true,
    ['type', 'dim2'],
    ['type', 'dim2'],
    ['type', 'dim2'],
    [
      {
        member: 'cube.type',
        operator: 'equals',
        values: ['aa']
      },
    ]
  ));

  it('2 Dimensions, 1 Filter, 1 Query Dim', () => testPreAggregationMatch(
    true,
    ['type', 'dim2'],
    ['type', 'dim2'],
    ['dim2'],
    [
      {
        member: 'cube.type',
        operator: 'equals',
        values: ['aa']
      },
    ]
  ));

  it('3 dims cube, 2 dims pre-agg, 1 Filter, 2 Query Dim', () => testPreAggregationMatch(
    false,
    ['type', 'dim2', 'dim3'],
    ['type', 'dim2'],
    ['dim2', 'dim3'],
    [
      {
        member: 'cube.type',
        operator: 'equals',
        values: ['aa']
      },
    ]
  ));

  it('1 Segment, 0 Filter', () => testPreAggregationMatch(
    false,
    ['type'],
    [],
    [],
    [],
    ['qqq']
  ));

  it('1 Segment, 1 Filter', () => testPreAggregationMatch(
    true,
    ['type'],
    ['type'],
    [],
    [
      {
        member: 'cube.type',
        operator: 'equals',
        values: ['aaa']
      },
    ],
    ['qqq'],
    ['qqq']
  ));
});
