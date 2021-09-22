import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareCompiler } from './PrepareCompiler';
import { PreAggregations } from '../../src/adapter/PreAggregations';

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

    return prepareCompiler(`cube('cube', ${JSON.stringify(cube)});`.replace(/"([^"]+)":/g, '$1:'));
  }

  function testPreAggregationMatch(
    expecting: boolean,
    cubeDimentions: Array<String>,
    preAggDimentions: Array<String>,
    queryDimentions: Array<String>,
    filters: Array<any>,
  ) {
    const aaa: any = {
      type: 'rollup',
      measures: ['cube.uniqueField'],
      dimensions: preAggDimentions.map(d => `cube.${d}`),
      timeDimension: 'cube.created',
      granularity: 'day',
      partitionGranularity: 'month',
    };

    const cube: any = {
      dimensions: {},
      preAggregations: { aaa }
    };

    cubeDimentions.forEach(d => {
      // @ts-ignore
      cube.dimensions[d] = { sql: d, type: 'string' };
    });

    const { compiler, joinGraph, cubeEvaluator } = getCube(cube);

    aaa.sortedDimensions = aaa.dimensions;
    aaa.sortedDimensions.sort();
    aaa.sortedTimeDimensions = [[aaa.timeDimension, aaa.granularity]];

    const result = compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        dimensions: queryDimentions.map(d => `cube.${d}`),
        measures: ['cube.uniqueField'],
        timeDimensions: [{
          dimension: 'cube.created',
          granularity: 'day',
          dateRange: { from: '2017-01-01', to: '2017-01-30' }
        }],
        timezone: 'America/Los_Angeles',
        filters
      });

      const usePreAggregation = PreAggregations.canUsePreAggregationForTransformedQueryFn(
        PreAggregations.transformQueryToCanUseForm(query),
        aaa
      );

      expect(usePreAggregation).toEqual(expecting);
    });

    return result;
  }

  it('Single Dimension, Single Filter', () => testPreAggregationMatch(
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

  it('Single Dimension, Single Filter, and', () => testPreAggregationMatch(
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

  it('Single Dimension, Single Filter, gt', () => testPreAggregationMatch(
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

  it('Single Dimension, Single Filter, Wrong Dimension', () => testPreAggregationMatch(
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
});
