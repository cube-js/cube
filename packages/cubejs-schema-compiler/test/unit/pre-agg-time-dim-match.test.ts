import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareCube } from './PrepareCompiler';
import { PreAggregations } from '../../src/adapter/PreAggregations';

describe('Pre Aggregation by filter match tests', () => {
  function getCube(cube) {
    cube.sql = 'select * from table';
    cube.dataSource = 'default';

    cube.dimensions.created = {
      sql: 'created',
      type: 'time',
      granularities: {
        one_week: {
          interval: '1 week',
        },
        one_week_by_sunday: {
          interval: '1 week',
          offset: '-1 day'
        },
        two_weeks_by_1st_feb_00am: {
          interval: '2 weeks',
          origin: '2024-02-01 00:00:00'
        },
        two_weeks_by_1st_feb_10am: {
          interval: '2 weeks',
          origin: '2024-02-01 10:00:00'
        }
      }
    };

    return prepareCube('cube', cube);
  }

  function testPreAggregationMatch(
    expecting: boolean,
    measures: Array<String>,
    preAggTimeGranularity: string,
    queryAggTimeGranularity: string,
    queryTimeZone: string = 'America/Los_Angeles',
  ) {
    const aaa: any = {
      type: 'rollup',
      dimensions: [],
      measures: measures.map(m => `cube.${m}`),
      timeDimension: 'cube.created',
      granularity: preAggTimeGranularity,
      partitionGranularity: 'year',
      // Enabling only for custom granularities
      allowNonStrictDateRangeMatch: !/^(minute|hour|day|week|month|quarter|year)$/.test(preAggTimeGranularity)
    };

    const cube: any = {
      dimensions: {},
      measures: {},
      preAggregations: { aaa }
    };

    measures.forEach(m => {
      // @ts-ignore
      cube.measures[m] = { type: m, sql: m };
    });

    const { compiler, joinGraph, cubeEvaluator } = getCube(cube);

    // aaa.sortedDimensions = aaa.dimensions;
    // aaa.sortedDimensions.sort();
    aaa.sortedTimeDimensions = [[aaa.timeDimension, aaa.granularity]];

    return compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: measures.map(m => `cube.${m}`),
        timeDimensions: [{
          dimension: 'cube.created',
          granularity: queryAggTimeGranularity,
          dateRange: { from: '2017-01-01', to: '2017-03-31' }
        }],
        timezone: queryTimeZone,
      });

      const usePreAggregation = PreAggregations.canUsePreAggregationForTransformedQueryFn(
        PreAggregations.transformQueryToCanUseForm(query),
        aaa
      );

      expect(usePreAggregation).toEqual(expecting);
    });
  }

  it('1 count measure, day, day', () => testPreAggregationMatch(
    true, ['count'], 'day', 'day'
  ));

  it('1 count measure, one_week_by_sunday, one_week_by_sunday', () => testPreAggregationMatch(
    true, ['count'], 'one_week_by_sunday', 'one_week_by_sunday'
  ));

  it('1 count measure, two_weeks_by_1st_feb_00am, two_weeks_by_1st_feb_00am', () => testPreAggregationMatch(
    true, ['count'], 'two_weeks_by_1st_feb_00am', 'two_weeks_by_1st_feb_00am'
  ));

  it('1 count measure, day, one_week_by_sunday', () => testPreAggregationMatch(
    true, ['count'], 'day', 'one_week_by_sunday'
  ));

  it('1 count measure, day, two_weeks_by_1st_feb_00am', () => testPreAggregationMatch(
    true, ['count'], 'day', 'two_weeks_by_1st_feb_00am', 'UTC'
  ));

  it('1 count measure, day, two_weeks_by_1st_feb_00am', () => testPreAggregationMatch(
    false, ['count'], 'day', 'two_weeks_by_1st_feb_00am', 'Europe/Berlin'
  ));

  it('1 count measure, day, two_weeks_by_1st_feb_10am', () => testPreAggregationMatch(
    false, ['count'], 'day', 'two_weeks_by_1st_feb_10am'
  ));

  it('1 count measure, week, day', () => testPreAggregationMatch(
    false, ['count'], 'week', 'day'
  ));

  it('1 count measure, day, week', () => testPreAggregationMatch(
    true, ['count'], 'day', 'week'
  ));

  it('1 countDistinct measure, day, day', () => testPreAggregationMatch(
    true, ['countDistinct'], 'day', 'day'
  ));

  it('1 countDistinct measure, week, day', () => testPreAggregationMatch(
    false, ['countDistinct'], 'week', 'day'
  ));

  it('1 countDistinct measure, day, week', () => testPreAggregationMatch(
    false, ['countDistinct'], 'day', 'week'
  ));

  it('count+countDistinct measures, day, day', () => testPreAggregationMatch(
    true, ['count', 'countDistinct'], 'day', 'day'
  ));

  it('count+countDistinct measures, week, day', () => testPreAggregationMatch(
    false, ['count', 'countDistinct'], 'week', 'day'
  ));

  it('count+countDistinct measures, day, week', () => testPreAggregationMatch(
    false, ['count', 'countDistinct'], 'day', 'week'
  ));

  it('count+sum measures, day, day', () => testPreAggregationMatch(
    true, ['count', 'sum'], 'day', 'day'
  ));

  it('count+sum measures, week, day', () => testPreAggregationMatch(
    false, ['count', 'sum'], 'week', 'day'
  ));

  it('count+sum measures, day, week', () => testPreAggregationMatch(
    true, ['count', 'sum'], 'day', 'week'
  ));
});
