import dayjs from 'dayjs';
import {
  groupBy, pipe, fromPairs, uniq, map, dropLast, equals, reduce, minBy, maxBy, clone, mergeDeepLeft,
  flatten,
} from 'ramda';

import { aliasSeries } from './utils';
import {
  DateRegex,
  dayRange,
  internalDayjs,
  isPredefinedGranularity,
  LocalDateRegex,
  TIME_SERIES,
  timeSeriesFromCustomInterval
} from './time';
import {
  Annotation,
  ChartPivotRow, DateRange,
  DrillDownLocator,
  LoadResponse,
  LoadResponseResult, Pivot,
  PivotConfig, PivotConfigFull,
  PivotQuery,
  PivotRow,
  Query,
  QueryAnnotations, QueryType,
  SerializedResult,
  Series,
  SeriesNamesColumn,
  TableColumn,
  TimeDimension
} from './types';

const groupByToPairs = function groupByToPairsImpl<T, K>(keyFn: (item: T) => K): (data: T[]) => [K, T[]][] {
  const acc = new Map();

  return (data) => {
    data.forEach((row) => {
      const key = keyFn(row);

      if (!acc.has(key)) {
        acc.set(key, []);
      }

      acc.get(key).push(row);
    });

    return Array.from(acc.entries());
  };
};

const unnest = (arr: any[][]): any[] => {
  const res: any[] = [];
  arr.forEach((subArr) => {
    subArr.forEach(element => res.push(element));
  });

  return res;
};

export const QUERY_TYPE: Record<string, QueryType> = {
  REGULAR_QUERY: 'regularQuery',
  COMPARE_DATE_RANGE_QUERY: 'compareDateRangeQuery',
  BLENDING_QUERY: 'blendingQuery',
};

export type ResultSetOptions = {
  parseDateMeasures?: boolean;
};

/**
 * Provides a convenient interface for data manipulation.
 */
export default class ResultSet<T extends Record<string, any> = any> {
  private readonly loadResponse: LoadResponse<T>;

  private readonly loadResponses: LoadResponseResult<T>[];

  private readonly queryType: QueryType;

  private readonly parseDateMeasures: boolean | undefined;

  private readonly options: {};

  private readonly backwardCompatibleData: Record<string, any>[][];

  public static measureFromAxis(axisValues: string[]): string {
    return axisValues[axisValues.length - 1];
  }

  public static timeDimensionMember(td: TimeDimension) {
    return `${td.dimension}.${td.granularity}`;
  }

  /**
   * ```js
   * import { ResultSet } from '@cubejs-client/core';
   *
   * const resultSet = await cubeApi.load(query);
   * // You can store the result somewhere
   * const tmp = resultSet.serialize();
   *
   * // and restore it later
   * const resultSet = ResultSet.deserialize(tmp);
   * ```
   * @param data the result of [serialize](#result-set-serialize)
   * @param options
   */
  public static deserialize<TData extends Record<string, any> = any>(data: SerializedResult, options?: Object): ResultSet<TData> {
    return new ResultSet(data.loadResponse, options);
  }

  public constructor(loadResponse: LoadResponse<T> | LoadResponseResult<T>, options: ResultSetOptions = {}) {
    if ('queryType' in loadResponse && loadResponse.queryType != null) {
      this.loadResponse = loadResponse;
      this.queryType = loadResponse.queryType;
      this.loadResponses = loadResponse.results;
    } else {
      this.queryType = QUERY_TYPE.REGULAR_QUERY;
      this.loadResponse = {
        ...loadResponse,
        pivotQuery: {
          ...loadResponse.query,
          queryType: this.queryType
        }
      } as LoadResponse<T>;
      this.loadResponses = [loadResponse as LoadResponseResult<T>];
    }

    if (!Object.values(QUERY_TYPE).includes(this.queryType)) {
      throw new Error('Unknown query type');
    }

    this.parseDateMeasures = options.parseDateMeasures;
    this.options = options;

    this.backwardCompatibleData = [];
  }

  /**
   * Returns a measure drill down query.
   *
   * Provided you have a measure with the defined `drillMembers` on the `Orders` cube
   * ```js
   * measures: {
   *   count: {
   *     type: `count`,
   *     drillMembers: [Orders.status, Users.city, count],
   *   },
   *   // ...
   * }
   * ```
   *
   * Then you can use the `drillDown` method to see the rows that contribute to that metric
   * ```js
   * resultSet.drillDown(
   *   {
   *     xValues,
   *     yValues,
   *   },
   *   // you should pass the `pivotConfig` if you have used it for axes manipulation
   *   pivotConfig
   * )
   * ```
   *
   * the result will be a query with the required filters applied and the dimensions/measures filled out
   * ```js
   * {
   *   measures: ['Orders.count'],
   *   dimensions: ['Orders.status', 'Users.city'],
   *   filters: [
   *     // dimension and measure filters
   *   ],
   *   timeDimensions: [
   *     //...
   *   ]
   * }
   * ```
   *
   * In case when you want to add `order` or `limit` to the query, you can simply spread it
   *
   * ```js
   * // An example for React
   * const drillDownResponse = useCubeQuery(
   *    {
   *      ...drillDownQuery,
   *      limit: 30,
   *      order: {
   *        'Orders.ts': 'desc'
   *      }
   *    },
   *    {
   *      skip: !drillDownQuery
   *    }
   *  );
   * ```
   * @returns Drill down query
   */
  public drillDown(drillDownLocator: DrillDownLocator, pivotConfig?: PivotConfig): Query | null {
    if (this.queryType === QUERY_TYPE.COMPARE_DATE_RANGE_QUERY) {
      throw new Error('compareDateRange drillDown query is not currently supported');
    }
    if (this.queryType === QUERY_TYPE.BLENDING_QUERY) {
      throw new Error('Data blending drillDown query is not currently supported');
    }

    const { query } = this.loadResponses[0];
    const xValues = drillDownLocator?.xValues ?? [];
    const yValues = drillDownLocator?.yValues ?? [];
    const normalizedPivotConfig = this.normalizePivotConfig(pivotConfig);

    const values: string[][] = [];
    normalizedPivotConfig?.x.forEach((member, currentIndex) => values.push([member, xValues[currentIndex]]));
    normalizedPivotConfig?.y.forEach((member, currentIndex) => values.push([member, yValues[currentIndex]]));

    const { filters: parentFilters = [], segments = [] } = this.query();
    const { measures } = this.loadResponses[0].annotation;
    let [, measureName] = values.find(([member]) => member === 'measures') || [];

    if (measureName === undefined) {
      [measureName] = Object.keys(measures);
    }

    if (!(measures[measureName]?.drillMembers?.length ?? 0)) {
      return null;
    }

    const filters = [
      {
        member: measureName,
        operator: 'measureFilter',
      },
      ...parentFilters
    ];
    const timeDimensions = [];

    values.filter(([member]) => member !== 'measures')
      .forEach(([member, value]) => {
        const [cubeName, dimension, granularity] = member.split('.');

        if (granularity !== undefined) {
          const range = dayRange(value, value).snapTo(granularity);
          const originalTimeDimension = query.timeDimensions?.find((td) => td.dimension);

          let dateRange = [
            range.start,
            range.end
          ];

          if (originalTimeDimension?.dateRange) {
            const [originalStart, originalEnd] = originalTimeDimension.dateRange;

            dateRange = [
              dayjs(originalStart) > range.start ? dayjs(originalStart) : range.start,
              dayjs(originalEnd) < range.end ? dayjs(originalEnd) : range.end,
            ];
          }

          timeDimensions.push({
            dimension: [cubeName, dimension].join('.'),
            dateRange: dateRange.map((dt) => dt.format('YYYY-MM-DDTHH:mm:ss.SSS')),
          });
        } else if (value == null) {
          filters.push({
            member,
            operator: 'notSet',
          });
        } else {
          filters.push({
            member,
            operator: 'equals',
            values: [value.toString()],
          });
        }
      });

    if (
      timeDimensions.length === 0 &&
      Array.isArray(query.timeDimensions) &&
      query.timeDimensions.length > 0 &&
      query.timeDimensions[0].granularity == null
    ) {
      timeDimensions.push(query.timeDimensions[0]);
    }

    return {
      ...measures[measureName].drillMembersGrouped,
      filters,
      ...(segments.length > 0 ? { segments } : {}),
      timeDimensions,
      segments,
      timezone: query.timezone
    };
  }

  /**
   * Returns an array of series with key, title and series data.
   * ```js
   * // For the query
   * {
   *   measures: ['Stories.count'],
   *   timeDimensions: [{
   *     dimension: 'Stories.time',
   *     dateRange: ['2015-01-01', '2015-12-31'],
   *     granularity: 'month'
   *   }]
   * }
   *
   * // ResultSet.series() will return
   * [
   *   {
   *     key: 'Stories.count',
   *     title: 'Stories Count',
   *     shortTitle: 'Count',
   *     series: [
   *       { x: '2015-01-01T00:00:00', value: 27120 },
   *       { x: '2015-02-01T00:00:00', value: 25861 },
   *       { x: '2015-03-01T00:00:00', value: 29661 },
   *       //...
   *     ],
   *   },
   * ]
   * ```
   */
  public series<SeriesItem = any>(pivotConfig?: PivotConfig): Series<SeriesItem>[] {
    return this.seriesNames(pivotConfig).map(({ title, shortTitle, key }) => ({
      title,
      shortTitle,
      key,
      series: this.chartPivot(pivotConfig).map(({ x, ...obj }) => ({ value: obj[key], x }))
    } as Series<SeriesItem>));
  }

  private axisValues(axis: string[], resultIndex = 0) {
    const { query } = this.loadResponses[resultIndex];

    return (row: Record<string, any>) => {
      const value = (measure?: string) => axis
        .filter(d => d !== 'measures')
        .map((d: string) => {
          const val = row[d];
          return val != null ? val : null;
        })
        .concat(measure ? [measure] : []);

      if (axis.find(d => d === 'measures') && (query.measures || []).length) {
        return (query.measures || []).map(value);
      }

      return [value()];
    };
  }

  private axisValuesString(axisValues: (string | number)[], delimiter: string = ', '): string {
    const formatValue = (v: string | number) => {
      if (v == null) {
        return '∅';
      } else if (v === '') {
        return '[Empty string]';
      } else {
        return v;
      }
    };
    return axisValues.map(formatValue).join(delimiter);
  }

  public static getNormalizedPivotConfig(query?: PivotQuery, pivotConfig?: PivotConfig): PivotConfigFull {
    const defaultPivotConfig: PivotConfig = {
      x: [],
      y: [],
      fillMissingDates: true,
      joinDateRange: false
    };

    const {
      measures = [],
      dimensions = []
    } = query || {};

    const timeDimensions = (query?.timeDimensions || []).filter(td => !!td.granularity);

    pivotConfig = pivotConfig || (timeDimensions.length ? {
      x: timeDimensions.map(td => ResultSet.timeDimensionMember(td)),
      y: dimensions
    } : {
      x: dimensions,
      y: []
    });

    const normalizedPivotConfig = mergeDeepLeft(pivotConfig, defaultPivotConfig) as PivotConfigFull;

    const substituteTimeDimensionMembers = (axis: string[]) => axis.map(
      subDim => (
        (
          timeDimensions.find(td => td.dimension === subDim) &&
          !dimensions.find(d => d === subDim)
        ) ?
          ResultSet.timeDimensionMember((query?.timeDimensions || []).find(td => td.dimension === subDim)!) :
          subDim
      )
    );

    normalizedPivotConfig.x = substituteTimeDimensionMembers(normalizedPivotConfig.x);
    normalizedPivotConfig.y = substituteTimeDimensionMembers(normalizedPivotConfig.y);

    const allIncludedDimensions = normalizedPivotConfig.x.concat(normalizedPivotConfig.y);
    const allDimensions = timeDimensions.map(td => ResultSet.timeDimensionMember(td)).concat(dimensions);

    const dimensionFilter = (key: string) => allDimensions.includes(key) || key === 'measures';

    normalizedPivotConfig.x = normalizedPivotConfig.x.concat(
      allDimensions.filter(d => !allIncludedDimensions.includes(d) && d !== 'compareDateRange')
    )
      .filter(dimensionFilter);
    normalizedPivotConfig.y = normalizedPivotConfig.y.filter(dimensionFilter);

    if (!normalizedPivotConfig.x.concat(normalizedPivotConfig.y).find(d => d === 'measures')) {
      normalizedPivotConfig.y.push('measures');
    }

    if (dimensions.includes('compareDateRange') && !normalizedPivotConfig.y.concat(normalizedPivotConfig.x).includes('compareDateRange')) {
      normalizedPivotConfig.y.unshift('compareDateRange');
    }

    if (!measures.length) {
      normalizedPivotConfig.x = normalizedPivotConfig.x.filter(d => d !== 'measures');
      normalizedPivotConfig.y = normalizedPivotConfig.y.filter(d => d !== 'measures');
    }

    return normalizedPivotConfig;
  }

  public normalizePivotConfig(pivotConfig?: PivotConfig): PivotConfigFull {
    return ResultSet.getNormalizedPivotConfig(this.loadResponse.pivotQuery, pivotConfig);
  }

  public timeSeries(timeDimension: TimeDimension, resultIndex?: number, annotations?: Record<string, Annotation>) {
    if (!timeDimension.granularity) {
      return null;
    }

    let dateRange: DateRange | null | undefined;
    dateRange = timeDimension.dateRange;

    if (!dateRange) {
      const member = ResultSet.timeDimensionMember(timeDimension);
      const rawRows: Record<string, any>[] = this.timeDimensionBackwardCompatibleData(resultIndex || 0);

      const dates = rawRows
        .map(row => {
          const value = row[member];
          return value ? internalDayjs(value) : null;
        })
        .filter((d): d is dayjs.Dayjs => Boolean(d));

      dateRange = dates.length && [
        (reduce(minBy((d: dayjs.Dayjs): Date => d.toDate()), dates[0], dates)).toString(),
        (reduce(maxBy((d: dayjs.Dayjs): Date => d.toDate()), dates[0], dates)).toString(),
      ] || null;
    }

    if (!dateRange) {
      return null;
    }

    const padToDay = timeDimension.dateRange ?
      (timeDimension.dateRange as string[]).find(d => d.match(DateRegex)) :
      !['hour', 'minute', 'second'].includes(timeDimension.granularity);

    const [start, end] = dateRange;
    const range = dayRange(start, end);

    if (isPredefinedGranularity(timeDimension.granularity)) {
      return TIME_SERIES[timeDimension.granularity](
        padToDay ? range.snapTo('d') : range
      );
    }

    if (!annotations?.[`${timeDimension.dimension}.${timeDimension.granularity}`]) {
      throw new Error(`Granularity "${timeDimension.granularity}" not found in time dimension "${timeDimension.dimension}"`);
    }

    return timeSeriesFromCustomInterval(
      start, end, annotations[`${timeDimension.dimension}.${timeDimension.granularity}`].granularity!
    );
  }

  /**
   * Base method for pivoting [ResultSet](#result-set) data.
   * Most of the time shouldn't be used directly and [chartPivot](#result-set-chart-pivot)
   * or [tablePivot](#table-pivot) should be used instead.
   *
   * You can find the examples of using the `pivotConfig` [here](#types-pivot-config)
   * ```js
   * // For query
   * {
   *   measures: ['Stories.count'],
   *   timeDimensions: [{
   *     dimension: 'Stories.time',
   *     dateRange: ['2015-01-01', '2015-03-31'],
   *     granularity: 'month'
   *   }]
   * }
   *
   * // ResultSet.pivot({ x: ['Stories.time'], y: ['measures'] }) will return
   * [
   *   {
   *     xValues: ["2015-01-01T00:00:00"],
   *     yValuesArray: [
   *       [['Stories.count'], 27120]
   *     ]
   *   },
   *   {
   *     xValues: ["2015-02-01T00:00:00"],
   *     yValuesArray: [
   *       [['Stories.count'], 25861]
   *     ]
   *   },
   *   {
   *     xValues: ["2015-03-01T00:00:00"],
   *     yValuesArray: [
   *       [['Stories.count'], 29661]
   *     ]
   *   }
   * ]
   * ```
   * @returns An array of pivoted rows.
   */
  public pivot(pivotConfig?: PivotConfig): PivotRow[] {
    const normalizedPivotConfig = this.normalizePivotConfig(pivotConfig);
    const { pivotQuery: query } = this.loadResponse;

    const pivotImpl = (resultIndex = 0) => {
      let groupByXAxis = groupByToPairs<{ xValues: string[], row: Record<string, any> }, string>(({ xValues }) => this.axisValuesString(xValues));

      const measureValue = (row: Record<string, any>, measure: string) => row[measure] || normalizedPivotConfig.fillWithValue || 0;

      if (
        normalizedPivotConfig.fillMissingDates &&
        normalizedPivotConfig.x.length === 1 &&
        (equals(
          normalizedPivotConfig.x,
          (query.timeDimensions || [])
            .filter(td => Boolean(td.granularity))
            .map(td => ResultSet.timeDimensionMember(td))
        ))
      ) {
        const series = this.loadResponses.map(
          (loadResponse) => this.timeSeries(
            loadResponse.query.timeDimensions![0],
            resultIndex, loadResponse.annotation.timeDimensions
          )
        );

        if (series[0]) {
          groupByXAxis = (rows) => {
            const byXValues = groupBy(
              ({ xValues }) => xValues[0],
              rows
            );
            return series[resultIndex]?.map(d => [d, byXValues[d] || [{ xValues: [d], row: {} }]]) ?? [];
          };
        }
      }

      const xGrouped: [string, { xValues: string[], row: Record<string, any> }[]][] = pipe(
        map((row: Record<string, any>) => this.axisValues(normalizedPivotConfig.x, resultIndex)(row).map(xValues => ({ xValues, row }))),
        unnest,
        groupByXAxis
      )(this.timeDimensionBackwardCompatibleData(resultIndex));

      const yValuesMap: Record<string, any> = {};
      xGrouped.forEach(([, rows]) => {
        rows.forEach(({ row }) => {
          this.axisValues(normalizedPivotConfig.y, resultIndex)(row).forEach((values) => {
            if (Object.keys(row).length > 0) {
              yValuesMap[values.join()] = values;
            }
          });
        });
      });
      const allYValues = Object.values(yValuesMap);

      const measureOnX = Boolean((normalizedPivotConfig.x).find(d => d === 'measures'));

      return xGrouped.map(([, rows]) => {
        const { xValues } = rows[0];
        const yGrouped: Record<string, any> = {};

        rows.forEach(({ row }) => {
          const arr = this.axisValues(normalizedPivotConfig.y, resultIndex)(row).map(yValues => ({ yValues, row }));
          arr.forEach((res) => {
            yGrouped[this.axisValuesString(res.yValues)] = res;
          });
        });

        return {
          xValues,
          yValuesArray: unnest(allYValues.map(yValues => {
            const measure = measureOnX ?
              ResultSet.measureFromAxis(xValues) :
              ResultSet.measureFromAxis(yValues);

            return [[yValues, measureValue((yGrouped[this.axisValuesString(yValues)] ||
              ({ row: {} })).row, measure)]];
          }))
        };
      });
    };

    const pivots = this.loadResponses.length > 1
      ? this.loadResponses.map((_, index) => pivotImpl(index))
      : [];

    return pivots.length
      ? this.mergePivots(pivots, normalizedPivotConfig.joinDateRange || false)
      : pivotImpl();
  }

  private mergePivots(pivots: Pivot[][], joinDateRange: ((pivots: Pivot, joinDateRange: any) => PivotRow[]) | false): PivotRow[] {
    const minLengthPivot: Pivot[] = pivots.reduce<Pivot[] | null>(
      (memo, current) => (memo != null && current.length >= memo.length ? memo : current), null
    ) || [];

    return minLengthPivot.map((_: any, index: number) => {
      const xValues = joinDateRange
        ? [pivots.map((pivot) => pivot[index]?.xValues || []).join(', ')]
        : minLengthPivot[index].xValues;

      return {
        xValues,
        yValuesArray: unnest(pivots.map((pivot) => pivot[index].yValuesArray))
      };
    });
  }

  /**
   * Returns normalized query result data in the following format.
   *
   * You can find the examples of using the `pivotConfig` [here](#types-pivot-config)
   * ```js
   * // For the query
   * {
   *   measures: ['Stories.count'],
   *   timeDimensions: [{
   *     dimension: 'Stories.time',
   *     dateRange: ['2015-01-01', '2015-12-31'],
   *     granularity: 'month'
   *   }]
   * }
   *
   * // ResultSet.chartPivot() will return
   * [
   *   { "x":"2015-01-01T00:00:00", "Stories.count": 27120, "xValues": ["2015-01-01T00:00:00"] },
   *   { "x":"2015-02-01T00:00:00", "Stories.count": 25861, "xValues": ["2015-02-01T00:00:00"]  },
   *   { "x":"2015-03-01T00:00:00", "Stories.count": 29661, "xValues": ["2015-03-01T00:00:00"]  },
   *   //...
   * ]
   *
   * ```
   * When using `chartPivot()` or `seriesNames()`, you can pass `aliasSeries` in the [pivotConfig](#types-pivot-config)
   * to give each series a unique prefix. This is useful for `blending queries` which use the same measure multiple times.
   *
   * ```js
   * // For the queries
   * {
   *   measures: ['Stories.count'],
   *   timeDimensions: [
   *     {
   *       dimension: 'Stories.time',
   *       dateRange: ['2015-01-01', '2015-12-31'],
   *       granularity: 'month',
   *     },
   *   ],
   * },
   * {
   *   measures: ['Stories.count'],
   *   timeDimensions: [
   *     {
   *       dimension: 'Stories.time',
   *       dateRange: ['2015-01-01', '2015-12-31'],
   *       granularity: 'month',
   *     },
   *   ],
   *   filters: [
   *     {
   *       member: 'Stores.read',
   *       operator: 'equals',
   *       value: ['true'],
   *     },
   *   ],
   * },
   *
   * // ResultSet.chartPivot({ aliasSeries: ['one', 'two'] }) will return
   * [
   *   {
   *     x: '2015-01-01T00:00:00',
   *     'one,Stories.count': 27120,
   *     'two,Stories.count': 8933,
   *     xValues: ['2015-01-01T00:00:00'],
   *   },
   *   {
   *     x: '2015-02-01T00:00:00',
   *     'one,Stories.count': 25861,
   *     'two,Stories.count': 8344,
   *     xValues: ['2015-02-01T00:00:00'],
   *   },
   *   {
   *     x: '2015-03-01T00:00:00',
   *     'one,Stories.count': 29661,
   *     'two,Stories.count': 9023,
   *     xValues: ['2015-03-01T00:00:00'],
   *   },
   *   //...
   * ]
   * ```
   */
  public chartPivot(pivotConfig?: PivotConfig): ChartPivotRow[] {
    const validate = (value: string) => {
      if (this.parseDateMeasures && LocalDateRegex.test(value)) {
        return new Date(value);
      } else if (!Number.isNaN(Number.parseFloat(value))) {
        return Number.parseFloat(value);
      }

      return value;
    };

    const duplicateMeasures = new Set<string>();
    if (this.queryType === QUERY_TYPE.BLENDING_QUERY) {
      const allMeasures = flatten(this.loadResponses.map(({ query }) => query.measures ?? []));
      allMeasures.filter((e, i, a) => a.indexOf(e) !== i).forEach(m => duplicateMeasures.add(m));
    }

    return this.pivot(pivotConfig).map(({ xValues, yValuesArray }) => {
      const yValuesMap: Record<string, number | string | Date> = {};

      yValuesArray
        .forEach(([yValues, m]: [string[], string], i: number) => {
          yValuesMap[this.axisValuesString(aliasSeries(yValues, i, pivotConfig, duplicateMeasures), ',')] = m && validate(m);
        });

      return ({
        x: this.axisValuesString(xValues, ','),
        xValues,
        ...yValuesMap
      } as ChartPivotRow);
    });
  }

  /**
   * Returns normalized query result data prepared for visualization in the table format.
   *
   * You can find the examples of using the `pivotConfig` [here](#types-pivot-config)
   *
   * For example:
   * ```js
   * // For the query
   * {
   *   measures: ['Stories.count'],
   *   timeDimensions: [{
   *     dimension: 'Stories.time',
   *     dateRange: ['2015-01-01', '2015-12-31'],
   *     granularity: 'month'
   *   }]
   * }
   *
   * // ResultSet.tablePivot() will return
   * [
   *   { "Stories.time": "2015-01-01T00:00:00", "Stories.count": 27120 },
   *   { "Stories.time": "2015-02-01T00:00:00", "Stories.count": 25861 },
   *   { "Stories.time": "2015-03-01T00:00:00", "Stories.count": 29661 },
   *   //...
   * ]
   * ```
   * @returns An array of pivoted rows
   */
  public tablePivot(pivotConfig?: PivotConfig): Array<{ [key: string]: string | number | boolean }> {
    const normalizedPivotConfig = this.normalizePivotConfig(pivotConfig || {});
    const isMeasuresPresent = normalizedPivotConfig.x.concat(normalizedPivotConfig.y).includes('measures');

    return this.pivot(normalizedPivotConfig).map(({ xValues, yValuesArray }) => fromPairs(
      [
        ...(normalizedPivotConfig.x).map((key, index): [string, string | number] => [
          key,
          xValues[index]
        ]),
        ...(isMeasuresPresent
          ? yValuesArray.map(([yValues, measure]): [string, string | number] => [
            yValues.length ? yValues.join() : 'value',
            measure
          ])
          : [])
      ]
    ));
  }

  /**
   * Returns an array of column definitions for `tablePivot`.
   *
   * For example:
   * ```js
   * // For the query
   * {
   *   measures: ['Stories.count'],
   *   timeDimensions: [{
   *     dimension: 'Stories.time',
   *     dateRange: ['2015-01-01', '2015-12-31'],
   *     granularity: 'month'
   *   }]
   * }
   *
   * // ResultSet.tableColumns() will return
   * [
   *   {
   *     key: 'Stories.time',
   *     dataIndex: 'Stories.time',
   *     title: 'Stories Time',
   *     shortTitle: 'Time',
   *     type: 'time',
   *     format: undefined,
   *   },
   *   {
   *     key: 'Stories.count',
   *     dataIndex: 'Stories.count',
   *     title: 'Stories Count',
   *     shortTitle: 'Count',
   *     type: 'count',
   *     format: undefined,
   *   },
   *   //...
   * ]
   * ```
   *
   * In case we want to pivot the table axes
   * ```js
   * // Let's take this query as an example
   * {
   *   measures: ['Orders.count'],
   *   dimensions: ['Users.country', 'Users.gender']
   * }
   *
   * // and put the dimensions on `y` axis
   * resultSet.tableColumns({
   *   x: [],
   *   y: ['Users.country', 'Users.gender', 'measures']
   * })
   * ```
   *
   * then `tableColumns` will group the table head and return
   * ```js
   * {
   *   key: 'Germany',
   *   type: 'string',
   *   title: 'Users Country Germany',
   *   shortTitle: 'Germany',
   *   meta: undefined,
   *   format: undefined,
   *   children: [
   *     {
   *       key: 'male',
   *       type: 'string',
   *       title: 'Users Gender male',
   *       shortTitle: 'male',
   *       meta: undefined,
   *       format: undefined,
   *       children: [
   *         {
   *           // ...
   *           dataIndex: 'Germany.male.Orders.count',
   *           shortTitle: 'Count',
   *         },
   *       ],
   *     },
   *     {
   *       // ...
   *       shortTitle: 'female',
   *       children: [
   *         {
   *           // ...
   *           dataIndex: 'Germany.female.Orders.count',
   *           shortTitle: 'Count',
   *         },
   *       ],
   *     },
   *   ],
   * },
   * // ...
   * ```
   * @returns An array of columns
   */
  public tableColumns(pivotConfig?: PivotConfig): TableColumn[] {
    const normalizedPivotConfig = this.normalizePivotConfig(pivotConfig || {});

    const annotations: QueryAnnotations = this.loadResponses
      .map((r) => r.annotation)
      .reduce<QueryAnnotations>((acc, annotation) => mergeDeepLeft(acc, annotation) as QueryAnnotations,
        {
          dimensions: {},
          measures: {},
          timeDimensions: {},
          segments: {},
        });

    const flatMeta = Object.values(annotations).reduce((a, b) => ({ ...a, ...b }), {});
    const schema: Record<string, any> = {};

    const extractFields = (key: string) => {
      const { title, shortTitle, type, format, meta } = flatMeta[key] || {};

      return {
        key,
        title,
        shortTitle,
        type,
        format,
        meta
      };
    };

    const pivot = this.pivot(normalizedPivotConfig);

    (pivot[0]?.yValuesArray || []).forEach(([yValues]) => {
      if (yValues.length > 0) {
        let currentItem = schema;

        yValues.forEach((value, index) => {
          currentItem[`_${value}`] = {
            key: value,
            memberId: normalizedPivotConfig.y[index] === 'measures'
              ? value
              : normalizedPivotConfig.y[index],
            children: currentItem[`_${value}`]?.children || {}
          };

          currentItem = currentItem[`_${value}`].children;
        });
      }
    });

    const toColumns = (item: Record<string, any> = {}, path: string[] = []): TableColumn[] => {
      if (Object.keys(item).length === 0) {
        return [];
      }

      return Object.values(item).map(({ key, ...currentItem }) => {
        const children = toColumns(currentItem.children, [
          ...path,
          key
        ]);

        const { title, shortTitle, ...fields } = extractFields(currentItem.memberId);

        const dimensionValue = key !== currentItem.memberId || title == null ? key : '';

        if (!children.length) {
          return {
            ...fields,
            key,
            dataIndex: [...path, key].join(),
            title: [title, dimensionValue].join(' ').trim(),
            shortTitle: dimensionValue || shortTitle,
          } as TableColumn;
        }

        return {
          ...fields,
          key,
          title: [title, dimensionValue].join(' ').trim(),
          shortTitle: dimensionValue || shortTitle,
          children,
        } as TableColumn;
      });
    };

    let otherColumns: TableColumn[] = [];

    if (!pivot.length && normalizedPivotConfig.y.includes('measures')) {
      otherColumns = (this.loadResponses[0].query.measures || []).map(
        (key) => ({ ...extractFields(key), dataIndex: key })
      );
    }

    // Synthetic column to display the measure value
    if (!normalizedPivotConfig.y.length && normalizedPivotConfig.x.includes('measures')) {
      otherColumns.push({
        key: 'value',
        dataIndex: 'value',
        title: 'Value',
        shortTitle: 'Value',
        type: 'string',
      });
    }

    return (normalizedPivotConfig.x).map((key) => {
      if (key === 'measures') {
        return {
          key: 'measures',
          dataIndex: 'measures',
          title: 'Measures',
          shortTitle: 'Measures',
          type: 'string',
        } as TableColumn;
      }

      return ({ ...extractFields(key), dataIndex: key });
    })
      .concat(toColumns(schema))
      .concat(otherColumns);
  }

  public totalRow(pivotConfig?: PivotConfig): ChartPivotRow {
    return this.chartPivot(pivotConfig)[0];
  }

  public categories(pivotConfig?: PivotConfig): ChartPivotRow[] {
    return this.chartPivot(pivotConfig);
  }

  /**
   * Returns an array of series objects, containing `key` and `title` parameters.
   * ```js
   * // For query
   * {
   *   measures: ['Stories.count'],
   *   timeDimensions: [{
   *     dimension: 'Stories.time',
   *     dateRange: ['2015-01-01', '2015-12-31'],
   *     granularity: 'month'
   *   }]
   * }
   *
   * // ResultSet.seriesNames() will return
   * [
   *   {
   *     key: 'Stories.count',
   *     title: 'Stories Count',
   *     shortTitle: 'Count',
   *     yValues: ['Stories.count'],
   *   },
   * ]
   * ```
   * @returns An array of series names
   */
  public seriesNames(pivotConfig?: PivotConfig): SeriesNamesColumn[] {
    const normalizedPivotConfig = this.normalizePivotConfig(pivotConfig);
    const measures = this.loadResponses
      .map(r => r.annotation.measures)
      .reduce((acc, m) => ({ ...acc, ...m }), {});

    const seriesNames = unnest(this.loadResponses.map((_, index) => pipe(
      map(this.axisValues(normalizedPivotConfig.y, index)),
      unnest,
      uniq
    )(
      this.timeDimensionBackwardCompatibleData(index)
    )));

    const duplicateMeasures = new Set<string>();
    if (this.queryType === QUERY_TYPE.BLENDING_QUERY) {
      const allMeasures = flatten(this.loadResponses.map(({ query }) => query.measures ?? []));
      allMeasures.filter((e, i, a) => a.indexOf(e) !== i).forEach(m => duplicateMeasures.add(m));
    }

    return seriesNames.map((axisValues, i) => {
      const aliasedAxis = aliasSeries(axisValues, i, normalizedPivotConfig, duplicateMeasures);
      return {
        title: this.axisValuesString(
          normalizedPivotConfig.y.find(d => d === 'measures') ?
            dropLast(1, aliasedAxis).concat(
              measures[
                ResultSet.measureFromAxis(axisValues)
              ].title
            ) :
            aliasedAxis, ', '
        ),
        shortTitle: this.axisValuesString(
          normalizedPivotConfig.y.find(d => d === 'measures') ?
            dropLast(1, aliasedAxis).concat(
              measures[
                ResultSet.measureFromAxis(axisValues)
              ].shortTitle
            ) :
            aliasedAxis, ', '
        ),
        key: this.axisValuesString(aliasedAxis, ','),
        yValues: axisValues
      };
    });
  }

  public query(): Query {
    if (this.queryType !== QUERY_TYPE.REGULAR_QUERY) {
      throw new Error(`Method is not supported for a '${this.queryType}' query type. Please use decompose`);
    }

    return this.loadResponses[0].query;
  }

  public pivotQuery(): PivotQuery {
    return this.loadResponse.pivotQuery || null;
  }

  /**
   * @return the total number of rows if the `total` option was set, when sending the query
   */
  public totalRows(): number | null | undefined {
    return this.loadResponses[0].total;
  }

  public rawData(): T[] {
    if (this.queryType !== QUERY_TYPE.REGULAR_QUERY) {
      throw new Error(`Method is not supported for a '${this.queryType}' query type. Please use decompose`);
    }

    return this.loadResponses[0].data;
  }

  public annotation(): QueryAnnotations {
    if (this.queryType !== QUERY_TYPE.REGULAR_QUERY) {
      throw new Error(`Method is not supported for a '${this.queryType}' query type. Please use decompose`);
    }

    return this.loadResponses[0].annotation;
  }

  private timeDimensionBackwardCompatibleData(resultIndex: number) {
    if (resultIndex === undefined) {
      throw new Error('resultIndex is required');
    }

    if (!this.backwardCompatibleData[resultIndex]) {
      const { data, query } = this.loadResponses[resultIndex];
      const timeDimensions = (query.timeDimensions || []).filter(td => Boolean(td.granularity));

      this.backwardCompatibleData[resultIndex] = data.map(row => (
        {
          ...row,
          ...(
            fromPairs(Object.keys(row)
              .filter(
                field => {
                  const foundTd = timeDimensions.find(d => d.dimension === field);
                  return foundTd && !row[ResultSet.timeDimensionMember(foundTd)];
                }
              ).map(field => (
                [ResultSet.timeDimensionMember(timeDimensions.find(d => d.dimension === field)!), row[field]]
              )))
          )
        }
      ));
    }

    return this.backwardCompatibleData[resultIndex];
  }

  /**
   * Can be used when you need access to the methods that can't be used with some query types (eg `compareDateRangeQuery` or `blendingQuery`)
   * ```js
   * resultSet.decompose().forEach((currentResultSet) => {
   *   console.log(currentResultSet.rawData());
   * });
   * ```
   */
  public decompose(): ResultSet<any>[] {
    return this.loadResponses.map((result) => new ResultSet({
      queryType: QUERY_TYPE.REGULAR_QUERY,
      pivotQuery: {
        ...result.query,
        queryType: QUERY_TYPE.REGULAR_QUERY,
      },
      results: [result]
    }, this.options));
  }

  /**
   * Can be used to stash the `ResultSet` in a storage and restored later with [deserialize](#result-set-deserialize)
   */
  public serialize(): SerializedResult {
    return {
      loadResponse: clone(this.loadResponse)
    };
  }
}
