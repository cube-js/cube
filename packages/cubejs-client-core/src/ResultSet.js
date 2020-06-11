/**
 * @module @cubejs-client/core
 */

import {
  groupBy, pipe, toPairs, uniq, filter, map, unnest, dropLast, equals, reduce, minBy, maxBy
} from 'ramda';
import Moment from 'moment';
import momentRange from 'moment-range';

const moment = momentRange.extendMoment(Moment);

const TIME_SERIES = {
  day: (range) => Array.from(range.by('day'))
    .map(d => d.format('YYYY-MM-DDT00:00:00.000')),
  month: (range) => Array.from(range.snapTo('month').by('month'))
    .map(d => d.format('YYYY-MM-01T00:00:00.000')),
  year: (range) => Array.from(range.snapTo('year').by('year'))
    .map(d => d.format('YYYY-01-01T00:00:00.000')),
  hour: (range) => Array.from(range.by('hour'))
    .map(d => d.format('YYYY-MM-DDTHH:00:00.000')),
  minute: (range) => Array.from(range.by('minute'))
    .map(d => d.format('YYYY-MM-DDTHH:mm:00.000')),
  second: (range) => Array.from(range.by('second'))
    .map(d => d.format('YYYY-MM-DDTHH:mm:ss.000')),
  week: (range) => Array.from(range.snapTo('isoweek').by('week'))
    .map(d => d.startOf('isoweek').format('YYYY-MM-DDT00:00:00.000'))
};

const DateRegex = /^\d\d\d\d-\d\d-\d\d$/;
const LocalDateRegex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z?$/;

/**
 * Provides a convenient interface for data manipulation.
 */
class ResultSet {
  constructor(loadResponse, options) {
    options = options || {};
    this.loadResponse = loadResponse;
    this.parseDateMeasures = options.parseDateMeasures;
  }

  /**
   * Returns a measure drill down query.
   *
   * Provided you have a measure with the defined `drillMemebers` on the `Orders` cube
   *
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
   *
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
   *
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
   * @param {{ xValues: [], yValues: [] }} drillDownLocator
   * @param {Object} pivotConfig - See {@link ResultSet#pivot}.
   * @returns {Object|null} Drill down query
   */
  drillDown(drillDownLocator, pivotConfig) {
    const { xValues = [], yValues = [] } = drillDownLocator;
    const normalizedPivotConfig = this.normalizePivotConfig(pivotConfig);

    const values = [];
    normalizedPivotConfig.x.forEach((member, currentIndex) => values.push([member, xValues[currentIndex]]));
    normalizedPivotConfig.y.forEach((member, currentIndex) => values.push([member, yValues[currentIndex]]));

    const { measures } = this.loadResponse.annotation;
    let [, measureName] = values.find(([member]) => member === 'measues') || [];

    if (measureName === undefined) {
      [measureName] = Object.keys(measures);
    }

    if (!(measures[measureName] && measures[measureName].drillMembers || []).length) {
      return null;
    }

    const filters = [{
      dimension: measureName,
      operator: 'measureFilter',
    }];
    const timeDimensions = [];

    values.filter(([member]) => member !== 'measures')
      .forEach(([member, value]) => {
        const [cubeName, dimension, granularity] = member.split('.');

        if (granularity !== undefined) {
          const range = moment.range(value, value).snapTo(
            granularity
          );

          timeDimensions.push({
            dimension: [cubeName, dimension].join('.'),
            dateRange: [
              range.start,
              range.end
            ].map((dt) => dt.format(moment.HTML5_FMT.DATETIME_LOCAL_MS)),
          });
        } else {
          filters.push({
            member,
            operator: 'equals',
            values: [value.toString()],
          });
        }
      });

    return {
      ...measures[measureName].drillMembersGrouped,
      filters,
      timeDimensions
    };
  }

  /**
   * Returns an array of series with key, title and series data.
   *
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
   * // ResultSet.series() will return
   * [
   *   {
   *     "key":"Stories.count",
   *     "title": "Stories Count",
   *     "series": [
   *       { "x":"2015-01-01T00:00:00", "value": 27120 },
   *       { "x":"2015-02-01T00:00:00", "value": 25861 },
   *       { "x": "2015-03-01T00:00:00", "value": 29661 },
   *       //...
   *     ]
   *   }
   * ]
   * ```
   * @param pivotConfig - See {@link ResultSet#pivot}.
   * @returns {Array}
   */
  series(pivotConfig) {
    return this.seriesNames(pivotConfig).map(({ title, key }) => ({
      title,
      key,
      series: this.chartPivot(pivotConfig).map(({ category, x, ...obj }) => ({ value: obj[key], category, x }))
    }));
  }

  axisValues(axis) {
    const { query } = this.loadResponse;
    return row => {
      const value = (measure) => axis.filter(d => d !== 'measures')
        .map(d => (row[d] != null ? row[d] : null)).concat(measure ? [measure] : []);
      if (axis.find(d => d === 'measures') && (query.measures || []).length) {
        return query.measures.map(value);
      }
      return [value()];
    };
  }

  axisValuesString(axisValues, delimiter) {
    const formatValue = (v) => {
      if (v == null) {
        return '∅';
      } else if (v === '') {
        return '[Empty string]';
      } else {
        return v;
      }
    };
    return axisValues.map(formatValue).join(delimiter || ', ');
  }

  static timeDimensionMember(td) {
    return `${td.dimension}.${td.granularity}`;
  }

  normalizePivotConfig(pivotConfig) {
    const { query } = this.loadResponse;
    const timeDimensions = (query.timeDimensions || []).filter(td => !!td.granularity);
    const dimensions = query.dimensions || [];
    pivotConfig = pivotConfig || (timeDimensions.length ? {
      x: timeDimensions.map(td => ResultSet.timeDimensionMember(td)),
      y: dimensions
    } : {
      x: dimensions,
      y: []
    });

    const substituteTimeDimensionMembers = axis => axis.map(
      subDim => (
        (
          timeDimensions.find(td => td.dimension === subDim) &&
          !dimensions.find(d => d === subDim)
        ) ?
          ResultSet.timeDimensionMember(query.timeDimensions.find(td => td.dimension === subDim)) :
          subDim
      )
    );

    pivotConfig.x = substituteTimeDimensionMembers(pivotConfig.x || []);
    pivotConfig.y = substituteTimeDimensionMembers(pivotConfig.y || []);

    const allIncludedDimensions = pivotConfig.x.concat(pivotConfig.y);
    const allDimensions = timeDimensions.map(td => ResultSet.timeDimensionMember(td)).concat(dimensions);
    pivotConfig.x = pivotConfig.x.concat(allDimensions.filter(d => allIncludedDimensions.indexOf(d) === -1));
    if (!pivotConfig.x.concat(pivotConfig.y).find(d => d === 'measures')) {
      pivotConfig.y = pivotConfig.y.concat(['measures']);
    }
    if (!(query.measures || []).length) {
      pivotConfig.x = pivotConfig.x.filter(d => d !== 'measures');
      pivotConfig.y = pivotConfig.y.filter(d => d !== 'measures');
    }
    if (pivotConfig.fillMissingDates == null) {
      pivotConfig.fillMissingDates = true;
    }
    return pivotConfig;
  }

  static measureFromAxis(axisValues) {
    return axisValues[axisValues.length - 1];
  }

  timeSeries(timeDimension) {
    if (!timeDimension.granularity) {
      return null;
    }
    let { dateRange } = timeDimension;
    if (!dateRange) {
      const dates = pipe(
        map(
          row => row[ResultSet.timeDimensionMember(timeDimension)] &&
            moment(row[ResultSet.timeDimensionMember(timeDimension)])
        ),
        filter(r => !!r)
      )(this.timeDimensionBackwardCompatibleData());

      dateRange = dates.length && [
        reduce(minBy(d => d.toDate()), dates[0], dates),
        reduce(maxBy(d => d.toDate()), dates[0], dates)
      ] || null;
    }

    if (!dateRange) {
      return null;
    }

    const padToDay = timeDimension.dateRange ?
      timeDimension.dateRange.find(d => d.match(DateRegex)) :
      !['hour', 'minute', 'second'].includes(timeDimension.granularity);

    const [start, end] = dateRange;
    const range = moment.range(start, end);

    if (!TIME_SERIES[timeDimension.granularity]) {
      throw new Error(`Unsupported time granularity: ${timeDimension.granularity}`);
    }

    return TIME_SERIES[timeDimension.granularity](
      padToDay ? range.snapTo('day') : range
    );
  }

  /**
   * Base method for pivoting {@link ResultSet} data.
   * Most of the times shouldn't be used directly and {@link ResultSet#chartPivot} or {@link ResultSet#tablePivot}
   * should be used instead.
   *
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
   *       ['Stories.count', 27120]
   *     ]
   *   },
   *   {
   *     xValues: ["2015-02-01T00:00:00"],
   *     yValuesArray: [
   *       ['Stories.count', 25861]
   *     ]
   *   },
   *   {
   *     xValues: ["2015-03-01T00:00:00"],
   *     yValuesArray: [
   *       ['Stories.count', 29661]
   *     ]
   *   }
   * ]
   * ```
   * @param [pivotConfig] - Configuration object that contains information about pivot axes and other options
   * @param {Array} pivotConfig.x - dimensions to put on **x** or **rows** axis. Put `measures` at the end of array here
   * to show measures in rows instead of columns.
   * @param {Array} pivotConfig.y - dimensions to put on **y** or **columns** axis.
   * @param {Boolean} [pivotConfig.fillMissingDates=true] - if `true` missing dates on time dimensions will be filled
   * with `0` for all measures.
   * @returns {Array} of pivoted rows.
   */
  pivot(pivotConfig) {
    pivotConfig = this.normalizePivotConfig(pivotConfig);
    let groupByXAxis = groupBy(({ xValues }) => this.axisValuesString(xValues));

    // eslint-disable-next-line no-unused-vars
    let measureValue = (row, measure, xValues) => row[measure];

    if (
      pivotConfig.fillMissingDates &&
      pivotConfig.x.length === 1 &&
      equals(
        pivotConfig.x,
        (this.loadResponse.query.timeDimensions || [])
          .filter(td => !!td.granularity)
          .map(td => ResultSet.timeDimensionMember(td))
      )
    ) {
      const series = this.timeSeries(this.loadResponse.query.timeDimensions[0]);
      if (series) {
        groupByXAxis = (rows) => {
          const byXValues = groupBy(
            ({ xValues }) => moment(xValues[0]).format(moment.HTML5_FMT.DATETIME_LOCAL_MS),
            rows
          );
          return series.map(d => ({ [d]: byXValues[d] || [{ xValues: [d], row: {} }] }))
            .reduce((a, b) => Object.assign(a, b), {});
        };

        // eslint-disable-next-line no-unused-vars
        measureValue = (row, measure, xValues) => row[measure] || 0;
      }
    }

    const xGrouped = pipe(
      map(row => this.axisValues(pivotConfig.x)(row).map(xValues => ({ xValues, row }))),
      unnest,
      groupByXAxis,
      toPairs
    )(this.timeDimensionBackwardCompatibleData());

    const allYValues = pipe(
      map(
        // eslint-disable-next-line no-unused-vars
        ([xValuesString, rows]) => unnest(
          // collect Y values only from filled rows
          rows.filter(({ row }) => Object.keys(row).length > 0).map(({ row }) => this.axisValues(pivotConfig.y)(row))
        )
      ),
      unnest,
      uniq
    )(xGrouped);

    // eslint-disable-next-line no-unused-vars
    return xGrouped.map(([xValuesString, rows]) => {
      const { xValues } = rows[0];
      const yGrouped = pipe(
        map(({ row }) => this.axisValues(pivotConfig.y)(row).map(yValues => ({ yValues, row }))),
        unnest,
        groupBy(({ yValues }) => this.axisValuesString(yValues))
      )(rows);
      return {
        xValues,
        yValuesArray: unnest(allYValues.map(yValues => {
          const measure = pivotConfig.x.find(d => d === 'measures') ?
            ResultSet.measureFromAxis(xValues) :
            ResultSet.measureFromAxis(yValues);
          return (yGrouped[this.axisValuesString(yValues)] ||
            [{ row: {} }]).map(({ row }) => [yValues, measureValue(row, measure, xValues)]);
        }))
      };
    });
  }

  pivotedRows(pivotConfig) { // TODO
    return this.chartPivot(pivotConfig);
  }

  /**
   * Returns normalized query result data in the following format.
   *
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
   * // ResultSet.chartPivot() will return
   * [
   *   { "x":"2015-01-01T00:00:00", "Stories.count": 27120 },
   *   { "x":"2015-02-01T00:00:00", "Stories.count": 25861 },
   *   { "x": "2015-03-01T00:00:00", "Stories.count": 29661 },
   *   //...
   * ]
   * ```
   * @param pivotConfig - See {@link ResultSet#pivot}.
   */
  chartPivot(pivotConfig) {
    const validate = (value) => {
      if (this.parseDateMeasures && LocalDateRegex.test(value)) {
        return new Date(value);
      } else if (!Number.isNaN(Number.parseFloat(value))) {
        return Number.parseFloat(value);
      }

      return value;
    };

    return this.pivot(pivotConfig).map(({ xValues, yValuesArray }) => ({
      category: this.axisValuesString(xValues, ', '), // TODO deprecated
      x: this.axisValuesString(xValues, ', '),
      xValues,
      ...(
        yValuesArray
          .map(([yValues, m]) => ({
            [this.axisValuesString(yValues, ', ')]: m && validate(m),
          }))
          .reduce((a, b) => Object.assign(a, b), {})
      )
    }));
  }

  /**
   * Returns normalized query result data prepared for visualization in the table format.
   *
   * For example
   *
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
   * // ResultSet.tablePivot() will return
   * [
   *   { "Stories.time": "2015-01-01T00:00:00", "Stories.count": 27120 },
   *   { "Stories.time": "2015-02-01T00:00:00", "Stories.count": 25861 },
   *   { "Stories.time": "2015-03-01T00:00:00", "Stories.count": 29661 },
   *   //...
   * ]
   * ```
   * @param pivotConfig - See {@link ResultSet#pivot}
   * @returns {Array} of pivoted rows
   */
  tablePivot(pivotConfig) {
    const normalizedPivotConfig = this.normalizePivotConfig(pivotConfig || {});
    const valueToObject =
      (valuesArray, measureValue) => (
        (field, index) => ({
          [field === 'measures' ? valuesArray[index] : field]: field === 'measures' ? measureValue : valuesArray[index]
        })
      );

    return this.pivot(normalizedPivotConfig).map(({ xValues, yValuesArray }) => (
      yValuesArray.map(([yValues, m]) => (
        normalizedPivotConfig.x.map(valueToObject(xValues, m))
          .concat(normalizedPivotConfig.y.map(valueToObject(yValues, m)))
          .reduce((a, b) => Object.assign(a, b), {})
      )).reduce((a, b) => Object.assign(a, b), {})
    ));
  }

  /**
   * Returns array of column definitions for `tablePivot`.
   *
   * For example
   *
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
   * // ResultSet.tableColumns() will return
   * [
   *   { key: "Stories.time", title: "Stories Time", shortTitle: "Time", type: "time", format: undefined },
   *   { key: "Stories.count", title: "Stories Count", shortTitle: "Count", type: "count", format: undefined },
   *   //...
   * ]
   * ```
   * @param pivotConfig - See {@link ResultSet#pivot}.
   * @returns {Array} of columns
   */
  tableColumns(pivotConfig) {
    const normalizedPivotConfig = this.normalizePivotConfig(pivotConfig);

    const column = (field) => {
      const exractFields = (annotation = {}) => {
        const {
          title,
          shortTitle,
          format,
          type,
          meta
        } = annotation;

        return {
          title,
          shortTitle,
          format,
          type,
          meta
        };
      };

      return field === 'measures' ? (this.query().measures || []).map((key) => ({
        key,
        ...exractFields(this.loadResponse.annotation.measures[key])
      })) : [
        {
          key: field,
          ...exractFields(this.loadResponse.annotation.dimensions[field] ||
              this.loadResponse.annotation.timeDimensions[field])
        },
      ];
    };

    return normalizedPivotConfig.x.map(column)
      .concat(normalizedPivotConfig.y.map(column))
      .reduce((a, b) => a.concat(b));
  }

  totalRow() {
    return this.chartPivot()[0];
  }

  categories(pivotConfig) { // TODO
    return this.chartPivot(pivotConfig);
  }

  /**
   * Returns the array of series objects, containing `key` and `title` parameters.
   *
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
   * { "key":"Stories.count", "title": "Stories Count" }
   * ]
   * ```
   * @param pivotConfig - See {@link ResultSet#pivot}.
   * @returns {Array} of series names
   */
  seriesNames(pivotConfig) {
    pivotConfig = this.normalizePivotConfig(pivotConfig);

    return pipe(map(this.axisValues(pivotConfig.y)), unnest, uniq)(
      this.timeDimensionBackwardCompatibleData()
    ).map(axisValues => ({
      title: this.axisValuesString(
        pivotConfig.y.find(d => d === 'measures') ?
          dropLast(1, axisValues).concat(
            this.loadResponse.annotation.measures[
              ResultSet.measureFromAxis(axisValues)
            ].title
          ) :
          axisValues, ', '
      ),
      key: this.axisValuesString(axisValues),
      yValues: axisValues
    }));
  }

  query() {
    return this.loadResponse.query;
  }

  rawData() {
    return this.loadResponse.data;
  }

  timeDimensionBackwardCompatibleData() {
    if (!this.backwardCompatibleData) {
      const { query } = this.loadResponse;
      const timeDimensions = (query.timeDimensions || []).filter(td => !!td.granularity);
      this.backwardCompatibleData = this.loadResponse.data.map(row => (
        {
          ...row,
          ...(
            Object.keys(row)
              .filter(
                field => timeDimensions.find(d => d.dimension === field) &&
                  !row[ResultSet.timeDimensionMember(timeDimensions.find(d => d.dimension === field))]
              ).map(field => ({
                [ResultSet.timeDimensionMember(timeDimensions.find(d => d.dimension === field))]: row[field]
              })).reduce((a, b) => ({ ...a, ...b }), {})
          )
        }
      ));
    }
    return this.backwardCompatibleData;
  }
}

export default ResultSet;
