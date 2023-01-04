import dayjs from 'dayjs';
import quarterOfYear from 'dayjs/plugin/quarterOfYear';

import en from 'dayjs/locale/en';
import {
  groupBy, pipe, fromPairs, uniq, filter, map, dropLast, equals, reduce, minBy, maxBy, clone, mergeDeepLeft,
  pluck, mergeAll, flatten,
} from 'ramda';

import { aliasSeries } from './utils';

dayjs.extend(quarterOfYear);

// When granularity is week, weekStart Value must be 1. However, since the client can change it globally (https://day.js.org/docs/en/i18n/changing-locale)
// So the function below has been added.
const internalDayjs = (...args) => dayjs(...args).locale({ ...en, weekStart: 1 });

export const TIME_SERIES = {
  day: (range) => range.by('d').map(d => d.format('YYYY-MM-DDT00:00:00.000')),
  month: (range) => range.snapTo('month').by('M').map(d => d.format('YYYY-MM-01T00:00:00.000')),
  year: (range) => range.snapTo('year').by('y').map(d => d.format('YYYY-01-01T00:00:00.000')),
  hour: (range) => range.by('h').map(d => d.format('YYYY-MM-DDTHH:00:00.000')),
  minute: (range) => range.by('m').map(d => d.format('YYYY-MM-DDTHH:mm:00.000')),
  second: (range) => range.by('s').map(d => d.format('YYYY-MM-DDTHH:mm:ss.000')),
  week: (range) => range.snapTo('week').by('w').map(d => d.startOf('week').format('YYYY-MM-DDT00:00:00.000')),
  quarter: (range) => range.snapTo('quarter').by('quarter').map(d => d.startOf('quarter').format('YYYY-MM-DDT00:00:00.000')),
};

const DateRegex = /^\d\d\d\d-\d\d-\d\d$/;
const LocalDateRegex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z?$/;

const groupByToPairs = (keyFn) => {
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

const unnest = (arr) => {
  const res = [];
  arr.forEach((subArr) => {
    subArr.forEach(element => res.push(element));
  });

  return res;
};

export const dayRange = (from, to) => ({
  by: (value) => {
    const results = [];

    let start = internalDayjs(from);
    const end = internalDayjs(to);

    while (start.isBefore(end) || start.isSame(end)) {
      results.push(start);
      start = start.add(1, value);
    }

    return results;
  },
  snapTo: (value) => dayRange(internalDayjs(from).startOf(value), internalDayjs(to).endOf(value)),
  start: internalDayjs(from),
  end: internalDayjs(to),
});

export const QUERY_TYPE = {
  REGULAR_QUERY: 'regularQuery',
  COMPARE_DATE_RANGE_QUERY: 'compareDateRangeQuery',
  BLENDING_QUERY: 'blendingQuery',
};

class ResultSet {
  static measureFromAxis(axisValues) {
    return axisValues[axisValues.length - 1];
  }

  static timeDimensionMember(td) {
    return `${td.dimension}.${td.granularity}`;
  }

  static deserialize(data, options = {}) {
    return new ResultSet(data.loadResponse, options);
  }

  constructor(loadResponse, options = {}) {
    this.loadResponse = loadResponse;

    if (this.loadResponse.queryType != null) {
      this.queryType = loadResponse.queryType;
      this.loadResponses = loadResponse.results;
    } else {
      this.queryType = QUERY_TYPE.REGULAR_QUERY;
      this.loadResponse.pivotQuery = {
        ...loadResponse.query,
        queryType: this.queryType
      };
      this.loadResponses = [loadResponse];
    }

    if (!Object.values(QUERY_TYPE).includes(this.queryType)) {
      throw new Error('Unknown query type');
    }

    this.parseDateMeasures = options.parseDateMeasures;
    this.options = options;

    this.backwardCompatibleData = [];
  }

  drillDown(drillDownLocator, pivotConfig) {
    if (this.queryType === QUERY_TYPE.COMPARE_DATE_RANGE_QUERY) {
      throw new Error('compareDateRange drillDown query is not currently supported');
    }
    if (this.queryType === QUERY_TYPE.BLENDING_QUERY) {
      throw new Error('Data blending drillDown query is not currently supported');
    }

    const { xValues = [], yValues = [] } = drillDownLocator;
    const normalizedPivotConfig = this.normalizePivotConfig(pivotConfig);

    const values = [];
    normalizedPivotConfig.x.forEach((member, currentIndex) => values.push([member, xValues[currentIndex]]));
    normalizedPivotConfig.y.forEach((member, currentIndex) => values.push([member, yValues[currentIndex]]));

    const { filters: parentFilters = [], segments = [] } = this.query();
    const { measures } = this.loadResponses[0].annotation;
    let [, measureName] = values.find(([member]) => member === 'measures') || [];

    if (measureName === undefined) {
      [measureName] = Object.keys(measures);
    }

    if (!(measures[measureName] && measures[measureName].drillMembers || []).length) {
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

          timeDimensions.push({
            dimension: [cubeName, dimension].join('.'),
            dateRange: [
              range.start,
              range.end
            ].map((dt) => dt.format('YYYY-MM-DDTHH:mm:ss.SSS')),
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

    const { query } = this.loadResponses[0];
    if (
      timeDimensions.length === 0 &&
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

  series(pivotConfig) {
    return this.seriesNames(pivotConfig).map(({ title, shortTitle, key }) => ({
      title,
      shortTitle,
      key,
      series: this.chartPivot(pivotConfig).map(({ x, ...obj }) => ({ value: obj[key], x }))
    }));
  }

  axisValues(axis, resultIndex = 0) {
    const { query } = this.loadResponses[resultIndex];

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

  static getNormalizedPivotConfig(query = {}, pivotConfig = null) {
    const defaultPivotConfig = {
      x: [],
      y: [],
      fillMissingDates: true,
      joinDateRange: false
    };

    const {
      measures = [],
      dimensions = []
    } = query;

    const timeDimensions = (query.timeDimensions || []).filter(td => !!td.granularity);

    pivotConfig = pivotConfig || (timeDimensions.length ? {
      x: timeDimensions.map(td => ResultSet.timeDimensionMember(td)),
      y: dimensions
    } : {
      x: dimensions,
      y: []
    });

    pivotConfig = mergeDeepLeft(pivotConfig, defaultPivotConfig);

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

    pivotConfig.x = substituteTimeDimensionMembers(pivotConfig.x);
    pivotConfig.y = substituteTimeDimensionMembers(pivotConfig.y);

    const allIncludedDimensions = pivotConfig.x.concat(pivotConfig.y);
    const allDimensions = timeDimensions.map(td => ResultSet.timeDimensionMember(td)).concat(dimensions);

    const dimensionFilter = (key) => allDimensions.includes(key) || key === 'measures';

    pivotConfig.x = pivotConfig.x.concat(
      allDimensions.filter(d => !allIncludedDimensions.includes(d) && d !== 'compareDateRange')
    )
      .filter(dimensionFilter);
    pivotConfig.y = pivotConfig.y.filter(dimensionFilter);

    if (!pivotConfig.x.concat(pivotConfig.y).find(d => d === 'measures')) {
      pivotConfig.y.push('measures');
    }

    if (dimensions.includes('compareDateRange') && !pivotConfig.y.concat(pivotConfig.x).includes('compareDateRange')) {
      pivotConfig.y.unshift('compareDateRange');
    }

    if (!measures.length) {
      pivotConfig.x = pivotConfig.x.filter(d => d !== 'measures');
      pivotConfig.y = pivotConfig.y.filter(d => d !== 'measures');
    }

    return pivotConfig;
  }

  normalizePivotConfig(pivotConfig) {
    return ResultSet.getNormalizedPivotConfig(this.loadResponse.pivotQuery, pivotConfig);
  }

  timeSeries(timeDimension, resultIndex) {
    if (!timeDimension.granularity) {
      return null;
    }

    let { dateRange } = timeDimension;

    if (!dateRange) {
      const member = ResultSet.timeDimensionMember(timeDimension);
      const dates = pipe(
        map(row => row[member] && internalDayjs(row[member])),
        filter(Boolean)
      )(this.timeDimensionBackwardCompatibleData(resultIndex));

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
    const range = dayRange(start, end);

    if (!TIME_SERIES[timeDimension.granularity]) {
      throw new Error(`Unsupported time granularity: ${timeDimension.granularity}`);
    }

    return TIME_SERIES[timeDimension.granularity](
      padToDay ? range.snapTo('d') : range
    );
  }

  pivot(pivotConfig) {
    pivotConfig = this.normalizePivotConfig(pivotConfig);
    const { pivotQuery: query } = this.loadResponse;

    const pivotImpl = (resultIndex = 0) => {
      let groupByXAxis = groupByToPairs(({ xValues }) => this.axisValuesString(xValues));

      const measureValue = (row, measure) => row[measure] || 0;

      if (
        pivotConfig.fillMissingDates &&
        pivotConfig.x.length === 1 &&
        (equals(
          pivotConfig.x,
          (query.timeDimensions || [])
            .filter(td => Boolean(td.granularity))
            .map(td => ResultSet.timeDimensionMember(td))
        ))
      ) {
        const series = this.loadResponses.map(
          (loadResponse) => this.timeSeries(loadResponse.query.timeDimensions[0], resultIndex)
        );

        if (series[0]) {
          groupByXAxis = (rows) => {
            const byXValues = groupBy(
              ({ xValues }) => xValues[0],
              rows
            );
            return series[resultIndex].map(d => [d, byXValues[d] || [{ xValues: [d], row: {} }]]);
          };
        }
      }

      const xGrouped = pipe(
        map(row => this.axisValues(pivotConfig.x, resultIndex)(row).map(xValues => ({ xValues, row }))),
        unnest,
        groupByXAxis
      )(this.timeDimensionBackwardCompatibleData(resultIndex));

      const yValuesMap = {};
      xGrouped.forEach(([, rows]) => {
        rows.forEach(({ row }) => {
          this.axisValues(pivotConfig.y, resultIndex)(row).forEach((values) => {
            if (Object.keys(row).length > 0) {
              yValuesMap[values.join()] = values;
            }
          });
        });
      });
      const allYValues = Object.values(yValuesMap);

      const measureOnX = Boolean(pivotConfig.x.find(d => d === 'measures'));

      return xGrouped.map(([, rows]) => {
        const { xValues } = rows[0];
        const yGrouped = {};

        rows.forEach(({ row }) => {
          const arr = this.axisValues(pivotConfig.y, resultIndex)(row).map(yValues => ({ yValues, row }));
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
      ? this.mergePivots(pivots, pivotConfig.joinDateRange)
      : pivotImpl();
  }

  mergePivots(pivots, joinDateRange) {
    const minLengthPivot = pivots.reduce(
      (memo, current) => (memo != null && current.length >= memo.length ? memo : current), null
    );

    return minLengthPivot.map((_, index) => {
      const xValues = joinDateRange
        ? [pivots.map((pivot) => pivot[index] && pivot[index].xValues || []).join(', ')]
        : minLengthPivot[index].xValues;

      return {
        xValues,
        yValuesArray: unnest(pivots.map((pivot) => pivot[index].yValuesArray))
      };
    });
  }

  pivotedRows(pivotConfig) { // TODO
    return this.chartPivot(pivotConfig);
  }

  chartPivot(pivotConfig) {
    const validate = (value) => {
      if (this.parseDateMeasures && LocalDateRegex.test(value)) {
        return new Date(value);
      } else if (!Number.isNaN(Number.parseFloat(value))) {
        return Number.parseFloat(value);
      }

      return value;
    };

    const duplicateMeasures = new Set();
    if (this.queryType === QUERY_TYPE.BLENDING_QUERY) {
      const allMeasures = flatten(this.loadResponses.map(({ query }) => query.measures));
      allMeasures.filter((e, i, a) => a.indexOf(e) !== i).forEach(m => duplicateMeasures.add(m));
    }

    return this.pivot(pivotConfig).map(({ xValues, yValuesArray }) => {
      const yValuesMap = {};

      yValuesArray
        .forEach(([yValues, m], i) => {
          yValuesMap[this.axisValuesString(aliasSeries(yValues, i, pivotConfig, duplicateMeasures), ',')] = m && validate(m);
        });

      return ({
        x: this.axisValuesString(xValues, ','),
        xValues,
        ...yValuesMap
      });
    });
  }

  tablePivot(pivotConfig) {
    const normalizedPivotConfig = this.normalizePivotConfig(pivotConfig || {});
    const isMeasuresPresent = normalizedPivotConfig.x.concat(normalizedPivotConfig.y).includes('measures');

    return this.pivot(normalizedPivotConfig).map(({ xValues, yValuesArray }) => fromPairs(
      normalizedPivotConfig.x
        .map((key, index) => [key, xValues[index]])
        .concat(
          isMeasuresPresent ? yValuesArray.map(([yValues, measure]) => [
            yValues.length ? yValues.join() : 'value',
            measure
          ]) : []
        )
    ));
  }

  tableColumns(pivotConfig) {
    const normalizedPivotConfig = this.normalizePivotConfig(pivotConfig || {});
    const annotations = pipe(
      pluck('annotation'),
      reduce(mergeDeepLeft(), {})
    )(this.loadResponses);
    const flatMeta = Object.values(annotations).reduce((a, b) => ({ ...a, ...b }), {});
    const schema = {};

    const extractFields = (key) => {
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

    (pivot[0] && pivot[0].yValuesArray || []).forEach(([yValues]) => {
      if (yValues.length > 0) {
        let currentItem = schema;

        yValues.forEach((value, index) => {
          currentItem[`_${value}`] = {
            key: value,
            memberId: normalizedPivotConfig.y[index] === 'measures'
              ? value
              : normalizedPivotConfig.y[index],
            children: (currentItem[`_${value}`] && currentItem[`_${value}`].children) || {}
          };

          currentItem = currentItem[`_${value}`].children;
        });
      }
    });

    const toColumns = (item = {}, path = []) => {
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
          };
        }

        return {
          ...fields,
          key,
          title: [title, dimensionValue].join(' ').trim(),
          shortTitle: dimensionValue || shortTitle,
          children,
        };
      });
    };

    let otherColumns = [];

    if (!pivot.length && normalizedPivotConfig.y.includes('measures')) {
      otherColumns = (this.loadResponses[0].query.measures || []).map(
        (key) => ({ ...extractFields(key), dataIndex: key })
      );
    }

    // Syntatic column to display the measure value
    if (!normalizedPivotConfig.y.length && normalizedPivotConfig.x.includes('measures')) {
      otherColumns.push({
        key: 'value',
        dataIndex: 'value',
        title: 'Value',
        shortTitle: 'Value',
        type: 'string',
      });
    }

    return normalizedPivotConfig.x
      .map((key) => {
        if (key === 'measures') {
          return {
            key: 'measures',
            dataIndex: 'measures',
            title: 'Measures',
            shortTitle: 'Measures',
            type: 'string',
          };
        }

        return ({ ...extractFields(key), dataIndex: key });
      })
      .concat(toColumns(schema))
      .concat(otherColumns);
  }

  totalRow(pivotConfig) {
    return this.chartPivot(pivotConfig)[0];
  }

  categories(pivotConfig) { // TODO
    return this.chartPivot(pivotConfig);
  }

  seriesNames(pivotConfig) {
    pivotConfig = this.normalizePivotConfig(pivotConfig);
    const measures = pipe(
      pluck('annotation'),
      pluck('measures'),
      mergeAll
    )(this.loadResponses);

    const seriesNames = unnest(this.loadResponses.map((_, index) => pipe(
      map(this.axisValues(pivotConfig.y, index)),
      unnest,
      uniq
    )(
      this.timeDimensionBackwardCompatibleData(index)
    )));
    const duplicateMeasures = new Set();
    if (this.queryType === QUERY_TYPE.BLENDING_QUERY) {
      const allMeasures = flatten(this.loadResponses.map(({ query }) => query.measures));
      allMeasures.filter((e, i, a) => a.indexOf(e) !== i).forEach(m => duplicateMeasures.add(m));
    }

    return seriesNames.map((axisValues, i) => {
      const aliasedAxis = aliasSeries(axisValues, i, pivotConfig, duplicateMeasures);
      return {
        title: this.axisValuesString(
          pivotConfig.y.find(d => d === 'measures') ?
            dropLast(1, aliasedAxis).concat(
              measures[
                ResultSet.measureFromAxis(axisValues)
              ].title
            ) :
            aliasedAxis, ', '
        ),
        shortTitle: this.axisValuesString(
          pivotConfig.y.find(d => d === 'measures') ?
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

  query() {
    if (this.queryType !== QUERY_TYPE.REGULAR_QUERY) {
      throw new Error(`Method is not supported for a '${this.queryType}' query type. Please use decompose`);
    }

    return this.loadResponses[0].query;
  }

  pivotQuery() {
    return this.loadResponse.pivotQuery || null;
  }

  rawData() {
    if (this.queryType !== QUERY_TYPE.REGULAR_QUERY) {
      throw new Error(`Method is not supported for a '${this.queryType}' query type. Please use decompose`);
    }

    return this.loadResponses[0].data;
  }

  annotation() {
    if (this.queryType !== QUERY_TYPE.REGULAR_QUERY) {
      throw new Error(`Method is not supported for a '${this.queryType}' query type. Please use decompose`);
    }

    return this.loadResponses[0].annotation;
  }

  timeDimensionBackwardCompatibleData(resultIndex) {
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
                field => timeDimensions.find(d => d.dimension === field) &&
                  !row[ResultSet.timeDimensionMember(timeDimensions.find(d => d.dimension === field))]
              ).map(field => (
                [ResultSet.timeDimensionMember(timeDimensions.find(d => d.dimension === field)), row[field]]
              )))
          )
        }
      ));
    }

    return this.backwardCompatibleData[resultIndex];
  }

  decompose() {
    return this.loadResponses.map((result) => new ResultSet({
      queryType: QUERY_TYPE.REGULAR_QUERY,
      pivotQuery: {
        ...result.query,
        queryType: QUERY_TYPE.REGULAR_QUERY,
      },
      results: [result]
    }, this.options));
  }

  serialize() {
    return {
      loadResponse: clone(this.loadResponse)
    };
  }
}

export default ResultSet;
