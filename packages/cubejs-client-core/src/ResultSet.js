import {
  groupBy, pipe, fromPairs, toPairs, uniq, filter, map, unnest, dropLast, equals, reduce, minBy, maxBy
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

class ResultSet {
  constructor(loadResponse, options) {
    options = options || {};
    this.loadResponse = loadResponse;
    this.parseDateMeasures = options.parseDateMeasures;
  }
  
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

  static getNormalizedPivotConfig(query, pivotConfig = null) {
    const timeDimensions = (query.timeDimensions || []).filter(td => !!td.granularity);
    const dimensions = query.dimensions || [];
    pivotConfig = pivotConfig || (timeDimensions.length ? {
      x: timeDimensions.map(td => ResultSet.timeDimensionMember(td)),
      y: dimensions
    } : {
      x: dimensions,
      y: []
    });
    
    pivotConfig = {
      ...pivotConfig,
      x: [...(pivotConfig.x || [])],
      y: [...(pivotConfig.y || [])],
    };

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
    
    const dimensionFilter = (key) => key === 'measures' || (key !== 'measures' && allDimensions.includes(key));
    
    pivotConfig.x = pivotConfig.x.concat(
      allDimensions.filter(d => !allIncludedDimensions.includes(d))
    ).filter(dimensionFilter);
    pivotConfig.y = pivotConfig.y.filter(dimensionFilter);
    
    if (!pivotConfig.x.concat(pivotConfig.y).find(d => d === 'measures')) {
      pivotConfig.y.push('measures');
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
  
  normalizePivotConfig(pivotConfig) {
    const { query } = this.loadResponse;
    
    return ResultSet.getNormalizedPivotConfig(query, pivotConfig);
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
        ([, rows]) => unnest(
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

  tablePivot(pivotConfig) {
    const normalizedPivotConfig = this.normalizePivotConfig(pivotConfig || {});

    return this.pivot(normalizedPivotConfig).map(({ xValues, yValuesArray }) => fromPairs(
      normalizedPivotConfig.x
        .map((key, index) => [key, xValues[index]])
        .concat(
          (yValuesArray[0][0].length &&
              yValuesArray.map(([yValues, measure]) => [
                yValues.join('.'),
                measure
              ])) ||
              []
        )
    ));
  }

  tableColumns(pivotConfig) {
    const normalizedPivotConfig = this.normalizePivotConfig(pivotConfig);
    const schema = {};
    
    const extractFields = (key) => {
      const flatMeta = Object.values(this.loadResponse.annotation).reduce((a, b) => ({ ...a, ...b }), {});
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
          currentItem[value] = {
            key: value,
            memberId: normalizedPivotConfig.y[index] === 'measures' ? value : normalizedPivotConfig.y[index],
            children: (currentItem[value] && currentItem[value].children) || {}
          };
    
          currentItem = currentItem[value].children;
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
        
        const dimensionValue = key !== currentItem.memberId ? key : '';
        
        if (!children.length) {
          return {
            ...fields,
            key,
            dataIndex: [...path, key].join('.'),
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
    
    let measureColumns = [];
    if (!pivot.length && normalizedPivotConfig.y.find((key) => key === 'measures')) {
      measureColumns = (this.query().measures || []).map((key) => ({ ...extractFields(key), dataIndex: key }));
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
      .concat(measureColumns);
  }

  totalRow() {
    return this.chartPivot()[0];
  }

  categories(pivotConfig) { // TODO
    return this.chartPivot(pivotConfig);
  }

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
