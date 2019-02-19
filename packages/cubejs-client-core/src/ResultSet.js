import { groupBy, pipe, toPairs, uniq, filter, map, unnest, dropLast, equals, reduce, minBy, maxBy } from 'ramda';
import * as Moment from 'moment';
import * as momentRange from 'moment-range';

const moment = momentRange.extendMoment(Moment);

const TIME_SERIES = {
  day: (range) =>
    Array.from(range.by('day'))
      .map(d => d.format('YYYY-MM-DDT00:00:00.000')),
  month: (range) =>
    Array.from(range.snapTo('month').by('month'))
      .map(d => d.format('YYYY-MM-01T00:00:00.000')),
  hour: (range) =>
    Array.from(range.by('hour'))
      .map(d => d.format('YYYY-MM-DDTHH:00:00.000')),
  week: (range) =>
    Array.from(range.snapTo('isoweek').by('week'))
      .map(d => d.startOf('isoweek').format('YYYY-MM-DDT00:00:00.000'))
};

export default class ResultSet {
  constructor(loadResponse) {
    this.loadResponse = loadResponse;
  }

  series(pivotConfig) {
    return this.seriesNames(pivotConfig).map(({ title, key }) => ({
      title,
      series: this.chartPivot(pivotConfig).map(({ category, x, ...obj }) => ({ value: obj[key], category, x }))
    }));
  }

  axisValues(axis) {
    const query = this.loadResponse.query;
    return row => {
      const value = (measure) =>
        axis.filter(d => d !== 'measures')
        .map(d => row[d] != null ? row[d] : null).concat(measure ? [measure] : []);
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
    return axisValues.map(formatValue).join(delimiter || ':');
  }

  normalizePivotConfig(pivotConfig) {
    const query = this.loadResponse.query;
    let timeDimensions = (query.timeDimensions || []).filter(td => !!td.granularity);
    pivotConfig = pivotConfig || (timeDimensions.length ? {
      x: timeDimensions.map(td => td.dimension),
      y: query.dimensions || []
    } : {
      x: query.dimensions || [],
      y: []
    });
    if (!pivotConfig.x.concat(pivotConfig.y).find(d => d === 'measures')) {
      pivotConfig.y = pivotConfig.y.concat(['measures']);
    }
    if (pivotConfig.fillMissingDates == null) {
      pivotConfig.fillMissingDates = true;
    }
    return pivotConfig;
  }

  static measureFromAxis(axisValues) {
    return axisValues[axisValues.length - 1];
  };

  timeSeries(timeDimension) {
    if (!timeDimension.granularity) {
      return null;
    }
    let dateRange = timeDimension.dateRange;
    if (!dateRange) {
      const dates = pipe(
        map(row => row[timeDimension.dimension] && moment(row[timeDimension.dimension])),
        filter(r => !!r)
      )(this.loadResponse.data);

      dateRange = dates.length && [
        reduce(minBy(d => d.toDate()), dates[0], dates),
        reduce(maxBy(d => d.toDate()), dates[0], dates)
      ] || null;
    }
    if (!dateRange) {
      return null;
    }
    const start = moment(dateRange[0]).format('YYYY-MM-DD 00:00:00');
    const end = moment(dateRange[1]).format('YYYY-MM-DD 23:59:59');
    const range = moment.range(start, end);
    if (!TIME_SERIES[timeDimension.granularity]) {
      throw new Error(`Unsupported time granularity: ${timeDimension.granularity}`);
    }
    return TIME_SERIES[timeDimension.granularity](range);
  }

  pivot(pivotConfig) {
    pivotConfig = this.normalizePivotConfig(pivotConfig);
    let groupByXAxis = groupBy(({ xValues }) => this.axisValuesString(xValues));

    let measureValue = (row, measure, xValues) => row[measure];

    if (
      pivotConfig.fillMissingDates &&
      pivotConfig.x.length === 1 &&
      equals(
        pivotConfig.x,
        (this.loadResponse.query.timeDimensions || []).filter(td => !!td.granularity).map(td => td.dimension)
      )
    ) {
      const series = this.timeSeries(this.loadResponse.query.timeDimensions[0]);
      if (series) {
        groupByXAxis = (rows) => {
          const byXValues = groupBy(({ xValues }) => moment(xValues[0]).format(moment.HTML5_FMT.DATETIME_LOCAL_MS), rows);
          return series.map(d => ({ [d]: byXValues[d] || [{ xValues: [d], row: {} }] }))
            .reduce((a, b) => Object.assign(a, b), {});
        };

        measureValue = (row, measure, xValues) => row[measure] || 0;
      }
    }

    const xGrouped = pipe(
      map(row => this.axisValues(pivotConfig.x)(row).map(xValues => ({ xValues, row }))),
      unnest,
      groupByXAxis,
      toPairs
    )(this.loadResponse.data);

    const allYValues = pipe(
      map(
        ([xValuesString, rows]) => unnest(rows.map(({ row }) => this.axisValues(pivotConfig.y)(row)))
      ),
      unnest,
      uniq
    )(xGrouped);

    return xGrouped.map(([xValuesString, rows]) => {
      const xValues = rows[0].xValues;
      const yGrouped = pipe(
        map(({ row }) => this.axisValues(pivotConfig.y)(row).map(yValues => ({ yValues, row }))),
        unnest,
        groupBy(({ yValues }) => this.axisValuesString(yValues))
      )(rows);
      return {
        xValues,
        yValuesArray: unnest(allYValues.map(yValues => {
          let measure = pivotConfig.x.find(d => d === 'measures') ?
            ResultSet.measureFromAxis(xValues) :
            ResultSet.measureFromAxis(yValues);
          return (yGrouped[this.axisValuesString(yValues)] || [{ row: {} }]).map(({ row }) => [yValues, measureValue(row, measure, xValues)])
        }))
      };
    });
  }

  pivotedRows(pivotConfig) { // TODO
    return this.chartPivot(pivotConfig);
  }

  chartPivot(pivotConfig) {
    return this.pivot(pivotConfig).map(({ xValues, yValuesArray }) => ({
      category: this.axisValuesString(xValues, ', '), //TODO deprecated
      x: this.axisValuesString(xValues, ', '),
      ...(
        yValuesArray
          .map(([yValues, m]) => ({ [this.axisValuesString(yValues, ', ')]: m && Number.parseFloat(m) }))
          .reduce((a, b) => Object.assign(a, b), {})
      )
    }));
  }

  totalRow() {
    return this.chartPivot()[0];
  }

  categories(pivotConfig) { //TODO
    return this.chartPivot(pivotConfig);
  }

  seriesNames(pivotConfig) {
    pivotConfig = this.normalizePivotConfig(pivotConfig);
    return pipe(map(this.axisValues(pivotConfig.y)), unnest, uniq)(this.loadResponse.data).map(axisValues => ({
      title: this.axisValuesString(pivotConfig.y.find(d => d === 'measures') ?
        dropLast(1, axisValues)
          .concat(this.loadResponse.annotation.measures[ResultSet.measureFromAxis(axisValues)].title) :
        axisValues, ', '),
      key: this.axisValuesString(axisValues)
    }))
  }

  query() {
    return this.loadResponse.query;
  }

  rawData() {
    return this.loadResponse.data;
  }
}
