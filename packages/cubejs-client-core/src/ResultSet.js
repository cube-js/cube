import { groupBy, pipe, toPairs, uniq, flatten, map, unnest, dropLast } from 'ramda';

export default class ResultSet {
  constructor(loadResponse) {
    this.loadResponse = loadResponse;
  }

  series(pivotConfig) {
    return this.seriesNames(pivotConfig).map(({ title, key }) => ({
      title,
      series: this.pivotedRows(pivotConfig).map(({ category, ...obj }) => ({ value: obj[key], category }))
    }));
  }

  axisValues(axis) {
    const query = this.loadResponse.query;
    return row => {
      const value = (measure) =>
        axis.filter(d => d !== 'measures')
        .map(d => row[d]).concat(measure ? [measure] : []);
      if (axis.find(d => d === 'measures') && (query.measures || []).length) {
        return query.measures.map(value);
      }
      return [value()];
    };
  }

  axisValuesString(axisValues, delimiter) {
    return axisValues.map(v => v != null ? v : '∅').join(delimiter || ':');
  }

  normalizePivotConfig(pivotConfig) {
    const query = this.loadResponse.query;
    let timeDimensions = (query.timeDimensions || []).filter(td => !!td.granularity);
    pivotConfig = pivotConfig || timeDimensions.length ? {
      x: timeDimensions.map(td => td.dimension),
      y: query.dimensions || []
    } : {
      x: query.dimensions || [],
      y: []
    };
    if (!pivotConfig.x.concat(pivotConfig.y).find(d => d === 'measures')) {
      pivotConfig.y = pivotConfig.y.concat(['measures']);
    }
    return pivotConfig;
  }

  static measureFromAxis(axisValues) {
    return axisValues[axisValues.length - 1];
  };

  pivot(pivotConfig) {
    // TODO missing date filling
    pivotConfig = this.normalizePivotConfig(pivotConfig);
    return pipe(
      map(row => this.axisValues(pivotConfig.x)(row).map(xValues => ({ xValues, row }))),
      unnest,
      groupBy(({ xValues }) => this.axisValuesString(xValues)),
      toPairs
    )(this.loadResponse.data).map(([xValuesString, rows]) => {
      const xValues = rows[0].xValues;
      return {
        xValues,
        ...(rows.map(r => r.row).map(row => this.axisValues(pivotConfig.y)(row).map(yValues => {
            let measure = pivotConfig.x.find(d => d === 'measures') ?
              ResultSet.measureFromAxis(xValues) :
              ResultSet.measureFromAxis(yValues);
            return {
              [this.axisValuesString(yValues)]: row[measure]
            }
          }).reduce((a, b) => Object.assign(a, b), {})
        )).reduce((a, b) => Object.assign(a, b), {})
      };
    });
  }

  pivotedRows(pivotConfig) { // TODO
    return this.chartPivot(pivotConfig);
  }

  chartPivot(pivotConfig) {
    return this.pivot(pivotConfig).map(({ xValues, ...measures }) => ({
      category: this.axisValuesString(xValues, ', '),
      ...(map(m => m && Number.parseFloat(m), measures))
    }));
  }

  totalRow() {
    return this.pivotedRows()[0];
  }

  categories(pivotConfig) { //TODO
    return this.pivotedRows(pivotConfig);
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