import { groupBy, pipe, toPairs, uniq, flatten, map } from 'ramda';

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
        .map(d => row[d]).concat(measure ? [measure] : [])
        .map(v => v != null ? v : '∅').join(', ');
      if (axis.find(d => d === 'measures') && (query.measures || []).length) {
        return query.measures.map(value);
      }
      return [value()];
    };
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

  static measureFromAxis(axis) {
    const axisValues = axis.split(', ');
    return axisValues[axisValues.length - 1];
  };

  pivotedRows(pivotConfig) {
    // TODO missing date filling
    pivotConfig = this.normalizePivotConfig(pivotConfig);
    return pipe(groupBy(this.axisValues(pivotConfig.x)), toPairs)(this.loadResponse.data).map(([category, rows]) => ({
      category,
      ...(rows.map(row => this.axisValues(pivotConfig.y)(row).map(series => {
            let measure = pivotConfig.x.find(d => d === 'measures') ?
              ResultSet.measureFromAxis(category) :
              ResultSet.measureFromAxis(series);
            return {
              [series]: row[measure] && Number.parseFloat(row[measure])
            }
          }).reduce((a, b) => Object.assign(a, b), {})
        )).reduce((a, b) => Object.assign(a, b), {})
      })
    );
  }

  totalRow() {
    return this.pivotedRows()[0];
  }

  categories(pivotConfig) { //TODO
    return this.pivotedRows(pivotConfig);
  }

  seriesNames(pivotConfig) {
    pivotConfig = this.normalizePivotConfig(pivotConfig);
    return pipe(map(this.axisValues(pivotConfig.y)), uniq, flatten)(this.loadResponse.data).map(axis => ({
      title: pivotConfig.y.find(d => d === 'measures') ? axis.replace(
        ResultSet.measureFromAxis(axis),
        this.loadResponse.annotation.measures[ResultSet.measureFromAxis(axis)].title
      ) : axis,
      key: axis
    }))
  }

  query() {
    return this.loadResponse.query;
  }

  rawData() {
    return this.loadResponse.data;
  }
}