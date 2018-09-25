import { fetch } from 'whatwg-fetch';

class ResultSet {
  constructor(loadResponse) {
    this.loadResponse = loadResponse;
  }

  series(pivotConfig) {
    const query = this.loadResponse.query;
    return query.measures.map(measure => ({
      title: this.loadResponse.annotation.measures[measure].title,
      series: this.categories().map(
        ({ row, category }) => ({ value: row[measure], category })
      )
    }))
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

  axisKeys(axis) {
    const query = this.loadResponse.query;
    if (axis.find(d => d === 'measures') && (query.measures || []).length) {
      let withoutMeasures = axis.filter(d => d !== 'measures');
      return query.measures.map(measure => withoutMeasures.concat(measure).join(', '));
    } else {
      return [axis.join(', ')];
    }
  }

  normalizePivotConfig(pivotConfig) {
    const query = this.loadResponse.query;
    pivotConfig = pivotConfig || {
      x: (query.timeDimensions || []).filter(td => !!td.granularity).map(td => td.dimension),
      y: query.dimensions || []
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
    return this.loadResponse.data.map(row =>
      this.axisValues(pivotConfig.x)(row).map(category => ({
        row,
        category,
        ...(this.axisValues(pivotConfig.y)(row).map(series =>{
            let measure = pivotConfig.x.find(d => d === 'measures') ?
              ResultSet.measureFromAxis(category) :
              ResultSet.measureFromAxis(series);
            return {
            [pivotConfig.y.filter(d => d !== 'measures').concat(measure).join(', ')]: row[measure]
          }
          }).reduce((a, b) => Object.assign(a, b), {})
        )
      }))
    ).reduce((a, b) => a.concat(b), []);
  }

  categories(pivotConfig) { //TODO
    return this.pivotedRows(pivotConfig);
  }

  seriesNames(pivotConfig) {
    pivotConfig = this.normalizePivotConfig(pivotConfig);
    return this.axisKeys(pivotConfig.y).map(axis => ({
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

class ProgressResult {
  constructor(progressResponse) {
    this.progressResponse = progressResponse;
  }

  stage() {
    return this.progressResponse.stage;
  }

  timeElapsed() {
    return this.progressResponse.timeElapsed;
  }
}

const API_URL = "https://statsbot.co/cubejs-api/v1";

class CubejsApi {
  constructor(apiToken) {
    this.apiToken = apiToken;
  }

  request(url, config) {
    return fetch(
      `${API_URL}${url}`,
      Object.assign({ headers: { Authorization: this.apiToken, 'Content-Type': 'application/json' }}, config || {})
    )
  }

  load(query, options, callback) {
    if (typeof options === 'function' && !callback) {
      callback = options;
      options = undefined;
    }

    const loadImpl = async () => {
      const response = await this.request(`/load?query=${JSON.stringify(query)}`);
      if (response.status === 502) {
        return loadImpl(); // TODO backoff wait
      }
      const body = await response.json();
      if (body.error === 'Continue wait') {
        if (options.progressCallback) {
          options.progressCallback(new ProgressResult(body));
        }
        return loadImpl();
      }
      if (response.status !== 200) {
        throw new Error(body.error); // TODO error class
      }
      return new ResultSet(body);
    };
    if (callback) {
      loadImpl().then(r => callback(null, r), e => callback(e));
    } else {
      return loadImpl();
    }
  }
}

var index = (apiToken) => {
  return new CubejsApi(apiToken);
};

export default index;
