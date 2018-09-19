import { fetch } from 'whatwg-fetch';

class ResultSet {
  constructor(loadResponse) {
    this.loadResponse = loadResponse;
  }

  series() {
    const query = this.loadResponse.query;
    return query.measures.map(measure => ({
      title: this.loadResponse.annotation.measures[measure].title,
      series: this.categories().map(
        ({ row, category }) => ({ value: row[measure], category })
      )
    }))
  }

  categoryFn() {
    const query = this.loadResponse.query;
    return row => {
      const dimensionValues = (query.dimensions || []).map(d => row[d]).concat(
        (query.timeDimensions || []).filter(td => !!td.granularity).map(td => row[td.dimension])
      );
      return dimensionValues.map(v => v || '∅').join(', ');
    };
  }

  categories() {
    const query = this.loadResponse.query;
    // TODO missing date filling
    return this.loadResponse.data.map(row => ({ row, category: this.categoryFn()(row) }));
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
