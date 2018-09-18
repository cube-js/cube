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
      const res = await this.request(`/load?query=${JSON.stringify(query)}`);
      const response = await res.json();
      if (response.error === 'Continue wait') {
        return loadImpl();
      }
      return new ResultSet(response);
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
