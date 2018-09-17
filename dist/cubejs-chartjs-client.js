import { fetch } from 'whatwg-fetch';

class ResultSet {
  constructor(loadResponse) {
    this.loadResponse = loadResponse;
  }

  series() {
    const query = this.loadResponse.query;
    return query.measures.map(measure => ({
      name: measure,
      series: this.loadResponse.data.map(row => {
        const dimensionValues = (query.dimensions || []).map(d => row[d]).concat(
          (query.timeDimensions || []).map(td => row[td.dimension])
        );
        return [dimensionValues.join(', '), row[measure]];
      })
    }))
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

  load(jobId, query, callback) {
    const loadImpl = async () => {
      const res = await this.request(`/load?query=${JSON.stringify(query)}`);
      const response = await res.json();
      return new ResultSet(response);
    };
    if (callback) {
      loadImpl().then(r => callback(null, r), e => callback(e));
    } else {
      return loadImpl();
    }
  }
}

var cubejs = (apiToken) => {
  return new CubejsApi(apiToken);
};

class ChartjsResultSet {
  constructor(resultSet) {
    this.resultSet = resultSet;
  }

  timeSeries(config) {
    return {
      type: 'line',
      data: {
        datasets: this.resultSet.series()
          .map(s => ({ label: s.name, data: s.series.map(r => ({ t: r[0], y: r[1] }) ) }) )
      },
      options: {
        scales: {
          xAxes: [{
            type: 'time',
            unit: this.resultSet.query().timeDimensions[0].granularity,
            distribution: 'series',
            bounds: 'data'
          }]
        }
      },
      ...config
    }
  }
}

cubejs.chartjs = (resultSet) => {
  return new ChartjsResultSet(resultSet);
};

export default cubejs;
