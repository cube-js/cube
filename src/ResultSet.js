export default class ResultSet {
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