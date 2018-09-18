export default class ResultSet {
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