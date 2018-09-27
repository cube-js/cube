class ChartjsResultSet {
  constructor(resultSet, userConfig) {
    this.resultSet = resultSet;
    this.userConfig = userConfig;
  }

  timeSeries() {
    return {
      type: 'line',
      data: {
        datasets: this.resultSet.series()
          .map(s => ({ label: s.title, data: s.series.map(r => ({ t: r.category, y: r.value }) ) }) )
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
      ...this.userConfig
    }
  }

  categories() {
    return {
      type: 'bar',
      data: {
        labels: this.resultSet.categories().map(c => c.category),
        datasets: this.resultSet.series()
          .map(s => ({ label: s.title, data: s.series.map(r => r.value) }) )
      },
      ...this.userConfig
    }
  }

  prepareConfig() {
    if ((this.resultSet.query().timeDimensions || []).find(td => !!td.granularity)) {
      return this.timeSeries();
    } else {
      return this.categories();
    }
  }
}

var index = (resultSet, userConfig) => {
  return new ChartjsResultSet(resultSet, userConfig).prepareConfig();
};

export default index;
