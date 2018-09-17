import cubejs from 'cubejs-client';

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