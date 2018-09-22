import React from 'react';
import toChartjsData from './toChartjsData';
import { Pie, Line, Bar } from 'react-chartjs-2';
import Chart from './Chart';

const chartsExamples = {
  line: {
    title: 'Line',
    render: () => (
      <Chart
        query={{ measures: ['Stories.count'], timeDimensions: [{dimension: 'Stories.time', granularity: 'month', dateRange: ["2015-01-01", "2015-08-01"] }] }}
        render={ ({ resultSet }) => (
          <Line data={toChartjsData('line', resultSet)} options={{ scales: { xAxes: [{ type: `time`, time: { unit: 'month' }}] }}} />
        )}
      />
    )
  },
  bar: {
    title: 'Bar',
    render: () => (
      <Chart
        query={{
          measures: ['Stories.count', 'Stories.totalScore'],
          timeDimensions: [{ dimension: 'Stories.time', granularity: 'month', dateRange: ["2015-01-01", "2015-08-01"] }]
        }}
        render={ ({ resultSet }) => (
          <Bar data={toChartjsData('bar', resultSet)} options={{ scales: { xAxes: [{ type: `time`, time: { unit: 'month' }}] }}} />
        )}
      />
    )
  },
  pie: {
    title: 'Pie',
    render: () => (
      <Chart
        query={{ measures: ['Stories.count'], dimensions: ['Stories.category'] }}
        render={ ({ resultSet }) => (
          <Pie data={toChartjsData('pie', resultSet)} />
        )}
      />
    )
  }
};

export default chartsExamples;
