import React from 'react';
import chartjsConfig from './toChartjsData';
import { Line, Bar, Pie }  from 'react-chartjs-2';
import Example from './Chart';

const chartTypesToComponents = {
  line: Line,
  bar: Bar,
  pie: Pie
};

class Chart extends React.Component {
  render() {
    const ChartComponent = chartTypesToComponents[this.props.type];
    return (
      <ChartComponent
        {...this.props}
        ref={ref => this.chartInstance = ref && ref.chartInstance}
      />
    )
  }
}

const renderChart = (resultSet, type) => {
  const { data, options } = chartjsConfig(type, resultSet)
  return <Chart data={data} type={type} options={options}  />
}

const chartsExamples = {
  line: {
    title: 'Line',
    render: () => (
      <Example
        query={{
          measures: ['Stories.count'], timeDimensions: [{dimension: 'Stories.time', granularity: 'month', dateRange: ["2015-01-01", "2015-08-01"] }]
        }}
        codeExample={
          `
import { default as cubejs, chartjsConfig } from 'cubejs-client';
import { QueryRenderer } from '@cubejs-client/react';
import { Line } from 'react-chartjs-2';

ReactDOM.render(
  <QueryRenderer
    query={{
      measures: ['Stories.count'], dimensions: ['Stories.date.month']
    }}
    cubejsApi={cubejs('API_KEY')}
    render={ ({ resultSet }) => {
      const { data, options } = chartjsConfig('line', resultSet)
      return <Line data={data} options={options} />
    }
  />
)
          `
        }
        render={ ({ resultSet }) => renderChart(resultSet, 'line') }
      />
    )
  },
  bar: {
    title: 'Bar',
    render: () => (
      <Example
        query={{
          measures: ['Stories.count', 'Stories.totalScore'],
          timeDimensions: [{ dimension: 'Stories.time', granularity: 'month', dateRange: ["2015-01-01", "2015-08-01"] }]
        }}
        render={ ({ resultSet }) => renderChart(resultSet, 'bar') }
      />
    )
  },
  pie: {
    title: 'Pie',
    render: () => (
      <Example
        query={{ measures: ['Stories.count'], dimensions: ['Stories.category'] }}
        render={ ({ resultSet }) => renderChart(resultSet, 'pie') }
      />
    )
  }
};

export default chartsExamples;
