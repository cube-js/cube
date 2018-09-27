import React from 'react';
import chartjsConfig from './toChartjsData';
import { Line, Bar, Pie }  from 'react-chartjs-2';
import Example from './Chart';

const chartTypesToComponents = {
  line: Line,
  bar: Bar,
  pie: Pie
};

const exampleTemplate = (query, type) => (
`
import { default as cubejs, chartjsConfig } from 'cubejs-client';
import { QueryRenderer } from '@cubejs-client/react';
import { Line } from 'react-chartjs-2';

const query =
${JSON.stringify(query, null, 2)}

ReactDOM.render(
  <QueryRenderer
    query={query}
    cubejsApi={cubejs('API_KEY')}
    render={ ({ resultSet }) => {
      const { data, options } = chartjsConfig('${type}', resultSet)
      return <${chartTypesToComponents[type].name} data={data} options={options} />
    }
  />
)
`
)

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

const renderExample = (type, query) => {
  return () => {
     return (<Example
       query={query}
       codeExample={exampleTemplate(query, type)}
       render={ ({ resultSet }) => renderChart(resultSet, type) }
     />);
  }
}

const chartsExamples = {
  line: {
    title: 'Line',
    render: renderExample('line', {
      measures: ['Stories.count'],
      timeDimensions: [
        {
          dimension: 'Stories.time',
          granularity: 'month',
          dateRange: ["2015-01-01", "2015-08-01"]
        }
      ]
    })
  },
  lineMulti: {
    title: 'Line',
    render: renderExample('line', {
      measures: ['Stories.count', 'Stories.totalScore'],
      timeDimensions: [
        {
          dimension: 'Stories.time',
          granularity: 'month',
          dateRange: ["2015-01-01", "2015-08-01"]
        }
      ]
    })
  },
  bar: {
    title: 'Bar',
    render: renderExample('bar', {
      measures: ['Stories.count'],
      timeDimensions: [
        {
          dimension: 'Stories.time',
          granularity: 'month',
          dateRange: ["2015-01-01", "2015-08-01"] }
      ]
    })
  },
  barStacked: {
    title: 'Bar',
    render: renderExample('bar', {
      measures: ['Stories.count'],
      dimensions: ['Stories.category'],
      timeDimensions: [
        {
          dimension: 'Stories.time',
          granularity: 'month',
          dateRange: ["2015-01-01", "2015-08-01"] }
      ]
    })
  },
  pie: {
    title: 'Pie',
    render: renderExample('pie', {
      measures: ['Stories.count'],
      dimensions: ['Stories.category']
    })
  }
};

export default chartsExamples;
