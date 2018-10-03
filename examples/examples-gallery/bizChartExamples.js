import React from 'react';
import chartjsConfig from './toChartjsData';
import { Line, Bar, Pie }  from 'react-chartjs-2';
import Example from './Example';
import SourceRender from 'react-source-render';

import cubejs from '@cubejs-client/core';
import * as cubejsReact from '@cubejs-client/react';
import * as antd from 'antd';

const chartTypesToComponents = {
  line: Line,
  bar: Bar,
  pie: Pie
};

const exampleTemplate = (query, chartCode) => (
`
import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/react';
import { Spin } from 'antd';

const HACKER_NEWS_API_KEY = '';

const query =
${JSON.stringify(query, null, 2)}

ReactDOM.render(
  <QueryRenderer
    query={query}
    cubejsApi={cubejs(HACKER_NEWS_API_KEY)}
    render={ ({ resultSet }) => {
      return resultSet && (${chartCode}) || <Spin />;
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

const babelConfig = [{
  presets: [
    'env',
    'react',
  ]
}];

const imports = {
  '@cubejs-client/core': cubejs,
  '@cubejs-client/react': cubejsReact,
  antd
};

const renderExample = (query, chartCode) => {
  return () => {
     return (<Example
       query={query}
       codeExample={exampleTemplate(query, chartCode)}
       render={() => (<SourceRender
         babelConfig={babelConfig}
         onError={error => console.log(error)}
         onSuccess={(error, { markup }) => console.log('HTML', markup)}
         resolver={importName => imports[importName]}
         source={exampleTemplate(query, chartCode)}
       />)
       }
     />);
  }
}

const chartsExamples = {
  line: {
    title: 'Line',
    render: renderExample({
      measures: ['Stories.count'],
      dimensions: ['Stories.time.month'],
      filters: [
        {
          dimension: `Stories.time`,
          operator: `beforeDate`,
          values: [`2010-01-01`]
        }
      ]
    }, ```
<Chart data={resultSet.chartPivot()}>
  <Axis
    name="category"
    title={false}
  />
  <Axis name="Stories.count" min={0} />
  <Tooltip showTitle={false} crosshairs={false} />
  <Geom type="interval" position={['category', 'Stories.count']} />
</Chart>```)
  },
  lineMulti: {
    title: 'Line',
    render: renderExample('line', {
      measures: ['Stories.count', 'Stories.totalScore'],
      dimensions: ['Stories.time.month'],
      filters: [
        {
          dimension: 'Stories.time',
          operator: 'inDateRange',
          values: ['2014-01-01', '2015-01-01']
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
