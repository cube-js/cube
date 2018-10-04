import React from 'react';
import Example from './Example';
import SourceRender from 'react-source-render';
import presetEnv from '@babel/preset-env';
import presetReact from '@babel/preset-react';

import cubejs from '@cubejs-client/core';
import * as cubejsReact from '@cubejs-client/react';
import * as antd from 'antd';
import * as bizcharts from 'bizcharts';
import moment from 'moment';

const exampleTemplate = (query, chartCode) => (
`import React from 'react';
import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/react';
import { Spin } from 'antd';
import { Chart, Axis, Tooltip, Geom, Coord, Legend } from 'bizcharts';
import moment from 'moment';

const HACKER_NEWS_API_KEY = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw';

const query =
${JSON.stringify(query, null, 2)}

const renderChart = (resultSet) => (${chartCode}
);

const Example = <QueryRenderer
  query={query}
  cubejsApi={cubejs(HACKER_NEWS_API_KEY)}
  render={ ({ resultSet }) => (
    resultSet && renderChart(resultSet) || (<Spin />)
  )}
/>;

export default Example;
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

const babelConfig = {
  presets: [
    presetEnv,
    presetReact
  ]
};

const imports = {
  '@cubejs-client/core': cubejs,
  '@cubejs-client/react': cubejsReact,
  antd,
  react: React,
  bizcharts,
  moment
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
};

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
}, `
<Chart scale={{ category: { tickCount: 8 } }} height={400} data={resultSet.chartPivot()} forceFit>
  <Axis name="category" label={{ formatter: val => moment(val).format("MMM 'YY") }} />
  <Axis name="Stories.count" />
  <Tooltip crosshairs={{type : 'y'}} />
  <Geom type="line" position="category*Stories.count" size={2} />
</Chart>`)
  },
  lineMulti: {
    title: 'Line',
    render: renderExample({
      measures: ['Stories.count', 'Stories.totalScore'],
      dimensions: ['Stories.time.month'],
      filters: [
        {
          dimension: 'Stories.time',
          operator: 'inDateRange',
          values: ['2014-01-01', '2015-01-01']
        }
      ]
    }, `
<Chart scale={{ category: { tickCount: 8 } }} height={400} data={resultSet.chartPivot()} forceFit>
  <Axis name="category" label={{ formatter: val => moment(val).format("MMM 'YY") }} />
  <Axis name="Stories.count" />
  <Tooltip crosshairs={{type : 'y'}} />
  <Geom type="line" position="category*Stories.count" />
  <Geom type="line" position="category*Stories.totalScore" color="#9AD681"/>
</Chart>`)
  },
  bar: {
    title: 'Bar',
    render: renderExample({
      measures: ['Stories.count'],
      timeDimensions: [
        {
          dimension: 'Stories.time',
          granularity: 'month',
          dateRange: ["2015-01-01", "2015-08-01"] }
      ]
    }, `
<Chart height={400} data={resultSet.chartPivot()} forceFit>
  <Axis name="category" label={{ formatter: val => moment(val).format("MMM 'YY") }} />
  <Axis name="Stories.count" />
  <Tooltip />
  <Geom type="interval" position="category*Stories.count" />
</Chart>`)
  },
  barStacked: {
    title: 'Bar',
    render: renderExample({
      measures: ['Stories.count'],
      dimensions: ['Stories.category'],
      timeDimensions: [
        {
          dimension: 'Stories.time',
          granularity: 'month',
          dateRange: ["2015-01-01", "2015-07-31"] }
      ]
    }, `
<Chart height={400} data={resultSet.rawData()} forceFit>
  <Legend />
  <Axis name="Stories.time" label={{ formatter: val => moment(val).format("MMM 'YY") }} />
  <Axis name="Stories.count" />
  <Tooltip />
  <Geom type='intervalStack' position="Stories.time*Stories.count" color="Stories.category" />
</Chart>`)
  },
  pie: {
    title: 'Pie',
    render: renderExample({
      measures: ['Stories.count'],
      dimensions: ['Stories.category']
    }, `
<Chart height={400} data={resultSet.chartPivot()} forceFit>
  <Coord type='theta' radius={0.75} />
  <Axis name="Stories.count" />
  <Legend position='right' />
  <Tooltip />
  <Geom
    type="intervalStack"
    position="Stories.count"
    color='category'>
  </Geom>
</Chart>`)
  }
};

export default chartsExamples;
