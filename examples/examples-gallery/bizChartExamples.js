import React from 'react';
import Example from './Example';
import SourceRender from 'react-source-render';
import presetEnv from '@babel/preset-env';
import presetReact from '@babel/preset-react';

import * as bizChartLibrary from './libraries/bizChart';
import * as chartjsLibrary from './libraries/chartjs';

const libraryToTemplate = {
  bizcharts: bizChartLibrary,
  chartjs: chartjsLibrary
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

const babelConfig = {
  presets: [
    presetEnv,
    presetReact
  ]
};

const renderExample = (chartType, query) => {
  return ({ chartLibrary }) => {
     return (<Example
       query={query}
       codeExample={libraryToTemplate[chartLibrary].sourceCodeTemplate(chartType, query)}
       render={() => (<SourceRender
         babelConfig={babelConfig}
         onError={error => console.log(error)}
         onSuccess={(error, { markup }) => console.log('HTML', markup)}
         resolver={importName => libraryToTemplate[chartLibrary].imports[importName]}
         source={libraryToTemplate[chartLibrary].sourceCodeTemplate(chartType, query)}
       />)
       }
     />);
  }
};

const chartsExamples = {
  line: {
    title: 'Line',
    render: renderExample('line', {
  measures: ['Stories.count'],
  dimensions: ['Stories.time.month'],
  filters: [
    {
      dimension: `Stories.time`,
      operator: `beforeDate`,
      values: [`2010-01-01`]
    }
  ]
})
  },
  lineMulti: {
    title: 'Line',
    render: renderExample('lineMulti', {
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
    render: renderExample('barStacked', {
      measures: ['Stories.count'],
      dimensions: ['Stories.category'],
      timeDimensions: [
        {
          dimension: 'Stories.time',
          granularity: 'month',
          dateRange: ["2015-01-01", "2015-07-31"] }
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
