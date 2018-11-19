import React from 'react';
import Example from './Example';
import SourceRender from 'react-source-render';
import presetEnv from '@babel/preset-env';
import presetReact from '@babel/preset-react';
import cubejs from '@cubejs-client/core';
import * as cubejsReact from '@cubejs-client/react';
import * as antd from 'antd';

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

const sourceCodeTemplate = (chartLibrary, chartType, query) => (
  `import React from 'react';
import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/react';
import { Spin } from 'antd';
${libraryToTemplate[chartLibrary].sourceCodeTemplate(chartType, query)}

const query =
${typeof query === 'object' ? JSON.stringify(query, null, 2) : query};

const HACKER_NEWS_API_KEY = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw';

const Example = <QueryRenderer
  query={query}
  cubejsApi={cubejs(HACKER_NEWS_API_KEY)}
  render={({ resultSet }) => (
    resultSet && renderChart(resultSet) || (<Spin />)
  )}
/>;

export default Example;
`);

const basicFilterCodeTemplate = (chartLibrary, chartType, query) => (
  `import React from 'react';
import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/react';
import { Spin, Row, Col, Select } from 'antd';
const Option = Select.Option;
${libraryToTemplate[chartLibrary].sourceCodeTemplate(chartType, query)}

const query =
${typeof query === 'object' ? JSON.stringify(query, null, 2) : query};

const HACKER_NEWS_API_KEY = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw';

export default class Example extends React.Component {
  constructor(props) {
    super(props);
    this.state = { category: 'Other' };
  }
  
  handleChange(value) {
    this.setState({ category: value })
  }
  
  render() {
    return <div>
      <Row style={{ marginBottom: 12 }}>
        <Col>
          <Select value={this.state.category} style={{ width: 120 }} onChange={this.handleChange.bind(this)}>
            <Option value="Ask">Ask</Option>
            <Option value="Show">Show</Option>
            <Option value="Other">Other</Option>
          </Select>
        </Col>
      </Row>
      <Row>
        <Col>
          <QueryRenderer
            query={{ 
              ...query, 
              filters: [ 
                ...query.filters, 
                { 
                  dimension: 'Stories.category', 
                  operator: 'equals', 
                  values: [this.state.category] 
                }
              ] 
            }}
            cubejsApi={cubejs(HACKER_NEWS_API_KEY)}
            render={ ({ resultSet }) => (
              resultSet && renderChart(resultSet) || (<Spin />)
            )}
          />
        </Col>
      </Row>
    </div>
  }
};
`);

const datePickerCodeTemplate = (chartLibrary, chartType, query) => (
  `import React from 'react';
import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/react';
import { Spin, Row, Col, DatePicker } from 'antd';
${libraryToTemplate[chartLibrary].sourceCodeTemplate(chartType, query)}

const HACKER_NEWS_API_KEY = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw';

export default class Example extends React.Component {
  constructor(props) {
    super(props);
    this.state = { range: [
      moment('2014-01-01'), moment('2014-12-31')
    ]};
  }
  
  render() {
    const range = this.state.range;
    return <div>
      <Row style={{ marginBottom: 12 }}>
        <Col>
          <DatePicker.RangePicker 
            onChange={(range) => this.setState({ range })} 
            value={this.state.range} 
            style={{ marginBottom: 12 }}
          />
        </Col>
      </Row>
      <Row>
        <Col>
          <QueryRenderer
            cubejsApi={cubejs(HACKER_NEWS_API_KEY)}
            query={{ 
              measures: ['Stories.count'], 
              dimensions: ['Stories.time.month'],
              filters: range && range.length && [{ 
                dimension: 'Stories.time', 
                operator: 'inDateRange', 
                values: range.map(d => d.format('YYYY-MM-DD')) 
              }] || []
            }}
            render={ ({ resultSet }) => (
              resultSet && renderChart(resultSet) || (<Spin />)
            )}
          />
        </Col>
      </Row>
    </div>;
  }
}
`);

const renderExample = ({ chartType, query, sourceCodeFn, title }) => {
  sourceCodeFn = sourceCodeFn || sourceCodeTemplate;
  return ({ chartLibrary }) => {
    const chart = (<SourceRender
      babelConfig={babelConfig}
      onError={error => console.log(error)}
      onSuccess={(error, { markup }) => console.log('HTML', markup)}
      resolver={importName => ({
        '@cubejs-client/core': cubejs,
        '@cubejs-client/react': cubejsReact,
        antd,
        react: React,
        ...libraryToTemplate[chartLibrary].imports
      })[importName]}
      source={sourceCodeFn(chartLibrary, chartType, query)}
    />);
    return (<Example
      title={title}
      query={query}
      codeExample={sourceCodeFn(chartLibrary, chartType, query)}
      render={() => chart}
    />);
  }
};

const chartsExamples = {
  line: {
    group: 'basic',
    render: renderExample({
      title: 'Line Chart',
      chartType: 'line',
      query: {
        measures: ['Stories.count'],
        dimensions: ['Stories.time.month'],
        filters: [
          {
            dimension: `Stories.time`,
            operator: `beforeDate`,
            values: [`2010-01-01`]
          }
        ]
      }
    })
  },
  lineMulti: {
    group: 'basic',
    render: renderExample({
      title: 'Multi Axis',
      chartType: 'lineMulti',
      query: {
        measures: ['Stories.count', 'Stories.totalScore'],
        dimensions: ['Stories.time.month'],
        filters: [
          {
            dimension: 'Stories.time',
            operator: 'inDateRange',
            values: ['2014-01-01', '2015-01-01']
          }
        ]
      }
    })
  },
  bar: {
    group: 'basic',
    render: renderExample({
      title: 'Bar Chart',
      chartType: 'bar',
      query: {
        measures: ['Stories.count'],
        timeDimensions: [
          {
            dimension: 'Stories.time',
            granularity: 'month',
            dateRange: ["2015-01-01", "2015-08-01"]
          }
        ]
      }
    })
  },
  barStacked: {
    group: 'basic',
    render: renderExample({
      title: 'Stacked Bar Chart',
      chartType: 'barStacked',
      query: {
        measures: ['Stories.count'],
        dimensions: ['Stories.category'],
        timeDimensions: [
          {
            dimension: 'Stories.time',
            granularity: 'month',
            dateRange: ["2015-01-01", "2015-07-31"]
          }
        ]
      }
    })
  },
  pie: {
    group: 'basic',
    render: renderExample({
      title: 'Pie Chart',
      chartType: 'pie',
      query: {
        measures: ['Stories.count'],
        dimensions: ['Stories.category']
      }
    })
  },
  categoryFilter: {
    group: 'interaction',
    render: renderExample({
      title: 'Category Filter',
      chartType: 'categoryFilter',
      query: {
        measures: ['Stories.count'],
        dimensions: ['Stories.time.month'],
        filters: [
          {
            dimension: `Stories.time`,
            operator: `beforeDate`,
            values: [`2010-01-01`]
          }
        ]
      },
      sourceCodeFn: basicFilterCodeTemplate
    })
  },
  datePicker: {
    group: 'interaction',
    render: renderExample({
      title: 'Date Picker',
      chartType: 'line',
      query: {
        measures: ['Stories.count'],
        dimensions: ['Stories.time.month']
      },
      sourceCodeFn: datePickerCodeTemplate
    })
  }
};

export default chartsExamples;
