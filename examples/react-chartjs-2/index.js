import React from 'react';
import ReactDOM from 'react-dom';
import cubejs from 'cubejs-client';
import JSONPretty from 'react-json-pretty';
import { QueryRenderer } from '@cubejs-client/react';
import { Pie, Line } from 'react-chartjs-2';
import { Row, Col } from 'antd';
import toChartjsData from './toChartjsData';
import 'antd/dist/antd.css';

// Minimal Example:
//
// import React from 'react';
// import { default as cubejs, toChartjsData } from 'cubejs-client';
// import { QueryRenderer } from '@cubejs-client/react';
// import { Pie } from 'react-chartjs-2';

// const Chart = () => (
//   <QueryRenderer
//     query={{ measures: ['Stories.count'], dimensions: ['Stories.date.week'] }}
//     cubejsApi={cubejs('API_KEY')}
//     render={ ({ resultSet }) => (
//       <Line data={toChartjsData('line', resultSet)} />
//     )
//   />
// )

const HACKER_NEWS_DATASET_API_KEY = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw'

const Chart = ({ query, render }) => (
  <QueryRenderer
    query={query}
    cubejsApi={cubejs(HACKER_NEWS_DATASET_API_KEY)}
    render={ ({ resultSet, error }) => {
      if (resultSet) {
        return (
          <Row>
            <Col span={12}>
              {render({ resultSet, error })}
            </Col>
            <Col span={12}>
              <JSONPretty id="json-pretty" json={resultSet}></JSONPretty>
            </Col>
          </Row>
        );
      }
      return <div>Loading</div>;
    }}
  />
)

const App = () => (
  <div>
    <Chart
      query={{ measures: ['Stories.count'], dimensions: ['Stories.category'] }}
      render={ ({ resultSet }) => (
        <Pie data={toChartjsData('pie', resultSet)} options={{legend: { position: 'bottom' }}} />
      )}
    />
    <Chart
      query={{ measures: ['Stories.count'], timeDimensions: [{dimension: 'Stories.time', granularity: 'month', dateRange: ["2015-01-01", "2015-08-01"] }] }}
      render={ ({ resultSet }) => (
        <Line data={toChartjsData('line', resultSet)} options={{legend: { position: 'bottom' }}} />
      )}
    />
  </div>
)

ReactDOM.render(<App />, document.getElementById('root'));
