import React from 'react';
import { Row, Col } from 'antd';
import { QueryRenderer } from '@cubejs-client/react';
import JSONPretty from 'react-json-pretty';
import cubejs from 'cubejs-client';

// Minimal Example:
//
// import { default as cubejs, toChartjsData } from 'cubejs-client';
// import { QueryRenderer } from '@cubejs-client/react';
// import { Pie } from 'react-chartjs-2';

// ReactDOM.render(
//   <QueryRenderer
//     query={{ measures: ['Stories.count'], dimensions: ['Stories.date.month'] }}
//     cubejsApi={cubejs('API_KEY')}
//     render={ ({ resultSet }) => (
//       <Line
//         data={toChartjsData('line', resultSet)}
//         options={{ scales: { xAxes: [{ type: `time`, time: { unit: 'month' }}] }}}
//       />
//     )
//   />
// )

const HACKER_NEWS_DATASET_API_KEY = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw'

const Chart = ({ query, render }) => (
  <Row>
    <QueryRenderer
      query={query}
      cubejsApi={cubejs(HACKER_NEWS_DATASET_API_KEY)}
      render={ ({ resultSet, error }) => {
        if (resultSet) {
          return [
            <Col span={12}>
              {render({ resultSet, error })}
            </Col>,
            <Col span={12}>
              <JSONPretty id="json-pretty" json={resultSet}></JSONPretty>
            </Col>
          ];
        }
        return <div>Loading</div>;
      }}
    />
  </Row>
)

export default Chart;
