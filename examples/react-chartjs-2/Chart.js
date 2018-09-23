import React from 'react';
import { Row, Col, Tabs, Spin } from 'antd';
import { QueryRenderer } from '@cubejs-client/react';
import JSONPretty from 'react-json-pretty';
import cubejs from 'cubejs-client';
import Prism from "prismjs";
import "./prism.css";


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

class PrismCode extends React.Component {
  componentDidMount() {
    Prism.highlightAll();
  }

  render() {
    return (
      <pre>
        <code className='language-javascript'>
          { this.props.code }
        </code>
      </pre>
    )
  }
}

const TabPane = Tabs.TabPane;
const Chart = ({ query, codeExample, render }) => (
  <Row gutter={24}>
    <QueryRenderer
      query={query}
      cubejsApi={cubejs(HACKER_NEWS_DATASET_API_KEY)}
      render={ ({ resultSet, error }) => {
        if (resultSet) {
          return [
            <Col span={12}>
              <div style={{ padding: "30px" }}>
                {render({ resultSet, error })}
              </div>
            </Col>,
            <Col span={12}>
              <div>
                <Tabs defaultActiveKey="query" type="card">
                  <TabPane tab="Query" key="query">
                    <PrismCode code={JSON.stringify(query, null, 2)} />
                  </TabPane>
                  <TabPane tab="Response" key="response">
                    <PrismCode code={JSON.stringify(resultSet, null, 2)} />
                  </TabPane>
                  <TabPane tab="Code" key="code">
                    <PrismCode code={codeExample} />
                  </TabPane>
                </Tabs>
              </div>
            </Col>
          ];
        }
        return <Spin style={{ textAlign: 'center' }} />;
      }}
    />
  </Row>
)

export default Chart;
