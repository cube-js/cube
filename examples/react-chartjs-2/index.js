import React from 'react';
import ReactDOM from 'react-dom';
import cubejs from 'cubejs-client';
import JSONPretty from 'react-json-pretty';
import { QueryRenderer } from '@cubejs-client/react';
import { Pie, Line, Bar } from 'react-chartjs-2';
import { Layout, Row, Col, Menu } from 'antd';
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

const { Header, Footer, Sider, Content } = Layout;
const App = () => (
  <Layout>
    <Header style={{ background: "#fff" }}>
      <Menu
        mode="horizontal"
        defaultSelectedKeys={['2']}
        style={{ lineHeight: '64px' }}
      >
        <Menu.Item key="1">Line</Menu.Item>
        <Menu.Item key="2">Bar</Menu.Item>
        <Menu.Item key="3">Pie</Menu.Item>
      </Menu>
    </Header>
    <Content style={{ padding: '0 50px', margin: '50px 0' }}>
      <Chart
        query={{ measures: ['Stories.count'], dimensions: ['Stories.category'] }}
        render={ ({ resultSet }) => (
          <Pie data={toChartjsData('pie', resultSet)} />
        )}
      />
      <Chart
        query={{ measures: ['Stories.count'], timeDimensions: [{dimension: 'Stories.time', granularity: 'month', dateRange: ["2015-01-01", "2015-08-01"] }] }}
        render={ ({ resultSet }) => (
          <Line data={toChartjsData('line', resultSet)} options={{ scales: { xAxes: [{ type: `time`, time: { unit: 'month' }}] }}} />
        )}
      />
      <Chart
        query={{
          measures: ['Stories.count', 'Stories.totalScore'],
          timeDimensions: [{ dimension: 'Stories.time', granularity: 'month', dateRange: ["2015-01-01", "2015-08-01"] }]
        }}
        render={ ({ resultSet }) => (
          <Bar data={toChartjsData('bar', resultSet)} options={{ scales: { xAxes: [{ type: `time`, time: { unit: 'month' }}] }}} />
        )}
      />
    </Content>
  </Layout>
)

ReactDOM.render(<App />, document.getElementById('root'));
