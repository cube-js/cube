import React, { Component } from 'react';
import "antd/dist/antd.css";
import "./index.css";
import { Row, Col, Card, Layout } from "antd";
import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/react';
import { Spin } from 'antd';
import { Chart, Axis, Tooltip, Geom, Coord, Legend } from 'bizcharts';
import moment from 'moment';
import GithubCorner from 'react-github-corner';
import { trackPageView } from './track';
import PrismCode from './PrismCode';

const dateRange = [
  moment().subtract(14,'d').format('YYYY-MM-DD'),
  moment().format('YYYY-MM-DD'),
];

const { Header, Footer, Sider, Content } = Layout;

const renderChart = (resultSet) => (
  <Chart scale={{ category: { tickCount: 8 } }} height={400} data={resultSet.chartPivot()} forceFit>
    <Axis name="category" label={{ formatter: val => moment(val).format("MMM DD") }} />
    {resultSet.seriesNames().map(s => (<Axis name={s.key} />))}
    <Tooltip crosshairs={{type : 'y'}} />
    {resultSet.seriesNames().map(s => (<Geom type="line" position={`category*${s.key}`} size={2} />))}
  </Chart>
);

const stackedBarChartData = (resultSet) => {
  const data = resultSet.pivot().map(
    ({ xValues, yValuesArray }) =>
      yValuesArray.map(([yValues, m]) => ({
        x: resultSet.axisValuesString(xValues, ', '),
        color: resultSet.axisValuesString([yValues[0]], ', '),
        [yValues[1]]: m && Number.parseFloat(m)
      }))
  ).reduce((a, b) => a.concat(b));

  return data;
}

const renderStackedBarChart = (resultSet, measure) => (
  <Chart height={400} data={stackedBarChartData(resultSet)} forceFit>
  <Legend />
    <Axis name="x" label={{ formatter: val => moment(val).format("MMM DD") }} />
    <Axis name={measure} />
    <Tooltip />
    <Geom type='intervalStack' position={`x*${measure}`} color="color" />
  </Chart>
);

const renderPieChart = (resultSet, measure) => (
  <Chart height={400} data={resultSet.chartPivot().map(v => ({ ...v, category: v.x }))} forceFit>
    <Coord type='theta' radius={0.75} />
    <Axis name={measure} />
    <Legend position='right' name="category" title={null} />
    <Tooltip showTitle={false}/>
    <Geom
      type="intervalStack"
      position={measure}
      color='x'>
    </Geom>
  </Chart>
);

const trackingCode = `import { fetch } from 'whatwg-fetch';
import cookie from 'component-cookie';
import uuidv4 from 'uuid/v4';

const trackPageView = () => {
  if (!cookie('aws_web_uid')) {
    cookie('aws_web_uid', uuidv4());
  }
  fetch(
    'https://4bfydqnx8i.execute-api.us-east-1.amazonaws.com/dev/collect',
    {
      method: 'POST',
      body: JSON.stringify({
        url: window.location.href,
        referrer: document.referrer,
        anonymousId: cookie('aws_web_uid'),
        eventType: 'pageView'
      }),
      headers: {
        'Content-Type': 'application/json'
      }
    }
  )
}`;

const lambdaCode = `const AWS = require('aws-sdk');
const { promisify } = require('util');

const kinesis = new AWS.Kinesis();

const putRecord = promisify(kinesis.putRecord.bind(kinesis));

const response = (body, status) => {
  return {
    statusCode: status || 200,
    body: body && JSON.stringify(body),
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Credentials': true,
      'Content-Type': 'application/json'
    }
  }
}

module.exports.collect = async (event, context) => {
  const body = JSON.parse(event.body);
  if (!body.anonymousId || !body.url || !body.eventType) {
    return response({
      error: 'anonymousId, url and eventType required'
    }, 400);
  }

  await putRecord({
    Data: JSON.stringify({
      anonymous_id: body.anonymousId,
      url: body.url,
      event_type: body.eventType,
      referrer: body.referrer,
      timestamp: (new Date()).toISOString(),
      source_ip: event.requestContext.identity.sourceIp,
      user_agent: event.requestContext.identity.userAgent
    }) + '\\n',
    PartitionKey: body.anonymousId,
    StreamName: 'event-collection'
  });

  return response();
};`;

const schemaCode = `cube(\`PageViews\`, {
  sql: \`select * from aws_web_analytics.aws_web_analytics_event_collection\`,

  measures: {
    count: {
      type: \`count\`
    },

    userCount: {
      sql: \`anonymous_id\`,
      type: \`countDistinct\`,
    }
  },

  dimensions: {
    url: {
      sql: \`url\`,
      type: \`string\`
    },

    anonymousId: {
      sql: \`anonymous_id\`,
      type: \`string\`
    },

    eventType: {
      sql: \`event_type\`,
      type: \`string\`
    },

    referrer: {
      sql: \`referrer\`,
      type: \`string\`
    },

    timestamp: {
      sql: \`from_iso8601_timestamp(timestamp)\`,
      type: \`time\`
    }
  }
});`;

const API_KEY = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjo0MDY3OH0.Vd-Qu4dZ95rVy9pKkyzy6Uxc5D-VOdTidCWYUVhKpYU';

class App extends Component {
  componentDidMount() {
    trackPageView();
  }

  render() {
    return [
      <Layout>
          <Header>
            <h2 style={{ color: '#fff' }}>AWS Web Analytics Dashboard</h2>
          </Header>
          <Content style={{ padding: '25px', margin: '25px' }}>
            <Row type="flex" justify="space-around" align="top" gutter={24}>
              <Col span={24} lg={12}>
                <Card title="Page Views" style={{ marginBottom: '24px' }}>
                  <QueryRenderer
                    query={{
                      "measures": [
                        "PageViews.count"
                      ],
                      "timeDimensions": [
                        {
                          "dimension": "PageViews.timestamp",
                          "dateRange": dateRange,
                          "granularity": "day"
                        }
                      ]
                    }}
                    cubejsApi={cubejs(API_KEY)}
                    render={({ resultSet }) => (
                      resultSet && renderChart(resultSet) || (<Spin />)
                    )}
                  />
                </Card>
              </Col>
              <Col span={24} lg={12}>
                <Card title="Unique Visitors" style={{ marginBottom: '24px' }}>
                  <QueryRenderer
                    query={{
                      "measures": [
                        "PageViews.userCount"
                      ],
                      "timeDimensions": [
                        {
                          "dimension": "PageViews.timestamp",
                          "dateRange": dateRange,
                          "granularity": "day"
                        }
                      ]
                    }}
                    cubejsApi={cubejs(API_KEY)}
                    render={({ resultSet }) => (
                      resultSet && renderChart(resultSet) || (<Spin />)
                    )}
                  />
                </Card>
              </Col>
              <Col lg={12} span={24}>
                <Card title="Visitor by Referrer" style={{ marginBottom: '24px' }}>
                  <QueryRenderer
                    query={{
                      "measures": [
                        "PageViews.userCount"
                      ],
                      "dimensions": [
                        "PageViews.referrer"
                      ],
                      "timeDimensions": [
                        {
                          "dimension": "PageViews.timestamp",
                          "dateRange": dateRange,
                          "granularity": "day"
                        }
                      ]
                    }}
                    cubejsApi={cubejs(API_KEY)}
                    render={({ resultSet }) => (
                      resultSet && renderStackedBarChart(resultSet, "PageViews.userCount") || (<Spin />)
                    )}
                  />
                </Card>
              </Col>
              <Col lg={12} span={24}>
                <Card title="Visitor by Referrer" style={{ marginBottom: '24px' }}>
                  <QueryRenderer
                    query={{
                      "measures": [
                        "PageViews.userCount"
                      ],
                      "dimensions": [
                        "PageViews.referrer"
                      ],
                      "timeDimensions": [
                        {
                          "dimension": "PageViews.timestamp",
                          "dateRange": dateRange
                        }
                      ]
                    }}
                    cubejsApi={cubejs(API_KEY)}
                    render={({ resultSet }) => (
                      resultSet && renderPieChart(resultSet, "PageViews.userCount") || (<Spin />)
                    )}
                  />
                </Card>
              </Col>
              <Col lg={12} span={24}>
                <Card title="Architecture" style={{ marginBottom: '24px', textAlign: 'center' }}>
                  <img src="./architecture.png" style={{ width: '100%', maxWidth: '500px' }}/>
                </Card>
              </Col>
              <Col lg={12} span={24}>
                <Card title="Page contains this tracking code" style={{ marginBottom: '24px' }}>
                  <PrismCode code={trackingCode}/>
                </Card>
              </Col>
              <Col lg={12} span={24}>
                <Card title="Events collected using this Lambda function" style={{ marginBottom: '24px' }}>
                  <PrismCode code={lambdaCode}/>
                </Card>
              </Col>
              <Col lg={12} span={24}>
                <Card title="Cube.js schema used for analytic querying" style={{ marginBottom: '24px' }}>
                  <PrismCode code={schemaCode}/>
                </Card>
              </Col>
              <Col span={24} style={{ textAlign: 'center' }}>
                <a href="https://statsbot.co/cubejs/">
                  <img src='./powered-by-cubejs-color.svg' style={{ width: 200 }}/>
                </a>
              </Col>
            </Row>
          </Content>
        </Layout>,
        <GithubCorner size={120} href="https://github.com/statsbotco/cubejs-client/tree/master/examples/aws-web-analytics" />
    ];
  }
}

export default App;
