import React, { Component } from 'react';
import "antd/dist/antd.css";
import "./index.css";
import { Row, Col, Card, Layout } from "antd";
import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/react';
import { Spin } from 'antd';
import { Chart, Axis, Tooltip, Geom, Coord, Legend } from 'bizcharts';
import moment from 'moment';
import { trackPageView } from './track';

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

const API_KEY = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjo0MDY3OH0.Vd-Qu4dZ95rVy9pKkyzy6Uxc5D-VOdTidCWYUVhKpYU';

class App extends Component {
  componentDidMount() {
    trackPageView();
  }

  render() {
    return (
      <Layout>
          <Header>
            <h2 style={{ color: '#fff' }}>AWS Web Analytics Dashboard</h2>
          </Header>
          <Content style={{ padding: '25px', margin: '25px' }}>
            <Row type="flex" justify="space-around" align="middle" gutter={24}>
              <Col lg={12} md={24}>
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
              <Col lg={12} md={24}>
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
              <Col lg={12} md={24}>
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
              <Col lg={12} md={24}>
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
              <Col span={24}>
                <Card title="Architecture" style={{ marginBottom: '24px', textAlign: 'center' }}>
                  <img src="/architecture.png" style={{ width: '100%', maxWidth: '500px' }}/>
                </Card>
              </Col>
            </Row>
          </Content>
        </Layout>
    );
  }
}

export default App;
