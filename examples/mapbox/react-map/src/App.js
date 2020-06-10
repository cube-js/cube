import React from 'react';
import './App.css';
import './body.css';
import 'antd/dist/antd.css';

import * as moment from 'moment';

import { ApolloProvider } from '@apollo/react-hooks';
import { Card, Col, Divider, Row, Layout, Slider, Tabs, Spin } from 'antd';
import * as Icon from '@ant-design/icons';

import cubejs from '@cubejs-client/core';
import { CubeProvider } from '@cubejs-client/react';
import client from './graphql/client';

import Header from './components/Header';
import Heatmap from './components/Heatmap';
import Choropleth from './components/Choropleth';

const API_URL = process.env.NODE_ENV === 'production' ? '' : "http://localhost:4000";
const CUBEJS_TOKEN =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1OTExNDkxODksImV4cCI6MTU5Mzc0MTE4OX0.TI-aVwcaYpS6jNbEZgWXFz3zUrhRsqQF7PIXnvmu4ow';
const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`,
});

const { TabPane } = Tabs;
class AppLayout extends React.Component {
  constructor() {
    super();
    this.state = {
      total: null,
      dates: {
        min: null,
        max: null,
        current: null,
      },
    };
  }

  componentDidMount = () => {
    cubejsApi
      .load({
        measures: ['stats.startDate', 'stats.endDate'],
      })
      .then((resultSet) => {
        this.setState({
          dates: {
            min: resultSet.tablePivot()[0]['stats.startDate'],
            max: resultSet.tablePivot()[0]['stats.endDate'],
            current: moment
              .unix(resultSet.tablePivot()[0]['stats.endDate'])
              .format('YYYY-MM-DD'),
          },
        });
      });
  };

  onSliderChange = (value) => {
    this.setState((prevState) => {
      return {
        ...prevState,
        dates: {
          ...prevState.dates,
          current: moment.unix(value).format('YYYY-MM-DD'),
        },
      };
    });
  };

  render() {
    if (!this.state.dates.min) {
      return (
        <Layout>
          <Layout.Content style={{ padding: '24px' }}>
            <Spin />
          </Layout.Content>
        </Layout>
      );
    }
    return (
      <React.Fragment>
        <Header />
        <Layout>
          <Layout.Content style={{ padding: '24px' }}>
            <Tabs defaultActiveKey='1' size='large'>
              <TabPane tab='choropleth' key='1'>
                <Row gutter={[16, 16]}>
                  <Col span={24}>
                    <Choropleth
                      cubejsApi={cubejsApi}
                      date={this.state.dates.current}
                    />
                    <div className='slider__container'>
                      <Slider
                        step={86400} //слайдер перевела в timestamp, соответственно, шаг = 1 день в секундах
                        defaultValue={this.state.dates.max}
                        min={this.state.dates.min}
                        max={this.state.dates.max}
                        tipFormatter={(value) =>
                          moment.unix(value).format('YYYY-MM-DD')
                        }
                        onChange={this.onSliderChange}
                        tooltipVisible
                      />
                    </div>
                  </Col>
                </Row>
              </TabPane>
              <TabPane tab='heatmap and clusters' key='2'>
                <Row gutter={[16, 16]}>
                  <Col span={24}>
                    <Heatmap cubejsApi={cubejsApi} />
                  </Col>
                </Row>
              </TabPane>
            </Tabs>
          </Layout.Content>
          <Layout.Sider style={{ background: 'white' }} width='300px'>
            <Card
              title={
                <React.Fragment>
                  <Icon.LinkOutlined /> Links
                </React.Fragment>
              }
              bordered={false}
            >
              <ul className='useful-links'>
                <li>
                  <a href='/' target='_blank'>
                    <img src='cube.png' />
                    Tutorial at Cube.JS
                  </a>
                </li>
                <li>
                  <a href='/' target='_blank'>
                    <img src='github.png' />
                    View source
                  </a>
                </li>
                <Divider />
                <li>
                  <a href='//cube.dev/docs/' target='_blank'>
                    <img src='cube.png' />
                    Cube.JS docs
                  </a>
                </li>
                <li>
                  <a href='//docs.mapbox.com/' target='_blank'>
                    <img src='mapbox.png' />
                    Mapbox docs
                  </a>
                </li>
                <li>
                  <a
                    href='//docs.mapbox.com/mapbox-gl-js/example/updating-choropleth/'
                    target='_blank'
                  >
                    <img src='mapbox.png' />
                    Mapbox Choropleth example
                  </a>
                </li>
                <li>
                  <a
                    href='//docs.mapbox.com/mapbox-gl-js/example/heatmap-layer/'
                    target='_blank'
                  >
                    <img src='mapbox.png' />
                    Mapbox Heatmap example
                  </a>
                </li>
                <li>
                  <a
                    href='//docs.mapbox.com/mapbox-gl-js/example/cluster/'
                    target='_blank'
                  >
                    <img src='mapbox.png' />
                    Mapbox Cluster example
                  </a>
                </li>
                <li>
                  <a href='//visgl.github.io/react-map-gl/' target='_blank'>
                    <img src='github.png' />
                    React Mapbox wrapper
                  </a>
                </li>
              </ul>
            </Card>
            <Card
              title={
                <React.Fragment>
                  <Icon.SmileOutlined /> Need help?
                </React.Fragment>
              }
              bordered={false}
            >
              <p>
                For help please use our&nbsp;
                <a href='//slack.cube.dev' target='_blank'>
                  Slack
                </a>
                &nbsp;channel or&nbsp;drop a mail on&nbsp;
                <a href='mailto:help@cube.dev'>help@cube.dev</a>.
              </p>
            </Card>
          </Layout.Sider>
        </Layout>
      </React.Fragment>
    );
  }
}

const App = ({ children }) => (
  <CubeProvider cubejsApi={cubejsApi}>
    <ApolloProvider client={client}>
      <AppLayout>{children}</AppLayout>
    </ApolloProvider>
  </CubeProvider>
);

export default App;
