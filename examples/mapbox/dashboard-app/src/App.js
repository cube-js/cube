import React from "react";

import cubejs from "@cubejs-client/core";
import { CubeProvider } from "@cubejs-client/react";

import "antd/dist/antd.css";
import { Tabs, Layout, Card, Tooltip } from "antd";
import * as Icon from '@ant-design/icons';
import { Scrollbars } from 'react-custom-scrollbars';

import Header from "./components/Header";
import Choropleth from './components/Choropleth';
import Heatmap from './components/Heatmap';
import ClickEvent from './components/ClickEvent';
import Points from './components/Points';


const API_URL = process.env.NODE_ENV === 'production' ? '' : "http://localhost:4000";
const CUBEJS_TOKEN =
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1OTE4NjM4MDEsImV4cCI6MTU5NDQ1NTgwMX0.NW6yiMgiZz_LCnkRn-dunzyqTRO9K7L-k5FpNn2-iCA";
const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`
});

const App = () => (
  <CubeProvider cubejsApi={cubejsApi}>
    <Header />
    <Layout className="mapbox">
      <div className="mapbox__content">
        <Tabs defaultActiveKey="0">
          <Tabs.TabPane tab={(<React.Fragment><Tooltip placement="bottom" title='heatmap'>location</Tooltip></React.Fragment>)} key={0}>
            <Heatmap />
          </Tabs.TabPane>
          <Tabs.TabPane tab={(<React.Fragment><Tooltip placement="bottom" title='points distribution'> personal rating</Tooltip></React.Fragment>)} key={1}>
            <Points />
          </Tabs.TabPane>
          <Tabs.TabPane tab={(<React.Fragment><Tooltip placement="bottom" title='click event'>q&amp;a</Tooltip></React.Fragment>)} key={2}>
            <ClickEvent cubejsApi={cubejsApi} />
          </Tabs.TabPane>
          <Tabs.TabPane tab={(<React.Fragment><Tooltip placement="bottom" title='choropleth'>rating by country</Tooltip></React.Fragment>)} key={3}>
            <Choropleth />
          </Tabs.TabPane>
        </Tabs>
      </div>
      <div className="mapbox__sider" width={300}>
        <Scrollbars style={{ minHeight: 300 }}>
          <Card
            title='Mapbox Example'
            bordered={false}
            className="mapbox__card"
          >
            <p>Learn how to visualize different types of data with <a href="//mapbox.com" target="_blank">Mapbox</a>, <a href="//cube.dev" target="_blank">Cube.js</a> and explore <a href="//console.cloud.google.com/marketplace/details/stack-exchange/stack-overflow" target="_blank">Stackoverflow</a> audience.</p>
          </Card>
          <Card
            title='Links'
            bordered={false}
          >
            <ul className='useful-links'>
              <li>
                <a href='/' target='_blank'>
                  Tutorial at Cube.JS
                  </a>
              </li>
              <li>
                <a href='//github.com/cube-js/cube.js/tree/master/examples/mapbox' target='_blank'>
                  View source
                  </a>
              </li>
              <li>
                <a href='//cube.dev/docs/' target='_blank'>
                  Cube.JS docs
                  </a>
              </li>
              <li>
                <a href='//docs.mapbox.com/' target='_blank'>
                  Mapbox docs
                  </a>
              </li>
              <li>
                <a
                  href='//docs.mapbox.com/mapbox-gl-js/example/updating-choropleth/'
                  target='_blank'
                >
                  Mapbox Choropleth example
                  </a>
              </li>
              <li>
                <a
                  href='//docs.mapbox.com/mapbox-gl-js/example/heatmap-layer/'
                  target='_blank'
                >
                  Mapbox Heatmap example
                  </a>
              </li>
              <li>
                <a
                  href='//docs.mapbox.com/mapbox-gl-js/example/cluster/'
                  target='_blank'
                >
                  Mapbox Cluster example
                  </a>
              </li>
              <li>
                <a href='//visgl.github.io/react-map-gl/' target='_blank'>
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
                &nbsp;channel.
              </p>
          </Card>

          <div className="mapbox__copyright">
            Created by Cube.js️
              </div>
        </Scrollbars>
      </div>
    </Layout>
  </CubeProvider>
);

export default App;

