import React from "react";

import cubejs from "@cubejs-client/core";
import { CubeProvider } from "@cubejs-client/react";

import "antd/dist/antd.min.css";
import { Tabs, Layout, Card, Tooltip } from "antd";
import { Scrollbars } from 'react-custom-scrollbars';

import Choropleth from './components/Choropleth';
import Heatmap from './components/Heatmap';
import ClickEvent from './components/ClickEvent';
import Points from './components/Points';

const cubejsApi = cubejs(process.env.REACT_APP_CUBEJS_TOKEN, {
  apiUrl: process.env.REACT_APP_API_URL
});

const App = () => (
  <CubeProvider cubejsApi={cubejsApi}>
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
            title='Learn more about Mapbox'
            bordered={false}
          >
            <ul className='useful-links'>
              <li>
                <a href='//docs.mapbox.com/' target='_blank' rel="noopener noreferrer">
                  Mapbox docs
                  </a>
              </li>
              <li>
                <a
                  href='//docs.mapbox.com/mapbox-gl-js/example/updating-choropleth/'
                  target='_blank'
                  rel="noopener noreferrer"
                >
                  Mapbox Choropleth example
                  </a>
              </li>
              <li>
                <a
                  href='//docs.mapbox.com/mapbox-gl-js/example/heatmap-layer/'
                  target='_blank'
                  rel="noopener noreferrer"
                >
                  Mapbox Heatmap example
                  </a>
              </li>
              <li>
                <a
                  href='//docs.mapbox.com/mapbox-gl-js/example/cluster/'
                  target='_blank'
                  rel="noopener noreferrer"
                >
                  Mapbox Cluster example
                  </a>
              </li>
              <li>
                <a href='//visgl.github.io/react-map-gl/' target='_blank' rel="noopener noreferrer">
                  React Mapbox wrapper
                  </a>
              </li>
            </ul>
          </Card>
        </Scrollbars>
      </div>
    </Layout>
  </CubeProvider>
);

export default App;

