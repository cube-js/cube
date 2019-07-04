/* eslint-disable no-undef */
import React, { Component } from 'react';
import { Link } from "react-router-dom";
import * as PropTypes from 'prop-types';
import "antd/dist/antd.css";
import "./index.css";
import {
  Layout, Menu, Alert, notification
} from "antd";
import { fetch } from 'whatwg-fetch';
import { withRouter } from "react-router";
import { event } from './events';

const { Header, Content } = Layout;

class App extends Component {
  async componentDidMount() {
    window.addEventListener("unhandledrejection", (promiseRejectionEvent) => {
      const error = promiseRejectionEvent.reason;
      console.log(error);
      const e = (error.stack || error).toString();
      event('Playground Error', {
        error: e
      });
      notification.error({
        message: (
          <span>
            <b>Error</b>
            &nbsp;😢
            <div>
              Ask about it in&nbsp;
              <a
                href="https://slack.cube.dev"
                target="_blank"
                rel="noopener noreferrer"
              >
                Slack
              </a>
              . These guys know how to fix this for sure!
            </div>
          </span>
        ),
        description: e,
      });
    });

    const res = await fetch('/playground/context');
    const result = await res.json();
    if (window.analytics) {
      window.analytics.identify(result.anonymousId, {
        coreServerVersion: result.coreServerVersion
      });
    }
  }

  static getDerivedStateFromError(error) {
    return { fatalError: error };
  }

  componentDidCatch(error, info) {
    event('Playground Error', {
      error: (error.stack || error).toString(),
      info: info.toString()
    });
  }

  render() {
    const { fatalError } = this.state || {};
    const { location, children } = this.props;
    return (
      <Layout style={{ height: '100%' }}>
        <Header style={{ padding: '0 32px' }}>
          <div style={{ float: 'left' }}>
            <img src="./cubejs-logo.svg" style={{ display: 'inline', width: 50 }} alt="" />
          </div>
          <Menu
            theme="dark"
            mode="horizontal"
            selectedKeys={[location.pathname]}
            style={{ lineHeight: '64px' }}
          >
            <Menu.Item key="/explore"><Link to="/explore">Explore</Link></Menu.Item>
            <Menu.Item key="/dashboard"><Link to="/dashboard">Dashboard</Link></Menu.Item>
            <Menu.Item key="/schema"><Link to="/schema">Schema</Link></Menu.Item>
          </Menu>
        </Header>
        <Content style={{ height: '100%' }}>
          {fatalError ? (
            <Alert
              message="Error occured while rendering"
              description={fatalError.stack}
              type="error"
            />
          ) : children}
        </Content>
      </Layout>
    );
  }
}

App.propTypes = {
  location: PropTypes.object.isRequired,
  children: PropTypes.array
};

App.defaultProps = {
  children: []
};

export default withRouter(App);
