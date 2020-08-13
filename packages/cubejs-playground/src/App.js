/* eslint-disable no-undef,react/jsx-no-target-blank */
import React, { Component } from 'react';
import * as PropTypes from 'prop-types';
import '@ant-design/compatible/assets/index.css';
import './index.less';
import './index.css';
import { Layout, Alert, notification } from 'antd';
import { fetch } from 'whatwg-fetch';
import { withRouter } from 'react-router';
import Header from './components/Header';
import { event, setAnonymousId } from './events';

const selectedTab = (pathname) => {
  if (pathname === '/template-gallery') {
    return ['/dashboard'];
  } else {
    return [pathname];
  }
};

class App extends Component {
  static getDerivedStateFromError(error) {
    return { fatalError: error };
  }

  async componentDidMount() {
    window.addEventListener('unhandledrejection', (promiseRejectionEvent) => {
      const error = promiseRejectionEvent.reason;
      console.log(error);
      const e = (error.stack || error).toString();
      event('Playground Error', {
        error: e,
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
    setAnonymousId(result.anonymousId, {
      coreServerVersion: result.coreServerVersion,
      projectFingerprint: result.projectFingerprint,
    });
  }

  componentDidCatch(error, info) {
    event('Playground Error', {
      error: (error.stack || error).toString(),
      info: info.toString(),
    });
  }

  render() {
    const { fatalError } = this.state || {};
    const { location, children } = this.props;
    return (
      <Layout style={{ height: '100%' }}>
        <Header selectedKeys={selectedTab(location.pathname)} />
        <Layout.Content style={{ height: '100%' }}>
          {fatalError ? (
            <Alert
              message="Error occured while rendering"
              description={fatalError.stack}
              type="error"
            />
          ) : (
            children
          )}
        </Layout.Content>
      </Layout>
    );
  }
}

App.propTypes = {
  location: PropTypes.object.isRequired,
  children: PropTypes.array,
};

App.defaultProps = {
  children: [],
};

export default withRouter(App);
