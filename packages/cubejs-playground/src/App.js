/* eslint-disable no-undef,react/jsx-no-target-blank */
import { Component } from 'react';
import '@ant-design/compatible/assets/index.css';
import { Layout, Alert, notification, Spin } from 'antd';
import { fetch } from 'whatwg-fetch';
import { withRouter } from 'react-router';

import Header from './components/Header';
import { event, setAnonymousId } from './events';
import GlobalStyles from './components/GlobalStyles';
import { AppContext } from './hooks';
import './index.less';
import './index.css';

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

  state = {
    fatalError: null,
    context: null,
    slowQuery: false,
    isPreAggregationBuildInProgress: false,
  };

  async componentDidMount() {
    const { history } = this.props;

    window['__cubejsPlayground'] = {
      ...window['__cubejsPlayground'],
      onQueryLoad: (data) => {
        let resultSet;
        
        if (data?.resultSet !== undefined) {
          resultSet = data.resultSet;
        } else {
          resultSet = data;
        }
        
        if (resultSet) {
          const { loadResponse } = resultSet.serialize();

          this.setState({ slowQuery: Boolean(loadResponse.slowQuery) });
        }
      },
      onQueryProgress: (progress) => {
        this.setState({
          isPreAggregationBuildInProgress: Boolean(progress?.stage?.stage.includes(
            'pre-aggregation'
          )),
        });
      }
    };

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
    const context = await res.json();
    setAnonymousId(context.anonymousId, {
      coreServerVersion: context.coreServerVersion,
      projectFingerprint: context.projectFingerprint,
    });
    this.setState({ context }, () => {
      if (context.shouldStartConnectionWizardFlow) {
        history.push('/connection');
      }
    });
  }

  componentDidCatch(error, info) {
    event('Playground Error', {
      error: (error.stack || error).toString(),
      info: info.toString(),
    });
  }

  render() {
    const { context, fatalError, slowQuery, isPreAggregationBuildInProgress } =
      this.state || {};
    const { location, children } = this.props;

    if (context == null) {
      return <Spin />;
    }

    return (
      <AppContext.Provider
        value={{
          slowQuery,
          isPreAggregationBuildInProgress,
        }}
      >
        <Layout style={{ height: '100%' }}>
          <GlobalStyles />
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
      </AppContext.Provider>
    );
  }
}

export default withRouter(App);
