/* eslint-disable no-undef,react/jsx-no-target-blank */
import { Component } from 'react';
import '@ant-design/compatible/assets/index.css';
import { Layout, Alert } from 'antd';
import { fetch } from 'whatwg-fetch';
import { RouteComponentProps, withRouter } from 'react-router';

import Header from './components/Header';
import GlobalStyles from './components/GlobalStyles';
import { CubeLoader } from './atoms';
import { event, setAnonymousId } from './events';
import './index.less';

const selectedTab = (pathname) => {
  if (pathname === '/template-gallery') {
    return ['/dashboard'];
  } else {
    return [pathname];
  }
};

type TAppState = {
  fatalError: Error | null;
  context: any;
  showLoader: boolean;
};

class App extends Component<RouteComponentProps, TAppState> {
  static getDerivedStateFromError(error) {
    return { fatalError: error };
  }

  state: TAppState = {
    fatalError: null,
    context: null,
    showLoader: false,
  };

  async componentDidMount() {
    const { history } = this.props;

    setTimeout(() => this.setState({ showLoader: true }), 700);

    window.addEventListener('unhandledrejection', (promiseRejectionEvent) => {
      const error = promiseRejectionEvent.reason;
      console.log(error);
      const e = (error.stack || error).toString();
      event('Playground Error', {
        error: e,
      });
    });

    const res = await fetch('/playground/context');
    const context = await res.json();
    setAnonymousId(context.anonymousId, {
      coreServerVersion: context.coreServerVersion,
      projectFingerprint: context.projectFingerprint,
      isDocker: Boolean(context.isDocker)
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
    const { context, fatalError, showLoader } = this.state;
    const { location, children } = this.props;

    if (!showLoader) {
      return null;
    }

    if (context == null) {
      return <CubeLoader />;
    }

    if (fatalError) {
      console.log(fatalError.stack)
    }

    return (
      <Layout style={{ height: '100%' }}>
        <GlobalStyles />
        <Header selectedKeys={selectedTab(location.pathname)} />
        <Layout.Content style={{ height: '100%' }}>
          {fatalError ? (
            <Alert
              message="Error occured while rendering"
              description={fatalError.stack || ''}
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

export default withRouter(App);
