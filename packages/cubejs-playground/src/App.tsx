/* eslint-disable no-undef,react/jsx-no-target-blank */
import { Component, useEffect } from 'react';
import '@ant-design/compatible/assets/index.css';
import { Layout, Alert } from 'antd';
import { RouteComponentProps, withRouter } from 'react-router-dom';
import styled from 'styled-components';

import Header from './components/Header/Header';
import GlobalStyles from './components/GlobalStyles';
import { CubeLoader } from './atoms';
import {
  event,
  setAnonymousId,
  setTracker,
  setTelemetry,
  trackImpl,
} from './events';
import { AppContextConsumer, PlaygroundContext } from './components/AppContext';
import { useAppContext } from './hooks';
import { LivePreviewContextProvider } from './components/LivePreviewContext/LivePreviewContextProvider';

console.log('>>>', 'hello')

const selectedTab = (pathname) => {
  if (pathname === '/template-gallery') {
    return ['/dashboard'];
  } else {
    return [pathname];
  }
};

const StyledLayoutContent = styled(Layout.Content)`
  height: 100%;
`;

type AppState = {
  fatalError: Error | null;
  context: PlaygroundContext | null;
  showLoader: boolean;
  isAppContextSet: boolean;
};

class App extends Component<RouteComponentProps, AppState> {
  static getDerivedStateFromError(error) {
    return { fatalError: error };
  }

  state: AppState = {
    fatalError: null,
    context: null,
    showLoader: false,
    isAppContextSet: false,
  };

  async componentDidMount() {
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

    setTelemetry(context.telemetry);
    setTracker(trackImpl);
    setAnonymousId(context.anonymousId, {
      coreServerVersion: context.coreServerVersion,
      projectFingerprint: context.projectFingerprint,
      isDocker: Boolean(context.isDocker),
      dockerVersion: context.dockerVersion,
    });

    this.setState({ context });
  }

  componentDidCatch(error, info) {
    event('Playground Error', {
      error: (error.stack || error).toString(),
      info: info.toString(),
    });
  }

  render() {
    const { location, children } = this.props;
    const { context, fatalError, isAppContextSet, showLoader } = this.state;

    if (context != null && !isAppContextSet) {
      return (
        <>
          <ContextSetter context={context} />
          <AppContextConsumer
            onReady={() => this.setState({ isAppContextSet: true })}
          />
        </>
      );
    }

    if (context == null && !isAppContextSet) {
      return showLoader ? <CubeLoader /> : null;
    }

    if (fatalError) {
      console.log(fatalError.stack);
    }

    return (
      <LivePreviewContextProvider
        disabled={context!.livePreview == null || !context!.livePreview}
      >
        <Layout>
          <GlobalStyles />

          <Header selectedKeys={selectedTab(location.pathname)} />

          <StyledLayoutContent>
            {fatalError ? (
              <Alert
                message="Error occured while rendering"
                description={fatalError.stack || ''}
                type="error"
              />
            ) : (
              children
            )}
          </StyledLayoutContent>
        </Layout>
      </LivePreviewContextProvider>
    );
  }
}

type ContextSetterProps = {
  context: PlaygroundContext;
};

function ContextSetter({ context }: ContextSetterProps) {
  const { setContext } = useAppContext();

  useEffect(() => {
    if (context !== null) {
      setContext({
        ready: true,
        playgroundContext: {
          ...context,
          isCloud: false,
        },
        identifier: context.identifier,
      });
    }
  }, [context]);

  return null;
}

export default withRouter(App);
