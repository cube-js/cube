/* eslint-disable no-undef,react/jsx-no-target-blank */
import '@ant-design/compatible/assets/index.css';
import { Alert, Layout } from 'antd';
import { Component, PropsWithChildren, useEffect } from 'react';
import { RouteComponentProps, withRouter } from 'react-router-dom';
import styled from 'styled-components';
import { Root } from '@cube-dev/ui-kit';

import { CubeLoader } from './atoms';
import { AppContextConsumer, PlaygroundContext } from './components/AppContext';
import GlobalStyles from './components/GlobalStyles';
import Header from './components/Header/Header';
import { LivePreviewContextProvider } from './components/LivePreviewContext/LivePreviewContextProvider';
import {
  event,
  setAnonymousId,
  setTelemetry,
  setTracker,
  trackImpl,
} from './events';
import { useAppContext } from './hooks';
import { QUERY_BUILDER_COLOR_TOKENS } from './QueryBuilderV2';

const StyledLayoutContent = styled(Layout.Content)`
  height: 100%;
`;

type AppState = {
  fatalError: Error | null;
  context: PlaygroundContext | null;
  showLoader: boolean;
  isAppContextSet: boolean;
};

const ROOT_STYLES = {
  height: 'min 100vh',
  display: 'grid',
  gridTemplateRows: 'min-content 1fr',
  ...QUERY_BUILDER_COLOR_TOKENS,
};

class App extends Component<PropsWithChildren<RouteComponentProps>, AppState> {
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

    const res = await fetch('playground/context');
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
        disabled={!context?.livePreview}
      >
        <Root publicUrl="." styles={ROOT_STYLES}>
          <GlobalStyles />

          <Header selectedKeys={[location.pathname]} />

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
        </Root>
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
