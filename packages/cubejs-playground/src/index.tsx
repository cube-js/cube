import ReactDOM from 'react-dom';
import { Router, Route } from 'react-router-dom';
import { createHashHistory } from 'history';

import App from './App';
import { page } from './events';
import {
  ExplorePage,
  ConnectionWizardPage,
  SchemaPage,
  IndexPage,
  ConnectToBiPage,
  FrontendIntegrationsPage,
} from './pages';
import { SecurityContextProvider } from './components/SecurityContext/SecurityContextProvider';
import { AppContextProvider } from './components/AppContext';

const history = createHashHistory();
history.listen((location) => {
  const { search, ...props } = location;
  page(props);
});

async function onTokenPayloadChange(payload: Record<string, any>, token) {
  if (token != null) {
    return token;
  }

  const response = await fetch('playground/token', {
    method: 'post',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      payload,
    }),
  });
  const json = await response.json();
  return json.token;
}

if (
  window.location.port === '3080' &&
  window.location.pathname.includes('/playground/live-preview/start')
) {
  fetch(window.location.pathname).then(() => window.close());
}

ReactDOM.render(
  <Router history={history}>
    <AppContextProvider
      playgroundContext={{
        isCloud: false,
      }}
    >
      <App>
        <Route key="index" exact path="/" component={IndexPage} />
        <Route
          key="build"
          path="/build"
          component={(props) => {
            return (
              <SecurityContextProvider
                onTokenPayloadChange={onTokenPayloadChange}
              >
                <ExplorePage {...props} />
              </SecurityContextProvider>
            );
          }}
        />
        <Route key="schema" path="/schema" component={SchemaPage} />
        <Route
          key="connection"
          path="/connection"
          component={() => <ConnectionWizardPage history={history} />}
        />
        <Route
          key="connect-to-bi"
          path="/connect-to-bi"
          component={ConnectToBiPage}
        />
        <Route
          key="frontend-integrations"
          path="/frontend-integrations"
          component={FrontendIntegrationsPage}
        />
      </App>
    </AppContextProvider>
  </Router>,
  // eslint-disable-next-line no-undef
  document.getElementById('playground-root')
);
