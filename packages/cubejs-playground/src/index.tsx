import ReactDOM from 'react-dom';
import { Router, Route } from 'react-router-dom';
import { createHashHistory } from 'history';

import App from './App';
import { page } from './events';
import { TemplateGalleryPage } from './pages';
import {
  ExplorePage,
  DashboardPage,
  ConnectionWizardPage,
  SchemaPage,
  IndexPage,
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

  const response = await fetch('/playground/token', {
    method: 'post',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      payload
    }),
  });
  const json = await response.json();
  return json.token;
}

ReactDOM.render(
  <Router history={history}>
    <AppContextProvider isCloud={false}>
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
        <Route key="dashboard" path="/dashboard" component={DashboardPage} />
        <Route
          key="template-gallery"
          path="/template-gallery"
          component={TemplateGalleryPage}
        />
      </App>
    </AppContextProvider>
  </Router>,
  // eslint-disable-next-line no-undef
  document.getElementById('playground-root')
);
