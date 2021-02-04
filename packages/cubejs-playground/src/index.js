import ReactDOM from 'react-dom';
import { Router, Route } from 'react-router-dom';
import { createHashHistory } from 'history';

import App from './App';
import { page } from './events';
import TemplateGalleryPage from './TemplateGallery/TemplateGalleryPage';
import {
  ExplorePage,
  DashboardPage,
  ConnectionWizardPage,
  SchemaPage,
  IndexPage,
} from './pages';
import SecurityContextProvider from './components/SecurityContext/SecurityContextProvider';

const history = createHashHistory();
history.listen((location) => {
  page(location);
});

ReactDOM.render(
  <Router history={history}>
    <App>
      <Route key="index" exact path="/" component={IndexPage} />
      <Route
        key="build"
        path="/build"
        component={(props) => {
          return (
            <SecurityContextProvider>
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
  </Router>,
  // eslint-disable-next-line no-undef
  document.getElementById('playground-root')
);
