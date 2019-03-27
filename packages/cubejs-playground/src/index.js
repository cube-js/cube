import React from 'react';
import ReactDOM from 'react-dom';
import { Router, Route } from 'react-router-dom';
import { createHashHistory } from 'history';
import IndexPage from './IndexPage';
import ExplorePage from './ExplorePage';
import SchemaPage from './SchemaPage';
import DashboardPage from './DashboardPage';
import App from './App';
import { page } from './events';

const history = createHashHistory({ basename: process.env.PUBLIC_URL });
history.listen((location) => {
  page(location);
});

ReactDOM.render(
  <Router history={history}>
    <App>
      <Route
        key="index"
        exact
        path="/"
        component={IndexPage}
      />
      <Route
        key="explore"
        path="/explore"
        component={ExplorePage}
      />
      <Route
        key="schema"
        path="/schema"
        component={SchemaPage}
      />
      <Route
        key="dashboard"
        path="/dashboard"
        component={DashboardPage}
      />
    </App>
  </Router>,
  // eslint-disable-next-line no-undef
  document.getElementById('root')
);
