import React from 'react';
import ReactDOM from 'react-dom';
import { Router, Route } from 'react-router-dom';
import createHistory from 'history/createHashHistory';
import IndexPage from './IndexPage';
import ExplorePage from './ExplorePage';
import SchemaPage from './SchemaPage';
import App from './App';
import { page } from './events';

const history = createHistory({ basename: process.env.PUBLIC_URL });
history.listen((location) => {
  page(location);
});

ReactDOM.render(
  <Router history={history}>
    <App>
      <Route
        exact
        path="/"
        component={IndexPage}
      />
      <Route
        path="/explore"
        component={ExplorePage}
      />
      <Route
        path="/schema"
        component={SchemaPage}
      />
    </App>
  </Router>,
  // eslint-disable-next-line no-undef
  document.getElementById('root')
);
