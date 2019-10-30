import React from 'react';
import ReactDOM from 'react-dom';
import { HashRouter as Router, Route } from 'react-router-dom';
import DashboardPage from './pages/DashboardPage';
import App from './App';

ReactDOM.render(
  <Router>
    <App>
      <Route
        key="index"
        exact
        path="/"
        component={DashboardPage}
      />
    </App>
  </Router>,
  // eslint-disable-next-line no-undef
  document.getElementById('root')
);
