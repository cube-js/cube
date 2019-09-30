import React from 'react';
import ReactDOM from 'react-dom';
import { HashRouter as Router, Route } from 'react-router-dom';
import cubejs from '@cubejs-client/core';
import ExplorePage from './pages/ExplorePage';
import DashboardPage from './pages/DashboardPage';
import App from './App';

// change to your actual endpoint
const API_URL = undefined;
// should be refreshed by your backend API endpoint. More info: https://cube.dev/docs/security
const CUBEJS_TOKEN = undefined;

const cubejsApi = cubejs(
  CUBEJS_TOKEN,
  { apiUrl: `${API_URL}/cubejs-api/v1` }
);

ReactDOM.render(
  <Router>
    <App>
      <Route
        key="index"
        exact
        path="/"
        render={(props) => <DashboardPage {...props} cubejsApi={cubejsApi}/>}
      />
      <Route
        key="explore"
        path="/explore"
        render={(props) => <ExplorePage {...props} cubejsApi={cubejsApi}/>}
      />
    </App>
  </Router>,
  // eslint-disable-next-line no-undef
  document.getElementById('root')
);
