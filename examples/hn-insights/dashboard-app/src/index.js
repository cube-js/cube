import React from 'react';
import ReactDOM from 'react-dom';
import { Router, Route } from 'react-router-dom';
import { createHashHistory } from 'history';
import cubejs from "@cubejs-client/core";
import IndexPage from './IndexPage';
import StoryPage from './StoryPage';
import StatisticsPage from './StatisticsPage';
import App from './App';

const history = createHashHistory();
history.listen((location) => {
  // page(location);
});

const API_URL =
  process.env.NODE_ENV === 'production' ?
    'https://lrb4ihnj8c.execute-api.us-east-1.amazonaws.com/dev' :
    'http://localhost:4000';
const cubejsApi = cubejs(
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.uCPvS2oOGbFgzqHQUHuU6CFB5jHzvRMOmbmh5LWgX-Q",
  {
    apiUrl: API_URL + "/cubejs-api/v1"
  }
);

ReactDOM.render(
  <Router history={history}>
    <App>
      <Route
        key="index"
        exact
        path="/"
        render={(props) => <IndexPage cubejsApi={cubejsApi} {...props} />}
      />
      <Route
        key="story"
        path="/stories/:storyId"
        render={(props) => <StoryPage cubejsApi={cubejsApi} {...props} />}
      />
      <Route
        key="statistics"
        path="/statistics"
        render={(props) => <StatisticsPage cubejsApi={cubejsApi} {...props} />}
      />
    </App>
  </Router>,
  // eslint-disable-next-line no-undef
  document.getElementById('root')
);
