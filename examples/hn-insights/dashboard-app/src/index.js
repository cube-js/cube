import React from 'react';
import ReactDOM from 'react-dom';
import { Router, Route } from 'react-router-dom';
import { createHashHistory } from 'history';
import IndexPage from './IndexPage';
import StoryPage from './StoryPage';
import App from './App';

const history = createHashHistory();
history.listen((location) => {
  // page(location);
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
        key="story"
        path="/stories/:storyId"
        component={StoryPage}
      />
    </App>
  </Router>,
  // eslint-disable-next-line no-undef
  document.getElementById('root')
);
