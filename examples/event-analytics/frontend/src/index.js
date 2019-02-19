import React from 'react';
import ReactDOM from 'react-dom';
import { Router, Switch } from "react-router-dom";
import createHistory from 'history/createBrowserHistory'
import SectionRoute from './auth/components/SectionRoute';
import DashboardPage from './dashboard';
import ReportsPage from './reports';
import * as serviceWorker from './serviceWorker';

const history = createHistory()
history.listen((location, action) => {
  // Use setTimeout due to react-helmet issue
  // https://github.com/nfl/react-helmet/issues/189
  setTimeout(() => {
    window.snowplow('trackPageView');
  }, 0);
});

ReactDOM.render(
  <Router history={history}>
    <Switch>
      <SectionRoute
        exact
        path="/"
        title="Dashboard"
        component={DashboardPage}
      />
      <SectionRoute
        path="/reports"
        title="Reports"
        component={ReportsPage}
      />
    </Switch>
  </Router>
  ,
  document.getElementById('root')
);

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: http://bit.ly/CRA-PWA
serviceWorker.unregister();
