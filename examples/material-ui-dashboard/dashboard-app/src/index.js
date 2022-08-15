import React from 'react';
import ReactDOM from 'react-dom';
import App from './App';
import { HashRouter as Router, Route, Switch, Redirect } from 'react-router-dom';
import DashboardPage from './pages/DashboardPage';
import DataTablePage from './pages/DataTablePage';
import UsersPage from './pages/UsersPage';

import createExampleWrapper from "@cube-dev/example-wrapper";

const exampleDescription = {
  title: "Material UI Dashboard",
  text: `<p>This live demo shows a Material UI dashboard created with React and Cube.</p>
    <p>
      Follow the <a href="https://material-ui-dashboard.cube.dev/">tutorial</a> or explore 
      the <a href="https://github.com/cube-js/cube.js/tree/master/examples/material-ui-dashboard">source code</a>
      to learn more.
    </p>`
};
createExampleWrapper(exampleDescription);

ReactDOM.render(
  <React.StrictMode>
    <Router>
      <App>
        <Switch>
          <Redirect exact from="/" to="/dashboard" />
          <Route key="index" exact path="/dashboard" component={DashboardPage} />
          <Route key="table" path="/orders" component={DataTablePage} />
          <Route key="table" path="/user/:id" component={UsersPage} />
          <Redirect to="/dashboard" />
        </Switch>
      </App>
    </Router>
  </React.StrictMode>,
  document.getElementById('root')
);
