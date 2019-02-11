import React from 'react';
import ReactDOM from 'react-dom';
import { BrowserRouter, Switch } from "react-router-dom";
import SectionRoute from './auth/components/SectionRoute';
import DashboardPage from './dashboard';
import ReportsPage from './reports';
import * as serviceWorker from './serviceWorker';

ReactDOM.render(
  <BrowserRouter>
    <Switch>
      <SectionRoute exact path="/" component={DashboardPage} />
      <SectionRoute path="/reports" component={ReportsPage} />
    </Switch>
  </BrowserRouter>
  ,
  document.getElementById('root')
);

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: http://bit.ly/CRA-PWA
serviceWorker.unregister();
