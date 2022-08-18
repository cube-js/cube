import React from 'react';
import ReactDOM from 'react-dom';
import { HashRouter as Router, Route } from 'react-router-dom';
import ExplorePage from './pages/ExplorePage';
import DashboardPage from './pages/DashboardPage';
import createExampleWrapper from "@cube-dev/example-wrapper";
import App from './App';

createExampleWrapper({
    title: "React Pivot Table with AG Grid and Cube.js",
    text: `
    <p>This live demo shows a pivot table created with AG Grid, React, and Cube.</p>
    <p>
        Follow the <a href="https://react-pivot-table.cube.dev/">tutorial</a> 
        or explore the <a href="https://github.com/cube-js/cube.js/tree/master/examples/react-pivot-table">source code</a> 
        to learn more.
    </p>
  `,
});

ReactDOM.render(
  <Router>
    <App>
      <Route key="index" exact path="/" component={DashboardPage} />
      <Route key="explore" path="/explore" component={ExplorePage} />
    </App>
  </Router>, // eslint-disable-next-line no-undef
  document.getElementById('root')
);
