import React from 'react';
import ReactDOM from 'react-dom';
import { HashRouter as Router, Route } from 'react-router-dom';
import DashboardPage from './pages/DashboardPage';
import App from './App';
import "./index.css"

import createExampleWrapper from "@cube-dev/example-wrapper";

const exampleDescription = {
  title: "React Data Table",
  text: `<p>This live demo shows a Material UI data table created with React and Cube.</p>
    <p>
      Follow 
      the <a href="https://dev.to/cubejs/react-data-table-with-material-ui-and-a-spark-of-joy-50o1">tutorial</a>
      or explore 
      the <a href="https://github.com/cube-js/cube.js/tree/master/examples/react-data-table">source code</a>
      to learn more.
    </p>`
};

createExampleWrapper(exampleDescription);

ReactDOM.render(
  <Router>
    <App>
      <Route key="index" exact path="/" component={DashboardPage} />
    </App>
  </Router>, // eslint-disable-next-line no-undef
  document.getElementById('root')
);
