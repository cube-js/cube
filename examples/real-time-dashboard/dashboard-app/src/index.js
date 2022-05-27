import React from "react";
import ReactDOM from "react-dom";
import "./index.css";
import App from "./App";
import { HashRouter as Router, Route } from "react-router-dom";
import DashboardPage from "./pages/DashboardPage";

import createExampleWrapper from "cube-example-wrapper"
const exampleDescription = {
  title: "Real-Time Dashboard",
  text: `
    <p>This live demo shows a <a href="https://cube.dev/docs/real-time-data-fetch">real-time data fetch</a> built with Cube, MongoDB, and React.</p>
    <p>You can use it to update charts as new data comes in.</p>
    <p>Follow the <a href="https://real-time-dashboard.cube.dev/">guide</a> or explore the <a href="https://github.com/cube-js/cube.js/tree/master/examples/real-time-dashboard">source code</a> to learn more.</p>
  `
};

createExampleWrapper(exampleDescription)

ReactDOM.render(
  <Router>
    <App>
      <Route key="index" exact path="/" component={DashboardPage} />
    </App>
  </Router>,
  document.getElementById("root")
);
