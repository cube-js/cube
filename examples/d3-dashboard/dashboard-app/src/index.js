import React from "react";
import ReactDOM from "react-dom";
import "./index.css";
import App from "./App";
import { HashRouter as Router, Route } from "react-router-dom";
import DashboardPage from "./pages/DashboardPage";

import createExampleWrapper from "@cube-dev/example-wrapper";

const exampleDescription = {
  title: "D3 Dashboard",
  text:`
    <p>This live demo shows a D3 dashboard created with React, Material UI, and Cube.</p>
    <p>
      Follow 
      the <a href="https://d3-dashboard.cube.dev/">tutorial</a>
      or explore 
      the <a href="https://github.com/cube-js/cube.js/tree/master/examples/d3-dashboard">source code</a>
      to learn more.
    </p>
  `
};

createExampleWrapper(exampleDescription);

ReactDOM.render(
  <Router>
    <App>
      <Route key="index" exact path="/" component={DashboardPage} />
    </App>
  </Router>,
  document.getElementById("root")
);
