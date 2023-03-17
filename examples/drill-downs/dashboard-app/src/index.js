import React from "react";
import ReactDOM from "react-dom";
import "./index.css";
import App from "./App";
import { HashRouter as Router, Route } from "react-router-dom";
import DashboardPage from "./pages/DashboardPage";

import createExampleWrapper from "@cube-dev/example-wrapper";

const exampleDescription = {
  title: "Drill Downs",
  text: `
    <p>This live demo shows a <a href="https://cube.dev/docs/schema/fundamentals/additional-concepts/#drilldowns">drill down</a> built with Cube and React.</p>
    <p>You can use it to dive deeper into data tables.</p> 
    <p>Read the <a href="https://cube.dev/blog/introducing-a-drill-down-table-api-in-cubejs">story</a> or explore the <a href="https://github.com/cube-js/cube.js/tree/master/examples/drill-downs">source code</a> to learn more.</p>
  `
};

createExampleWrapper(exampleDescription);

ReactDOM.render(
  <React.StrictMode>
    <Router>
      <App>
        <Route key="index" exact path="/" component={DashboardPage} />
      </App>
    </Router>
  </React.StrictMode>,
  document.getElementById("root")
);
