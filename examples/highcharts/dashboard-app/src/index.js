import React from "react";
import ReactDOM from "react-dom";
import "./index.css";
import App from "./App";
import { HashRouter as Router } from "react-router-dom";

import createExampleWrapper from "@cube-dev/example-wrapper";

const exampleDescription = {
  title: "E-commerce dashboard with Highcharts",
  text: `
  <p>This live demo shows a Hightcharts dashboard created with React and Cube.</p>
  <p>
    Follow the <a href="https://cube.dev/blog/react-highcharts-example">tutorial</a> 
    or explore 
    the <a href="https://github.com/cube-js/cube.js/tree/master/examples/highcharts">source code</a>
    to learn more.
  </p>
  `
};

createExampleWrapper(exampleDescription);

ReactDOM.render(
  <React.StrictMode>
    <Router>
      <App></App>
    </Router>
  </React.StrictMode>,
  document.getElementById("root")
);
