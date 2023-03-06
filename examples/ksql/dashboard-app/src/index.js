import React from "react";
import ReactDOM from "react-dom";
import createExampleWrapper from "@cube-dev/example-wrapper";
import "./index.css";
import App from "./App";
import DashboardPage from "./pages/DashboardPage";

const exampleDescription = {
  title: "Real-Time Dashboard with Kafka, ksqlDB and Cube",
  text: `
    <p>This live demo shows a real-time dashboard built with Kafka, ksqlDB and Cube.</p>
    <p>Read about <a href="https://cube.dev/blog/headless-bi-with-streaming-data">ksqlDB and Cube integration</a> or explore the <a href="https://github.com/cube-js/cube.js/tree/master/examples/ksql">source code</a> to learn more.</p>
  `
};

createExampleWrapper(exampleDescription);

ReactDOM.render(
  <App>
    <DashboardPage/>
  </App>,
  document.getElementById("root")
);
