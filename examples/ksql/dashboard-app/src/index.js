import React from "react";
import ReactDOM from "react-dom";
import createExampleWrapper from "@cube-dev/example-wrapper";
import "./index.css";
import App from "./App";
import DashboardPage from "./pages/DashboardPage";

const exampleDescription = {
  title: "Real-Time Dashboard with Kafka, ksqlDB and Cube",
  text: `<p>This live demo shows a real-time dashboard built with Kafka, ksqlDB and Cube.</p>
    <p>Follow the <a href="https://real-time-dashboard.cube.dev/">guide</a> or explore the <a href="https://github.com/cube-js/cube.js/tree/master/examples/real-time-dashboard">source code</a> to learn more.</p>`
};

createExampleWrapper(exampleDescription);

ReactDOM.render(
  <App>
    <DashboardPage/>
  </App>,
  document.getElementById("root")
);
