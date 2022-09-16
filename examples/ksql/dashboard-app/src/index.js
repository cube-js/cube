import React from "react";
import ReactDOM from "react-dom";
import createExampleWrapper from "@cube-dev/example-wrapper";
import "./index.css";
import App from "./App";
import DashboardPage from "./pages/DashboardPage";

const exampleDescription = {
  title: "Real-Time Dashboard with Kafka, ksqlDB and Cube",
  text: `<p>This live demo shows a real-time dashboard built with Kafka, ksqlDB and Cube.</p>`
};

createExampleWrapper(exampleDescription);

ReactDOM.render(
  <App>
    <DashboardPage/>
  </App>,
  document.getElementById("root")
);
