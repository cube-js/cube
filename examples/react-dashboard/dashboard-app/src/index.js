import React from "react";
import { createRoot } from "react-dom/client";
import "./index.less";
import App from "./App";
import * as serviceWorker from "./serviceWorker";
import { BrowserRouter } from "react-router-dom";
import createExampleWrapper from "@cube-dev/example-wrapper";

createExampleWrapper({ title: "React Dashboard" });

const root = createRoot(
  document.getElementById("root")
);

root.render(
  <BrowserRouter>
      <App />
  </BrowserRouter>
);

serviceWorker.unregister();
