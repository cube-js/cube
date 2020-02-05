import React from "react";
import ReactDOM from "react-dom";
import "./index.css";
import App from "./App";
import * as serviceWorker from "./serviceWorker";
import { HashRouter as Router, Route } from "react-router-dom";

import DashboardPage from "./pages/DashboardPage";
import AudiencePage from "./pages/AudiencePage";
import BehaviorPage from "./pages/BehaviorPage";
import AcquisitionPage from "./pages/AcquisitionPage";

ReactDOM.render(
  <Router>
    <App>
      <Route key="index" exact path="/" component={AudiencePage} />
      <Route key="index" exact path="/behavior" component={BehaviorPage} />
      <Route key="index" exact path="/acquisition" component={AcquisitionPage} />
    </App>
  </Router>,
  document.getElementById("root")
); // If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: https://bit.ly/CRA-PWA

serviceWorker.unregister();
