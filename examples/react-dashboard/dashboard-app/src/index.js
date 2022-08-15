import React from "react";
import ReactDOM from "react-dom";
import "./index.less";
import App from "./App";
import * as serviceWorker from "./serviceWorker";
import { HashRouter as Router, Route } from "react-router-dom";
import createExampleWrapper from "@cube-dev/example-wrapper";
import ExplorePage from "./pages/ExplorePage";
import DashboardPage from "./pages/DashboardPage";

createExampleWrapper({ title: "React Dashboard" });

ReactDOM.render(
  <Router>
    <App>
      <Route key="index" exact path="/" component={DashboardPage} />
      <Route key="explore" path="/explore" component={ExplorePage} />
    </App>
  </Router>,
  document.getElementById("root")
); // If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: https://bit.ly/CRA-PWA

serviceWorker.unregister();
